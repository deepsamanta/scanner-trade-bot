import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import os
import gspread

from decimal import Decimal, getcontext
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY PARAMETERS  (PDH/PDL Liquidity Sweep — LONG + SHORT)
#
# TIMEFRAME ARCHITECTURE:
#   - PDH / PDL levels     : previous day's high / low  (daily candles)
#   - Entry confirmation   : 1-minute candles (sweep + 2-candle reversal)
#   - Scan interval        : 15 minutes
#
# LONG SETUP:
#   1. A 1m candle's low  breaks below PDL  →  sweep detected
#   2. First bullish 1m candle after sweep  →  record its high
#   3. Any subsequent 1m candle closes above that high  →  ENTER LONG
#   SL = lowest low reached during/after the sweep
#   TP = entry × (1 + TP_PCT / 100)
#
# SHORT SETUP:
#   1. A 1m candle's high breaks above PDH  →  sweep detected
#   2. First bearish 1m candle after sweep  →  record its low
#   3. Any subsequent 1m candle closes below that low  →  ENTER SHORT
#   SL = highest high reached during/after the sweep
#   TP = entry × (1 − TP_PCT / 100)
# =============================================================================

TP_PCT               = 1.5    # fixed TP %
MIN_SL_PCT           = 0.5    # minimum SL distance from entry (%)

SWEEP_EXPIRY_BARS    = 60     # 1m bars before an unresolved sweep expires (= 60 min)
CANDLES_DAILY        = 5      # daily candles fetched  (only prev day needed)
CANDLES_1M           = 300    # 1m candles fetched per scan  (~5 hours)

RESOLUTION_DAILY     = "1D"
RESOLUTION_1M        = "1"
RESOLUTION_1H        = "60"
CANDLE_SECONDS_DAILY = 86400
CANDLE_SECONDS_1M    = 60
CANDLE_SECONDS_1H    = 3600

SCAN_INTERVAL        = 90
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE           = "pdh_pdl_state.json"
# =============================================================================


# =====================================================
# GOOGLE SHEETS
# =====================================================

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_sheet          = None
_last_auth_time = 0


def get_sheet():
    global _sheet, _last_auth_time
    now = time.time()
    if _sheet is None or (now - _last_auth_time) > GSHEET_REAUTH_INTERVAL:
        try:
            creds           = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
            client          = gspread.authorize(creds)
            _sheet          = client.open_by_key(SHEET_ID).sheet1
            _last_auth_time = now
            print("[GSHEET] Re-authenticated successfully")
        except Exception as e:
            print(f"[GSHEET] Re-auth failed: {e}")
    return _sheet


def get_sheet_data():
    try:
        sheet = get_sheet()
        if sheet is None:
            return pd.DataFrame()
        data = sheet.get_all_values()
        df   = pd.DataFrame(data)
        if df.shape[1] < 3:
            for col in range(df.shape[1], 3):
                df[col] = ""
        return df
    except Exception as e:
        print("Sheet read error:", e)
        return pd.DataFrame()


def update_sheet_tp(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update_acell(f"B{row + 1}", str(value))
        print(f"[SHEET] Row {row + 1} col B -> {value}")
    except Exception as e:
        print("Sheet update error:", e)


def update_sheet_sl(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update_acell(f"C{row + 1}", str(value))
        print(f"[SHEET] Row {row + 1} col C (SL) -> {value}")
    except Exception as e:
        print("Sheet SL update error:", e)


# =====================================================
# LOCAL STATE PERSISTENCE
# =====================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[STATE] Load error: {e} — starting fresh")
            return {}
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE] Save error: {e}")


def init_symbol_state():
    return {
        "in_position":          False,
        "direction":            None,   # "long" or "short"
        "entry_price":          None,
        "tp_level":             None,
        "sl_price":             None,
        "last_entry_ts":        0,

        # Daily level tracking
        "current_day_str":      None,   # "YYYY-MM-DD" — resets sweep on new day
        "pdh":                  None,
        "pdl":                  None,

        # Sweep detection
        "sweep_direction":      None,   # "long" or "short"
        "sweep_ts":             0,      # ms ts of candle that confirmed sweep
        "recent_swing_low":     None,   # lowest low during/after sweep (long SL)
        "recent_swing_high":    None,   # highest high during/after sweep (short SL)

        # Sweep candle snapshot (for signal readout)
        "sweep_o":              None,
        "sweep_h":              None,
        "sweep_l":              None,
        "sweep_c":              None,

        # 2-candle pattern state — NO reset once armed; expires only with the sweep
        "first_signal_high":    None,   # first bullish candle high (long confirm level)
        "first_signal_low":     None,   # first bearish candle low  (short confirm level)
        "first_signal_ts":      0,      # ms ts of first signal candle
        "first_signal_o":       None,   # OHLC of first signal candle (for readout)
        "first_signal_h":       None,
        "first_signal_l":       None,
        "first_signal_c":       None,
        "bars_to_first_signal": None,   # how many 1m bars from sweep to first signal

        "last_processed_1m_ts": 0,

        # Already-swept guard (reset each new day)
        "pdl_swept_today":      False,  # 1h candle already swept PDL today
        "pdh_swept_today":      False,  # 1h candle already swept PDH today
    }


# =====================================================
# SYMBOL HELPERS
# =====================================================

def normalize_symbol(symbol):
    symbol = str(symbol).upper().strip()
    if "USDT" in symbol:
        return symbol.split("USDT")[0] + "USDT"
    return symbol


def fut_pair(symbol):
    return f"B-{symbol.replace('USDT', '')}_USDT"


# =====================================================
# SIGN REQUEST
# =====================================================

def sign_request(body):
    payload   = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(
        bytes(COINDCX_SECRET, encoding="utf-8"),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Content-Type":     "application/json",
        "X-AUTH-APIKEY":    COINDCX_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    return payload, headers


# =====================================================
# TELEGRAM
# =====================================================

def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }
        for attempt in range(3):
            r = requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
            if r.status_code == 200:
                return
            if r.status_code == 429:
                retry_after = r.json().get("parameters", {}).get("retry_after", 10)
                print(f"[TELEGRAM] Rate limited — waiting {retry_after}s (attempt {attempt + 1}/3)")
                time.sleep(retry_after + 1)
            else:
                print(f"[TELEGRAM] Non-200: {r.status_code} {r.text[:200]}")
                return
    except Exception as e:
        print(f"[TELEGRAM] Failed: {e}")


# =====================================================
# PRECISION
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# CANDLE FETCH
# =====================================================

def fetch_candles(symbol, num_candles_needed, resolution_str, candle_seconds):
    pair_api      = fut_pair(symbol)
    url           = "https://public.coindcx.com/market_data/candlesticks"
    now           = int(time.time())
    fetch_seconds = (num_candles_needed + 50) * candle_seconds
    params = {
        "pair":       pair_api,
        "from":       now - fetch_seconds,
        "to":         now,
        "resolution": resolution_str,
        "pcode":      "f",
    }
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data     = response.json().get("data", [])
        return sorted(data, key=lambda x: x["time"])
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} fetch error: {e}")
        return []


# =====================================================
# RECENT HIGH / LOW  (for TP wick detection between scans)
# =====================================================

def get_recent_high(symbol):
    try:
        pair_api = fut_pair(symbol)
        url      = "https://public.coindcx.com/market_data/candlesticks"
        now      = int(time.time())
        params   = {"pair": pair_api, "from": now - SCAN_INTERVAL,
                    "to": now, "resolution": "1", "pcode": "f"}
        candles  = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return max(float(c["high"]) for c in candles) if candles else None
    except Exception as e:
        print(f"[RECENT HIGH] {symbol} error: {e}")
        return None


def get_recent_low(symbol):
    try:
        pair_api = fut_pair(symbol)
        url      = "https://public.coindcx.com/market_data/candlesticks"
        now      = int(time.time())
        params   = {"pair": pair_api, "from": now - SCAN_INTERVAL,
                    "to": now, "resolution": "1", "pcode": "f"}
        candles  = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return min(float(c["low"]) for c in candles) if candles else None
    except Exception as e:
        print(f"[RECENT LOW] {symbol} error: {e}")
        return None


# =====================================================
# POSITIONS & ORDERS
# =====================================================

def get_open_positions():
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "page":                       "1",
            "size":                       "50",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        response  = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/positions",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        positions = response.json()
        if not isinstance(positions, list):
            return []
        return [p for p in positions if float(p.get("active_pos", 0)) != 0]
    except Exception as e:
        print("get_open_positions error:", e)
        return []


def get_position_by_pair(symbol):
    pair = fut_pair(symbol)
    for p in get_open_positions():
        if p.get("pair") == pair:
            return p
    return None


def has_open_order(symbol):
    pair = fut_pair(symbol)
    for side in ("buy", "sell"):
        try:
            body = {
                "timestamp":                  int(time.time() * 1000),
                "status":                     "open,partially_filled",
                "side":                       side,
                "page":                       "1",
                "size":                       "50",
                "margin_currency_short_name": ["USDT"],
            }
            payload, headers = sign_request(body)
            orders = requests.post(
                BASE_URL + "/exchange/v1/derivatives/futures/orders",
                data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
            ).json()
            if isinstance(orders, list):
                for o in orders:
                    if o.get("pair") == pair:
                        return True
        except Exception as e:
            print(f"has_open_order error ({symbol}, {side}): {e}")
    return False


def extract_tp_sl(obj):
    if not isinstance(obj, dict):
        return None, None
    tp_keys = ["take_profit_price", "take_profit_trigger", "tp_price"]
    sl_keys = ["stop_loss_price",   "stop_loss_trigger",   "sl_price"]

    def _pick(keys):
        for k in keys:
            v = obj.get(k)
            if v is None or v == "" or v == "0" or v == 0:
                continue
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                continue
        return None

    return _pick(tp_keys), _pick(sl_keys)


# =====================================================
# QUANTITY
# =====================================================

def get_quantity_step(symbol):
    try:
        pair = fut_pair(symbol)
        url  = (
            "https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
            f"?pair={pair}&margin_currency_short_name=USDT"
        )
        data       = requests.get(url, timeout=REQUEST_TIMEOUT).json()
        instrument = data["instrument"]
        qty_inc    = Decimal(str(instrument["quantity_increment"]))
        min_qty    = Decimal(str(instrument["min_quantity"]))
        return max(qty_inc, min_qty)
    except Exception:
        return Decimal("1")


def compute_qty(entry_price, symbol):
    step     = get_quantity_step(symbol)
    exposure = Decimal(str(CAPITAL_USDT)) * Decimal(str(LEVERAGE))
    raw_qty  = exposure / Decimal(str(entry_price))
    qty      = (raw_qty / step).quantize(Decimal("1")) * step
    if qty <= 0:
        qty = step
    return float(qty.quantize(step))


# =====================================================
# PLACE LONG ORDER
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision, signal_info=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    tp_pct_display = round(((tp - entry) / entry) * 100, 2) if entry else 0
    sl_pct_display = round(((entry - sl) / entry) * 100, 2) if entry else 0

    print(
        f"[LONG TRADE] {symbol} BUY (long_sweep) | Entry {entry} | "
        f"TP {tp} (+{tp_pct_display}%) | SL {sl} (-{sl_pct_display}%) | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "buy",
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry,
            "total_quantity":    qty,
            "leverage":          LEVERAGE,
            "take_profit_price": tp,
            "stop_loss_price":   sl,
        },
    }
    payload, headers = sign_request(body)
    try:
        result = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        ).json()
    except Exception as e:
        print(f"[ERROR] {symbol} order request failed: {e}")
        return False

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} long order not placed: {result}")
        send_telegram(
            f"❌ <b>LONG ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>\n"
            f"🛑 SL      : <code>{sl}</code>\n"
            f"⚠️ Response: <code>{str(result)[:200]}</code>"
        )
        return False

    si = signal_info or {}
    send_telegram(
        f"🟢 <b>NEW LONG (PDL SWEEP) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry          : <code>{entry}</code>\n"
        f"🎯 TP             : <code>{tp}</code>  (+{tp_pct_display}%)\n"
        f"🛑 SL             : <code>{sl}</code>  (-{sl_pct_display}%)\n"
        f"📦 Qty            : <code>{qty}</code>\n"
        f"💰 Margin         : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Why this trade:</b>\n"
        f"📅 Prev day range : PDH <code>{si.get('pdh')}</code>  |  PDL <code>{si.get('pdl')}</code>\n"
        f"⬇️ PDL swept by   : <code>{si.get('sweep_ext')}</code> pts "
        f"→ low hit <code>{si.get('sweep_low')}</code>\n"
        f"🕯 Sweep candle   : O=<code>{si.get('sweep_o')}</code> "
        f"H=<code>{si.get('sweep_h')}</code> "
        f"L=<code>{si.get('sweep_l')}</code> "
        f"C=<code>{si.get('sweep_c')}</code>\n"
        f"🟢 Signal candle  : O=<code>{si.get('sig_o')}</code> "
        f"H=<code>{si.get('sig_h')}</code> "
        f"L=<code>{si.get('sig_l')}</code> "
        f"C=<code>{si.get('sig_c')}</code>  "
        f"(bullish, {si.get('bars_to_sig')}b after sweep)\n"
        f"✅ Confirm candle : O=<code>{si.get('confirm_o')}</code> "
        f"H=<code>{si.get('confirm_h')}</code> "
        f"L=<code>{si.get('confirm_l')}</code> "
        f"C=<code>{si.get('confirm_c')}</code>  "
        f"(closed above signal high, {si.get('bars_to_confirm')}b after sweep)\n"
        f"📌 Trigger        : close <code>{si.get('confirm_c')}</code> "
        f"&gt; signal high <code>{si.get('sig_h')}</code>"
    )
    return True


# =====================================================
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, signal_info=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    tp_pct_display = round(((entry - tp) / entry) * 100, 2) if entry else 0
    sl_pct_display = round(((sl - entry) / entry) * 100, 2) if entry else 0

    print(
        f"[SHORT TRADE] {symbol} SELL (short_sweep) | Entry {entry} | "
        f"TP {tp} (-{tp_pct_display}%) | SL {sl} (+{sl_pct_display}%) | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "sell",
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry,
            "total_quantity":    qty,
            "leverage":          LEVERAGE,
            "take_profit_price": tp,
            "stop_loss_price":   sl,
        },
    }
    payload, headers = sign_request(body)
    try:
        result = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        ).json()
    except Exception as e:
        print(f"[ERROR] {symbol} order request failed: {e}")
        return False

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} short order not placed: {result}")
        send_telegram(
            f"❌ <b>SHORT ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>\n"
            f"🛑 SL      : <code>{sl}</code>\n"
            f"⚠️ Response: <code>{str(result)[:200]}</code>"
        )
        return False

    si = signal_info or {}
    send_telegram(
        f"🔴 <b>NEW SHORT (PDH SWEEP) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry          : <code>{entry}</code>\n"
        f"🎯 TP             : <code>{tp}</code>  (-{tp_pct_display}%)\n"
        f"🛑 SL             : <code>{sl}</code>  (+{sl_pct_display}%)\n"
        f"📦 Qty            : <code>{qty}</code>\n"
        f"💰 Margin         : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Why this trade:</b>\n"
        f"📅 Prev day range : PDH <code>{si.get('pdh')}</code>  |  PDL <code>{si.get('pdl')}</code>\n"
        f"⬆️ PDH swept by   : <code>{si.get('sweep_ext')}</code> pts "
        f"→ high hit <code>{si.get('sweep_high')}</code>\n"
        f"🕯 Sweep candle   : O=<code>{si.get('sweep_o')}</code> "
        f"H=<code>{si.get('sweep_h')}</code> "
        f"L=<code>{si.get('sweep_l')}</code> "
        f"C=<code>{si.get('sweep_c')}</code>\n"
        f"🔴 Signal candle  : O=<code>{si.get('sig_o')}</code> "
        f"H=<code>{si.get('sig_h')}</code> "
        f"L=<code>{si.get('sig_l')}</code> "
        f"C=<code>{si.get('sig_c')}</code>  "
        f"(bearish, {si.get('bars_to_sig')}b after sweep)\n"
        f"✅ Confirm candle : O=<code>{si.get('confirm_o')}</code> "
        f"H=<code>{si.get('confirm_h')}</code> "
        f"L=<code>{si.get('confirm_l')}</code> "
        f"C=<code>{si.get('confirm_c')}</code>  "
        f"(closed below signal low, {si.get('bars_to_confirm')}b after sweep)\n"
        f"📌 Trigger        : close <code>{si.get('confirm_c')}</code> "
        f"&lt; signal low <code>{si.get('sig_l')}</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def _clear_sweep(st):
    """Reset sweep + pattern state without touching position state."""
    st["sweep_direction"]      = None
    st["sweep_ts"]             = 0
    st["recent_swing_low"]     = None
    st["recent_swing_high"]    = None
    st["sweep_o"]              = None
    st["sweep_h"]              = None
    st["sweep_l"]              = None
    st["sweep_c"]              = None
    st["first_signal_high"]    = None
    st["first_signal_low"]     = None
    st["first_signal_ts"]      = 0
    st["first_signal_o"]       = None
    st["first_signal_h"]       = None
    st["first_signal_l"]       = None
    st["first_signal_c"]       = None
    st["bars_to_first_signal"] = None


def check_and_trade(symbol, row, df, all_state):
    now_ms = int(time.time() * 1000)

    # ── 1. Fetch & validate daily candles for PDH / PDL ──────────────────
    daily = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAILY)

    # Drop incomplete current-day bar
    if daily and (now_ms - int(daily[-1]["time"])) < CANDLE_SECONDS_DAILY * 1000:
        daily = daily[:-1]

    if len(daily) < 1:
        print(f"[SKIP] {symbol} — no completed daily candles")
        return

    prev_day = daily[-1]
    pdh      = float(prev_day["high"])
    pdl      = float(prev_day["low"])

    # ── 2. Per-symbol state ───────────────────────────────────────────────
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset (preserve position state, reset sweep) ───────────
    # CoinDCX daily candle resets at 00:00 UTC (= 05:30 AM IST)
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    if st["current_day_str"] != today_str:
        print(f"[NEW DAY] {symbol} — PDH={pdh} PDL={pdl}  (resetting sweep state)")

        # Keep position/last_entry; wipe everything else
        preserved = {k: st[k] for k in ("in_position", "direction", "entry_price",
                                          "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    st["pdh"] = pdh
    st["pdl"] = pdl

    # Precision from the most recent daily close
    precision = get_precision(prev_day["close"])

    # ── 4. TP COMPLETED marker check ──────────────────────────────────────
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED in sheet")
        save_state(all_state)
        return

    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        direction = st.get("direction")
        tp_hit    = False
        hit_kind  = None
        hit_price = None

        last_1m = fetch_candles(symbol, 2, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None

        if direction == "long" or (direction is None and tp_stored > (last_close or 0)):
            if last_close is not None and last_close >= tp_stored:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rh = get_recent_high(symbol)
                if rh is not None and rh >= tp_stored:
                    tp_hit, hit_kind, hit_price = True, "wick", rh
        elif direction == "short" or (direction is None and tp_stored < (last_close or float("inf"))):
            if last_close is not None and last_close <= tp_stored:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rl = get_recent_low(symbol)
                if rl is not None and rl <= tp_stored:
                    tp_hit, hit_kind, hit_price = True, "wick", rl

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} — {hit_kind} {hit_price}  target={tp_stored}")

            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"] = prev_last
            save_state(all_state)
            return

    # ── 5. Reconcile with exchange ────────────────────────────────────────
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px  = float(position.get("avg_price") or position.get("entry_price") or 0)
            active    = float(position.get("active_pos", 0))
            direction = "long" if active > 0 else "short"
            st["in_position"] = True
            st["direction"]   = direction
            st["entry_price"] = entry_px
            print(f"[RECONCILE] {symbol} — reconstructed {direction} from exchange")

        tp_pos, sl_pos = extract_tp_sl(position)
        if st.get("tp_level") is None and tp_pos is not None:
            st["tp_level"] = round(tp_pos, precision)
        if st.get("sl_price") is None and sl_pos is not None:
            st["sl_price"] = round(sl_pos, precision)

        b_val = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
        c_val = str(df.iloc[row, 2]).strip() if df.shape[1] > 2 else ""
        if st.get("tp_level") is not None and b_val == "":
            update_sheet_tp(row, st["tp_level"])
        if st.get("sl_price") is not None and c_val == "":
            update_sheet_sl(row, st["sl_price"])

        save_state(all_state)
        return

    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")

        prev_last = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # ── 6. Check 1h candles — has this level already been swept today? ───
    # Fetch today's 1h closed candles and see if any already swept PDH/PDL.
    # If yes, skip this symbol for the rest of the day.
    if not st.get("pdl_swept_today") or not st.get("pdh_swept_today"):
        candles_1h = fetch_candles(symbol, 30, RESOLUTION_1H, CANDLE_SECONDS_1H)
        # Keep only candles that opened today (UTC)
        # Daily candle on CoinDCX starts at 00:00 UTC = 05:30 AM IST
        # today_str is already derived from UTC, so this is correct
        from datetime import timezone
        today_start_ms = int(datetime.strptime(today_str, "%Y-%m-%d")
                             .replace(tzinfo=timezone.utc).timestamp() * 1000)
        todays_1h = [c for c in candles_1h if int(c["time"]) >= today_start_ms]
        for c1h in todays_1h:
            if float(c1h["low"])  < pdl:
                st["pdl_swept_today"] = True
            if float(c1h["high"]) > pdh:
                st["pdh_swept_today"] = True

    if st.get("pdl_swept_today") and st.get("pdh_swept_today"):
        print(f"[SKIP] {symbol} — both PDH and PDL already swept today on 1h, no trade")
        save_state(all_state)
        return

    # ── 7. Fetch 1m candles ───────────────────────────────────────────────
    candles_1m = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)

    # Drop in-progress 1m bar
    if candles_1m and (now_ms - int(candles_1m[-1]["time"])) < CANDLE_SECONDS_1M * 1000:
        candles_1m = candles_1m[:-1]

    if len(candles_1m) < 5:
        print(f"[SKIP] {symbol} — insufficient 1m candles")
        save_state(all_state)
        return

    # Only process candles we haven't seen yet
    last_processed = st.get("last_processed_1m_ts", 0)
    new_candles    = [c for c in candles_1m if int(c["time"]) > last_processed]

    if not new_candles:
        save_state(all_state)
        return

    # ── 8. Walk new 1m candles — sweep detection + 2-candle pattern ───────
    entry_path   = None
    entry_price  = None
    sl_price_val = None
    tp_price_val = None
    signal_info  = None

    for candle in new_candles:
        c_ts = int(candle["time"])
        o    = float(candle["open"])
        h    = float(candle["high"])
        l    = float(candle["low"])
        c    = float(candle["close"])

        sweep_dir = st["sweep_direction"]

        # ── A. No sweep yet — detect one ──────────────────────────────────
        if sweep_dir is None:
            if l < pdl and not st.get("pdl_swept_today"):  # swept below PDL → long setup
                st["sweep_direction"]  = "long"
                st["sweep_ts"]         = c_ts
                st["recent_swing_low"] = l
                st["sweep_o"] = o; st["sweep_h"] = h
                st["sweep_l"] = l; st["sweep_c"] = c
                ext = round(pdl - l, precision)
                print(f"[SWEEP-LONG]  {symbol} | low={l} < PDL={pdl} (ext={ext}) | ts={c_ts}")

            elif h > pdh and not st.get("pdh_swept_today"):  # swept above PDH → short setup
                st["sweep_direction"]   = "short"
                st["sweep_ts"]          = c_ts
                st["recent_swing_high"] = h
                st["sweep_o"] = o; st["sweep_h"] = h
                st["sweep_l"] = l; st["sweep_c"] = c
                ext = round(h - pdh, precision)
                print(f"[SWEEP-SHORT] {symbol} | high={h} > PDH={pdh} (ext={ext}) | ts={c_ts}")


        # ── B. Long sweep armed ───────────────────────────────────────────
        elif sweep_dir == "long":
            # Update swing low if price extends further down
            if l < st["recent_swing_low"]:
                st["recent_swing_low"] = l

            bars_since_sweep = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
            if bars_since_sweep > SWEEP_EXPIRY_BARS:
                print(f"[SWEEP-EXPIRE] {symbol} — long sweep expired ({bars_since_sweep} bars)")

                _clear_sweep(st)
                st["last_processed_1m_ts"] = c_ts
                continue

            if st["first_signal_high"] is None:
                # Looking for the FIRST bullish 1m candle after sweep — one shot only
                if c > o:
                    bars_to_sig = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
                    st["first_signal_high"]    = h
                    st["first_signal_ts"]      = c_ts
                    st["first_signal_o"]       = o
                    st["first_signal_h"]       = h
                    st["first_signal_l"]       = l
                    st["first_signal_c"]       = c
                    st["bars_to_first_signal"] = bars_to_sig
                    print(f"[SIGNAL-1-LONG] {symbol} | bullish candle high={h} | {bars_to_sig}b after sweep")
            else:
                # Waiting for a candle to close above first_signal_high — no reset
                if c_ts > st["first_signal_ts"] and c > st["first_signal_high"]:
                    bars_to_confirm = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
                    entry_price  = c
                    natural_sl   = st["recent_swing_low"]
                    min_sl       = entry_price * (1 - MIN_SL_PCT / 100)
                    sl_price_val = min(natural_sl, min_sl)   # further of the two
                    tp_price_val = entry_price * (1 + TP_PCT / 100)
                    entry_path   = "long_sweep"
                    signal_info  = {
                        "direction":           "long",
                        "pdh":                 round(pdh, precision),
                        "pdl":                 round(pdl, precision),
                        "sweep_ext":           round(pdl - st["recent_swing_low"], precision),
                        "sweep_low":           round(st["recent_swing_low"], precision),
                        "sweep_o":             round(st["sweep_o"], precision),
                        "sweep_h":             round(st["sweep_h"], precision),
                        "sweep_l":             round(st["sweep_l"], precision),
                        "sweep_c":             round(st["sweep_c"], precision),
                        "sig_o":               round(st["first_signal_o"], precision),
                        "sig_h":               round(st["first_signal_h"], precision),
                        "sig_l":               round(st["first_signal_l"], precision),
                        "sig_c":               round(st["first_signal_c"], precision),
                        "confirm_o":           round(o, precision),
                        "confirm_h":           round(h, precision),
                        "confirm_l":           round(l, precision),
                        "confirm_c":           round(c, precision),
                        "bars_to_sig":         st["bars_to_first_signal"],
                        "bars_to_confirm":     bars_to_confirm,
                    }
                    st["last_processed_1m_ts"] = c_ts
                    break

        # ── C. Short sweep armed ──────────────────────────────────────────
        elif sweep_dir == "short":
            # Update swing high if price extends further up
            if h > st["recent_swing_high"]:
                st["recent_swing_high"] = h

            bars_since_sweep = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
            if bars_since_sweep > SWEEP_EXPIRY_BARS:
                print(f"[SWEEP-EXPIRE] {symbol} — short sweep expired ({bars_since_sweep} bars)")

                _clear_sweep(st)
                st["last_processed_1m_ts"] = c_ts
                continue

            if st["first_signal_low"] is None:
                # Looking for the FIRST bearish 1m candle after sweep — one shot only
                if c < o:
                    bars_to_sig = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
                    st["first_signal_low"]     = l
                    st["first_signal_ts"]      = c_ts
                    st["first_signal_o"]       = o
                    st["first_signal_h"]       = h
                    st["first_signal_l"]       = l
                    st["first_signal_c"]       = c
                    st["bars_to_first_signal"] = bars_to_sig
                    print(f"[SIGNAL-1-SHORT] {symbol} | bearish candle low={l} | {bars_to_sig}b after sweep")
            else:
                # Waiting for a candle to close below first_signal_low — no reset
                if c_ts > st["first_signal_ts"] and c < st["first_signal_low"]:
                    bars_to_confirm = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
                    entry_price  = c
                    natural_sl   = st["recent_swing_high"]
                    min_sl       = entry_price * (1 + MIN_SL_PCT / 100)
                    sl_price_val = max(natural_sl, min_sl)   # further of the two
                    tp_price_val = entry_price * (1 - TP_PCT / 100)
                    entry_path   = "short_sweep"
                    signal_info  = {
                        "direction":           "short",
                        "pdh":                 round(pdh, precision),
                        "pdl":                 round(pdl, precision),
                        "sweep_ext":           round(st["recent_swing_high"] - pdh, precision),
                        "sweep_high":          round(st["recent_swing_high"], precision),
                        "sweep_o":             round(st["sweep_o"], precision),
                        "sweep_h":             round(st["sweep_h"], precision),
                        "sweep_l":             round(st["sweep_l"], precision),
                        "sweep_c":             round(st["sweep_c"], precision),
                        "sig_o":               round(st["first_signal_o"], precision),
                        "sig_h":               round(st["first_signal_h"], precision),
                        "sig_l":               round(st["first_signal_l"], precision),
                        "sig_c":               round(st["first_signal_c"], precision),
                        "confirm_o":           round(o, precision),
                        "confirm_h":           round(h, precision),
                        "confirm_l":           round(l, precision),
                        "confirm_c":           round(c, precision),
                        "bars_to_sig":         st["bars_to_first_signal"],
                        "bars_to_confirm":     bars_to_confirm,
                    }
                    st["last_processed_1m_ts"] = c_ts
                    break

        st["last_processed_1m_ts"] = c_ts

    # If no entry was found, mark the last candle processed
    if entry_path is None:
        if new_candles:
            st["last_processed_1m_ts"] = int(new_candles[-1]["time"])
        save_state(all_state)
        return

    # ── 9. Validate SL / TP ───────────────────────────────────────────────
    if entry_path == "long_sweep" and sl_price_val >= entry_price:
        print(f"[SKIP] {symbol} — invalid SL for long (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return
    if entry_path == "short_sweep" and sl_price_val <= entry_price:
        print(f"[SKIP] {symbol} — invalid SL for short (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return

    # Last-second guards
    if get_position_by_pair(symbol) is not None:
        print(f"[ABORT] {symbol} — position appeared just before placement")
        return
    if has_open_order(symbol):
        print(f"[ABORT] {symbol} — order appeared just before placement")
        return

    # ── 10. Place order ───────────────────────────────────────────────────
    if entry_path == "long_sweep":
        placed = place_long_order(symbol, entry_price, tp_price_val, sl_price_val, precision, signal_info)
    else:
        placed = place_short_order(symbol, entry_price, tp_price_val, sl_price_val, precision, signal_info)

    if placed:
        st["in_position"]  = True
        st["direction"]    = "long" if entry_path == "long_sweep" else "short"
        st["entry_price"]  = round(entry_price,  precision)
        st["tp_level"]     = round(tp_price_val, precision)
        st["sl_price"]     = round(sl_price_val, precision)
        st["last_entry_ts"] = st["last_processed_1m_ts"]
        _clear_sweep(st)
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>PDH/PDL Sweep Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy  : <code>Previous Day High/Low Liquidity Sweep</code>\n"
    f"📅 Levels    : <code>PDH + PDL  (daily candles)</code>\n"
    f"⚡ Entry     : <code>2-candle reversal on 1m after sweep</code>\n"
    f"🔁 Scan      : <code>Every 15 minutes</code>\n"
    f"🎯 TP        : <code>entry ± {TP_PCT}%</code>\n"
    f"🛑 SL (long) : <code>below sweep low</code>\n"
    f"🛑 SL (short): <code>above sweep high</code>\n"
    f"⏳ Sweep exp : <code>{SWEEP_EXPIRY_BARS} × 1m bars</code>\n"
    f"💰 Capital   : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"----- TRADE SCAN — CYCLE {cycle} -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            try:
                check_and_trade(symbol, row, df, state)
            except Exception as e:
                print(f"[ERROR] {symbol} check_and_trade failed: {e}")
                continue

        save_state(state)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        consecutive_errors += 1
        print(f"BOT ERROR ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}")

        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            send_telegram(
                f"🚨 <b>Bot Crashed — Restarting</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ Error : <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive errors"
            )
            raise SystemExit(1)

        time.sleep(60)