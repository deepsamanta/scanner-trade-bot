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
# LONG SETUP:
#   1. A 1m candle's low  breaks below PDL           → sweep detected
#   2. First full-body bullish 1m candle after sweep  → candle 1
#   3. IMMEDIATELY next 1m candle is also full-body
#      bullish (no wick)                              → ENTER LONG at close
#   SL = sweep low  (min MIN_SL_PCT% from entry)
#   TP = entry × (1 + TP_PCT / 100)
#
# SHORT SETUP:
#   1. A 1m candle's high breaks above PDH            → sweep detected
#   2. First full-body bearish 1m candle after sweep  → candle 1
#   3. IMMEDIATELY next 1m candle is also full-body
#      bearish (no wick)                              → ENTER SHORT at close
#   SL = sweep high  (min MIN_SL_PCT% from entry)
#   TP = entry × (1 − TP_PCT / 100)
#
# FULL-BODY = wick on each side ≤ MAX_WICK_RATIO × body size
# If candle 2 fails, reset candle 1 and look for a fresh pair.
#
# 1H GUARD:
#   Before arming any sweep, check today's CLOSED 1h candles.
#   If PDL already swept on a closed 1h bar → skip long side.
#   If PDH already swept on a closed 1h bar → skip short side.
# =============================================================================

TP_PCT          = 1.5    # fixed TP %
MIN_SL_PCT      = 0.5    # minimum SL distance from entry (%)
MAX_WICK_RATIO  = 0.1    # wick ≤ 10% of body = "no wick" / full-body candle
SWEEP_EXPIRY_BARS = 60   # 1m bars before unresolved sweep expires (60 min)

CANDLES_DAILY   = 5
CANDLES_1M      = 300    # ~5 hours of 1m candles per scan
CANDLES_1H      = 30

RESOLUTION_DAILY   = "1D"
RESOLUTION_1M      = "1"
RESOLUTION_1H      = "60"
CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_1M  = 60
CANDLE_SECONDS_1H  = 3600

SCAN_INTERVAL          = 90
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "pdh_pdl_state.json"


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
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        while df.shape[1] < 3:
            df[df.shape[1]] = ""
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
        print(f"[SHEET] Row {row + 1} col C -> {value}")
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
        "in_position":   False,
        "direction":     None,
        "entry_price":   None,
        "tp_level":      None,
        "sl_price":      None,
        "last_entry_ts": 0,

        # Day tracking
        "current_day_str": None,
        "pdh":             None,
        "pdl":             None,

        # 1h sweep guard (reset each new day)
        "pdl_swept_1h": False,
        "pdh_swept_1h": False,

        # Sweep state
        "sweep_direction":   None,   # "long" or "short"
        "sweep_ts":          0,
        "recent_swing_low":  None,   # lowest low during sweep (long SL)
        "recent_swing_high": None,   # highest high during sweep (short SL)
        "sweep_o": None, "sweep_h": None,
        "sweep_l": None, "sweep_c": None,

        # 2-consecutive-full-body pattern
        # candle1 = first qualifying full-body reversal candle after sweep
        "candle1_ts": 0,
        "candle1_o":  None, "candle1_h": None,
        "candle1_l":  None, "candle1_c": None,

        # 1m dedup — reset to 0 on new day
        "last_processed_1m_ts": 0,
    }


# =====================================================
# SYMBOL HELPERS
# =====================================================

def normalize_symbol(raw):
    s = str(raw).upper().strip()
    if not s or s in ("SYMBOL", "PAIR", "COIN", "NAME"):
        return None
    if "USDT" in s:
        return s.split("USDT")[0] + "USDT"
    return s


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
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
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
    return len(s.split(".")[1]) if "." in s else 0


# =====================================================
# FULL-BODY CANDLE CHECKS
# =====================================================

def is_full_body_bullish(o, h, l, c):
    """Bullish candle with wick ≤ MAX_WICK_RATIO × body on each side."""
    if c <= o:
        return False
    body = c - o
    if body == 0:
        return False
    upper_wick = h - c   # wick above close
    lower_wick = o - l   # wick below open
    return upper_wick <= body * MAX_WICK_RATIO and lower_wick <= body * MAX_WICK_RATIO


def is_full_body_bearish(o, h, l, c):
    """Bearish candle with wick ≤ MAX_WICK_RATIO × body on each side."""
    if c >= o:
        return False
    body = o - c
    if body == 0:
        return False
    upper_wick = h - o   # wick above open
    lower_wick = c - l   # wick below close
    return upper_wick <= body * MAX_WICK_RATIO and lower_wick <= body * MAX_WICK_RATIO


# =====================================================
# CANDLE FETCH
# =====================================================

def fetch_candles(symbol, num_candles, resolution_str, candle_seconds):
    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    params   = {
        "pair":       pair_api,
        "from":       now - (num_candles + 50) * candle_seconds,
        "to":         now,
        "resolution": resolution_str,
        "pcode":      "f",
    }
    try:
        data = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return sorted(data, key=lambda x: x["time"])
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} error: {e}")
        return []


# =====================================================
# RECENT HIGH / LOW  (TP wick detection)
# =====================================================

def get_recent_high(symbol):
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL,
                  "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get("https://public.coindcx.com/market_data/candlesticks",
                               params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return max(float(c["high"]) for c in candles) if candles else None
    except Exception as e:
        print(f"[RECENT HIGH] {symbol} error: {e}")
        return None


def get_recent_low(symbol):
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL,
                  "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get("https://public.coindcx.com/market_data/candlesticks",
                               params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return min(float(c["low"]) for c in candles) if candles else None
    except Exception as e:
        print(f"[RECENT LOW] {symbol} error: {e}")
        return None


# =====================================================
# POSITIONS & ORDERS
# =====================================================

def get_open_positions():
    try:
        body = {"timestamp": int(time.time() * 1000), "page": "1",
                "size": "50", "margin_currency_short_name": ["USDT"]}
        payload, headers = sign_request(body)
        positions = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/positions",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        ).json()
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
            body = {"timestamp": int(time.time() * 1000),
                    "status": "open,partially_filled", "side": side,
                    "page": "1", "size": "50",
                    "margin_currency_short_name": ["USDT"]}
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
        url  = (f"https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
                f"?pair={pair}&margin_currency_short_name=USDT")
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

def place_long_order(symbol, entry_price, tp_price, sl_price, precision, si=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    tp_pct = round(((tp - entry) / entry) * 100, 2)
    sl_pct = round(((entry - sl) / entry) * 100, 2)

    print(f"  [LONG] Entry={entry} TP={tp}(+{tp_pct}%) SL={sl}(-{sl_pct}%) Qty={qty}")

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side": "buy", "pair": fut_pair(symbol),
            "order_type": "limit_order", "price": entry,
            "total_quantity": qty, "leverage": LEVERAGE,
            "take_profit_price": tp, "stop_loss_price": sl,
        },
    }
    payload, headers = sign_request(body)
    try:
        result = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        ).json()
    except Exception as e:
        print(f"  [ERROR] order request failed: {e}")
        return False

    print(f"  [API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(
            f"❌ <b>LONG REJECTED — {symbol}</b>\n"
            f"Entry <code>{entry}</code> | TP <code>{tp}</code> | SL <code>{sl}</code>\n"
            f"<code>{str(result)[:200]}</code>"
        )
        return False

    si = si or {}
    send_telegram(
        f"🟢 <b>NEW LONG (PDL SWEEP) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry        : <code>{entry}</code>\n"
        f"🎯 TP           : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL           : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty          : <code>{qty}</code>\n"
        f"💰 Margin       : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Why this trade:</b>\n"
        f"📅 PDH <code>{si.get('pdh')}</code>  |  PDL <code>{si.get('pdl')}</code>\n"
        f"⬇️ PDL swept by <code>{si.get('sweep_ext')}</code> pts → low <code>{si.get('sweep_low')}</code>\n"
        f"🕯 Sweep candle : O=<code>{si.get('s_o')}</code> H=<code>{si.get('s_h')}</code> "
        f"L=<code>{si.get('s_l')}</code> C=<code>{si.get('s_c')}</code>\n"
        f"🟢 Candle 1     : O=<code>{si.get('c1_o')}</code> H=<code>{si.get('c1_h')}</code> "
        f"L=<code>{si.get('c1_l')}</code> C=<code>{si.get('c1_c')}</code>  (full-body bull)\n"
        f"🟢 Candle 2     : O=<code>{si.get('c2_o')}</code> H=<code>{si.get('c2_h')}</code> "
        f"L=<code>{si.get('c2_l')}</code> C=<code>{si.get('c2_c')}</code>  (full-body bull)\n"
        f"📌 Entry at close of candle 2"
    )
    return True


# =====================================================
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, si=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    tp_pct = round(((entry - tp) / entry) * 100, 2)
    sl_pct = round(((sl - entry) / entry) * 100, 2)

    print(f"  [SHORT] Entry={entry} TP={tp}(-{tp_pct}%) SL={sl}(+{sl_pct}%) Qty={qty}")

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side": "sell", "pair": fut_pair(symbol),
            "order_type": "limit_order", "price": entry,
            "total_quantity": qty, "leverage": LEVERAGE,
            "take_profit_price": tp, "stop_loss_price": sl,
        },
    }
    payload, headers = sign_request(body)
    try:
        result = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        ).json()
    except Exception as e:
        print(f"  [ERROR] order request failed: {e}")
        return False

    print(f"  [API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] short rejected: {result}")
        send_telegram(
            f"❌ <b>SHORT REJECTED — {symbol}</b>\n"
            f"Entry <code>{entry}</code> | TP <code>{tp}</code> | SL <code>{sl}</code>\n"
            f"<code>{str(result)[:200]}</code>"
        )
        return False

    si = si or {}
    send_telegram(
        f"🔴 <b>NEW SHORT (PDH SWEEP) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry        : <code>{entry}</code>\n"
        f"🎯 TP           : <code>{tp}</code>  (-{tp_pct}%)\n"
        f"🛑 SL           : <code>{sl}</code>  (+{sl_pct}%)\n"
        f"📦 Qty          : <code>{qty}</code>\n"
        f"💰 Margin       : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Why this trade:</b>\n"
        f"📅 PDH <code>{si.get('pdh')}</code>  |  PDL <code>{si.get('pdl')}</code>\n"
        f"⬆️ PDH swept by <code>{si.get('sweep_ext')}</code> pts → high <code>{si.get('sweep_high')}</code>\n"
        f"🕯 Sweep candle : O=<code>{si.get('s_o')}</code> H=<code>{si.get('s_h')}</code> "
        f"L=<code>{si.get('s_l')}</code> C=<code>{si.get('s_c')}</code>\n"
        f"🔴 Candle 1     : O=<code>{si.get('c1_o')}</code> H=<code>{si.get('c1_h')}</code> "
        f"L=<code>{si.get('c1_l')}</code> C=<code>{si.get('c1_c')}</code>  (full-body bear)\n"
        f"🔴 Candle 2     : O=<code>{si.get('c2_o')}</code> H=<code>{si.get('c2_h')}</code> "
        f"L=<code>{si.get('c2_l')}</code> C=<code>{si.get('c2_c')}</code>  (full-body bear)\n"
        f"📌 Entry at close of candle 2"
    )
    return True


# =====================================================
# SWEEP STATE CLEAR
# =====================================================

def _clear_sweep(st):
    st["sweep_direction"]   = None
    st["sweep_ts"]          = 0
    st["recent_swing_low"]  = None
    st["recent_swing_high"] = None
    st["sweep_o"] = None; st["sweep_h"] = None
    st["sweep_l"] = None; st["sweep_c"] = None
    st["candle1_ts"] = 0
    st["candle1_o"]  = None; st["candle1_h"] = None
    st["candle1_l"]  = None; st["candle1_c"] = None


def _reset_candle1(st):
    """Reset only candle1 — keep sweep state intact."""
    st["candle1_ts"] = 0
    st["candle1_o"]  = None; st["candle1_h"] = None
    st["candle1_l"]  = None; st["candle1_c"] = None


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    now_ms = int(time.time() * 1000)

    # ── 1. Daily candles → PDH / PDL ─────────────────────────────────────
    daily = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
    if daily and (now_ms - int(daily[-1]["time"])) < CANDLE_SECONDS_DAY * 1000:
        daily = daily[:-1]

    if not daily:
        print(f"  [{symbol}] SKIP — no completed daily candles")
        return

    prev_day  = daily[-1]
    pdh       = float(prev_day["high"])
    pdl       = float(prev_day["low"])
    precision = get_precision(prev_day["close"])

    # ── 2. State init / backfill ──────────────────────────────────────────
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset (00:00 UTC = 05:30 IST) ─────────────────────────
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — PDH={pdh} PDL={pdl}")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    st["pdh"]             = pdh
    st["pdl"]             = pdl

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
    if tp_raw.upper() == "TP COMPLETED":
        print(f"  [{symbol}] SKIP — TP COMPLETED in sheet")
        save_state(all_state)
        return

    try:
        tp_stored = float(tp_raw)
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored and tp_stored > 0:
        direction  = st.get("direction")
        tp_hit     = False
        hit_kind   = None
        hit_price  = None
        last_1m    = fetch_candles(symbol, 2, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        is_long    = direction == "long" or (direction is None and last_close and tp_stored > last_close)

        if is_long:
            if last_close and last_close >= tp_stored:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rh = get_recent_high(symbol)
                if rh and rh >= tp_stored:
                    tp_hit, hit_kind, hit_price = True, "wick", rh
        else:
            if last_close and last_close <= tp_stored:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rl = get_recent_low(symbol)
                if rl and rl <= tp_stored:
                    tp_hit, hit_kind, hit_price = True, "wick", rl

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"  [{symbol}] TP HIT ({hit_kind}) price={hit_price} target={tp_stored}")
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
            st["in_position"] = True
            st["direction"]   = "long" if active > 0 else "short"
            st["entry_price"] = entry_px
            print(f"  [{symbol}] RECONCILE — {st['direction']} found on exchange")

        tp_pos, sl_pos = extract_tp_sl(position)
        if st.get("tp_level") is None and tp_pos:
            st["tp_level"] = round(tp_pos, precision)
        if st.get("sl_price") is None and sl_pos:
            st["sl_price"] = round(sl_pos, precision)

        b_val = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
        c_val = str(df.iloc[row, 2]).strip() if df.shape[1] > 2 else ""
        if st.get("tp_level") and b_val == "":
            update_sheet_tp(row, st["tp_level"])
        if st.get("sl_price") and c_val == "":
            update_sheet_sl(row, st["sl_price"])

        save_state(all_state)
        return

    if st.get("in_position"):
        print(f"  [{symbol}] POSITION CLOSED — resetting state")
        prev_last = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"  [{symbol}] SKIP — open order on book")
        return

    # ── 6. 1h sweep guard — only closed 1h bars ───────────────────────────
    if not st["pdl_swept_1h"] or not st["pdh_swept_1h"]:
        candles_1h = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)
        # Drop in-progress 1h bar
        if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
            candles_1h = candles_1h[:-1]
        today_start_ms = int(datetime.strptime(today_str, "%Y-%m-%d")
                             .replace(tzinfo=timezone.utc).timestamp() * 1000)
        todays_1h = [c for c in candles_1h if int(c["time"]) >= today_start_ms]

        for c1h in todays_1h:
            if float(c1h["low"])  < pdl and not st["pdl_swept_1h"]:
                st["pdl_swept_1h"] = True
                print(f"  [{symbol}] 1H GUARD — PDL swept (low={c1h['low']} < PDL={pdl})")
            if float(c1h["high"]) > pdh and not st["pdh_swept_1h"]:
                st["pdh_swept_1h"] = True
                print(f"  [{symbol}] 1H GUARD — PDH swept (high={c1h['high']} > PDH={pdh})")

    if st["pdl_swept_1h"] and st["pdh_swept_1h"]:
        print(f"  [{symbol}] SKIP — both sides already swept on 1h today")
        save_state(all_state)
        return

    # ── 7. Fetch 1m candles ───────────────────────────────────────────────
    candles_1m = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
    if candles_1m and (now_ms - int(candles_1m[-1]["time"])) < CANDLE_SECONDS_1M * 1000:
        candles_1m = candles_1m[:-1]

    if len(candles_1m) < 5:
        print(f"  [{symbol}] SKIP — not enough 1m candles ({len(candles_1m)})")
        save_state(all_state)
        return

    last_processed = st.get("last_processed_1m_ts", 0)
    new_candles    = [c for c in candles_1m if int(c["time"]) > last_processed]

    c1_armed = st["candle1_ts"] > 0
    print(f"  [{symbol}] PDH={pdh} PDL={pdl} | sweep={st['sweep_direction'] or 'none'} "
          f"c1={'armed' if c1_armed else 'waiting'} | "
          f"1h_guard pdl={st['pdl_swept_1h']} pdh={st['pdh_swept_1h']} | "
          f"new_1m={len(new_candles)}")

    if not new_candles:
        save_state(all_state)
        return

    # ── 8. Walk 1m candles — sweep + 2 consecutive full-body candles ──────
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

        # ── A. No sweep yet ────────────────────────────────────────────
        if sweep_dir is None:
            if l < pdl and not st["pdl_swept_1h"]:
                st["sweep_direction"]  = "long"
                st["sweep_ts"]         = c_ts
                st["recent_swing_low"] = l
                st["sweep_o"] = o; st["sweep_h"] = h
                st["sweep_l"] = l; st["sweep_c"] = c
                print(f"  [{symbol}] SWEEP-LONG  low={l} < PDL={pdl} "
                      f"(ext={round(pdl-l, precision)})")

            elif h > pdh and not st["pdh_swept_1h"]:
                st["sweep_direction"]   = "short"
                st["sweep_ts"]          = c_ts
                st["recent_swing_high"] = h
                st["sweep_o"] = o; st["sweep_h"] = h
                st["sweep_l"] = l; st["sweep_c"] = c
                print(f"  [{symbol}] SWEEP-SHORT high={h} > PDH={pdh} "
                      f"(ext={round(h-pdh, precision)})")

        # ── B. Long sweep armed ────────────────────────────────────────
        elif sweep_dir == "long":
            # Track lowest low during sweep
            if l < st["recent_swing_low"]:
                st["recent_swing_low"] = l

            bars_since = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
            if bars_since > SWEEP_EXPIRY_BARS:
                print(f"  [{symbol}] SWEEP-EXPIRE long ({bars_since}b) — resetting")
                _clear_sweep(st)
                st["last_processed_1m_ts"] = c_ts
                continue

            if st["candle1_ts"] == 0:
                # Looking for FIRST full-body bullish candle
                if is_full_body_bullish(o, h, l, c):
                    st["candle1_ts"] = c_ts
                    st["candle1_o"]  = o; st["candle1_h"] = h
                    st["candle1_l"]  = l; st["candle1_c"] = c
                    print(f"  [{symbol}] CANDLE1-LONG full-body bull "
                          f"O={round(o,precision)} H={round(h,precision)} "
                          f"L={round(l,precision)} C={round(c,precision)}")
            else:
                # Candle 2 MUST be the immediately next 1m bar
                expected_ts = st["candle1_ts"] + CANDLE_SECONDS_1M * 1000
                if c_ts == expected_ts:
                    if is_full_body_bullish(o, h, l, c):
                        # ✅ Two consecutive full-body bull candles — ENTER LONG
                        entry_price  = c
                        natural_sl   = st["recent_swing_low"]
                        min_sl       = entry_price * (1 - MIN_SL_PCT / 100)
                        sl_price_val = min(natural_sl, min_sl)
                        tp_price_val = entry_price * (1 + TP_PCT / 100)
                        entry_path   = "long_sweep"
                        signal_info  = {
                            "pdh":        round(pdh, precision),
                            "pdl":        round(pdl, precision),
                            "sweep_ext":  round(pdl - st["recent_swing_low"], precision),
                            "sweep_low":  round(st["recent_swing_low"], precision),
                            "s_o": round(st["sweep_o"], precision),
                            "s_h": round(st["sweep_h"], precision),
                            "s_l": round(st["sweep_l"], precision),
                            "s_c": round(st["sweep_c"], precision),
                            "c1_o": round(st["candle1_o"], precision),
                            "c1_h": round(st["candle1_h"], precision),
                            "c1_l": round(st["candle1_l"], precision),
                            "c1_c": round(st["candle1_c"], precision),
                            "c2_o": round(o, precision),
                            "c2_h": round(h, precision),
                            "c2_l": round(l, precision),
                            "c2_c": round(c, precision),
                        }
                        st["last_processed_1m_ts"] = c_ts
                        break
                    else:
                        # Candle 2 not full-body — reset candle1, try again
                        print(f"  [{symbol}] CANDLE2-LONG failed (not full-body) — reset C1")
                        _reset_candle1(st)
                        # Check if this candle itself qualifies as a new candle1
                        if is_full_body_bullish(o, h, l, c):
                            st["candle1_ts"] = c_ts
                            st["candle1_o"]  = o; st["candle1_h"] = h
                            st["candle1_l"]  = l; st["candle1_c"] = c
                            print(f"  [{symbol}] CANDLE1-LONG (retry) "
                                  f"O={round(o,precision)} C={round(c,precision)}")
                else:
                    # Gap between candle1 and this candle — candle1 is stale, reset
                    print(f"  [{symbol}] CANDLE1-LONG stale (gap) — reset C1")
                    _reset_candle1(st)
                    if is_full_body_bullish(o, h, l, c):
                        st["candle1_ts"] = c_ts
                        st["candle1_o"]  = o; st["candle1_h"] = h
                        st["candle1_l"]  = l; st["candle1_c"] = c
                        print(f"  [{symbol}] CANDLE1-LONG (after gap) "
                              f"O={round(o,precision)} C={round(c,precision)}")

        # ── C. Short sweep armed ───────────────────────────────────────
        elif sweep_dir == "short":
            # Track highest high during sweep
            if h > st["recent_swing_high"]:
                st["recent_swing_high"] = h

            bars_since = (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_1M * 1000)
            if bars_since > SWEEP_EXPIRY_BARS:
                print(f"  [{symbol}] SWEEP-EXPIRE short ({bars_since}b) — resetting")
                _clear_sweep(st)
                st["last_processed_1m_ts"] = c_ts
                continue

            if st["candle1_ts"] == 0:
                # Looking for FIRST full-body bearish candle
                if is_full_body_bearish(o, h, l, c):
                    st["candle1_ts"] = c_ts
                    st["candle1_o"]  = o; st["candle1_h"] = h
                    st["candle1_l"]  = l; st["candle1_c"] = c
                    print(f"  [{symbol}] CANDLE1-SHORT full-body bear "
                          f"O={round(o,precision)} H={round(h,precision)} "
                          f"L={round(l,precision)} C={round(c,precision)}")
            else:
                expected_ts = st["candle1_ts"] + CANDLE_SECONDS_1M * 1000
                if c_ts == expected_ts:
                    if is_full_body_bearish(o, h, l, c):
                        # ✅ Two consecutive full-body bear candles — ENTER SHORT
                        entry_price  = c
                        natural_sl   = st["recent_swing_high"]
                        min_sl       = entry_price * (1 + MIN_SL_PCT / 100)
                        sl_price_val = max(natural_sl, min_sl)
                        tp_price_val = entry_price * (1 - TP_PCT / 100)
                        entry_path   = "short_sweep"
                        signal_info  = {
                            "pdh":        round(pdh, precision),
                            "pdl":        round(pdl, precision),
                            "sweep_ext":  round(st["recent_swing_high"] - pdh, precision),
                            "sweep_high": round(st["recent_swing_high"], precision),
                            "s_o": round(st["sweep_o"], precision),
                            "s_h": round(st["sweep_h"], precision),
                            "s_l": round(st["sweep_l"], precision),
                            "s_c": round(st["sweep_c"], precision),
                            "c1_o": round(st["candle1_o"], precision),
                            "c1_h": round(st["candle1_h"], precision),
                            "c1_l": round(st["candle1_l"], precision),
                            "c1_c": round(st["candle1_c"], precision),
                            "c2_o": round(o, precision),
                            "c2_h": round(h, precision),
                            "c2_l": round(l, precision),
                            "c2_c": round(c, precision),
                        }
                        st["last_processed_1m_ts"] = c_ts
                        break
                    else:
                        print(f"  [{symbol}] CANDLE2-SHORT failed (not full-body) — reset C1")
                        _reset_candle1(st)
                        if is_full_body_bearish(o, h, l, c):
                            st["candle1_ts"] = c_ts
                            st["candle1_o"]  = o; st["candle1_h"] = h
                            st["candle1_l"]  = l; st["candle1_c"] = c
                            print(f"  [{symbol}] CANDLE1-SHORT (retry) "
                                  f"O={round(o,precision)} C={round(c,precision)}")
                else:
                    print(f"  [{symbol}] CANDLE1-SHORT stale (gap) — reset C1")
                    _reset_candle1(st)
                    if is_full_body_bearish(o, h, l, c):
                        st["candle1_ts"] = c_ts
                        st["candle1_o"]  = o; st["candle1_h"] = h
                        st["candle1_l"]  = l; st["candle1_c"] = c
                        print(f"  [{symbol}] CANDLE1-SHORT (after gap) "
                              f"O={round(o,precision)} C={round(c,precision)}")

        st["last_processed_1m_ts"] = c_ts

    if entry_path is None:
        if new_candles:
            st["last_processed_1m_ts"] = int(new_candles[-1]["time"])
        save_state(all_state)
        return

    # ── 9. Validate SL ───────────────────────────────────────────────────
    if entry_path == "long_sweep" and sl_price_val >= entry_price:
        print(f"  [{symbol}] SKIP — invalid long SL (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return
    if entry_path == "short_sweep" and sl_price_val <= entry_price:
        print(f"  [{symbol}] SKIP — invalid short SL (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return

    if get_position_by_pair(symbol) is not None:
        print(f"  [{symbol}] ABORT — position appeared just before placement")
        return
    if has_open_order(symbol):
        print(f"  [{symbol}] ABORT — order appeared just before placement")
        return

    # ── 10. Place order ───────────────────────────────────────────────────
    if entry_path == "long_sweep":
        placed = place_long_order(symbol, entry_price, tp_price_val, sl_price_val, precision, signal_info)
    else:
        placed = place_short_order(symbol, entry_price, tp_price_val, sl_price_val, precision, signal_info)

    if placed:
        st["in_position"]   = True
        st["direction"]     = "long" if entry_path == "long_sweep" else "short"
        st["entry_price"]   = round(entry_price,  precision)
        st["tp_level"]      = round(tp_price_val, precision)
        st["sl_price"]      = round(sl_price_val, precision)
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
    f"📐 Strategy  : <code>PDH/PDL Liquidity Sweep (Long + Short)</code>\n"
    f"⚡ Entry     : <code>2 consecutive full-body 1m candles after sweep</code>\n"
    f"🔒 1h Guard  : <code>Skip if already swept on closed 1h today</code>\n"
    f"🔁 Scan      : <code>Every 90 seconds</code>\n"
    f"🎯 TP        : <code>entry ± {TP_PCT}%</code>\n"
    f"🛑 SL        : <code>sweep extreme (min {MIN_SL_PCT}% from entry)</code>\n"
    f"📏 Full-body : <code>wick ≤ {int(MAX_WICK_RATIO*100)}% of body</code>\n"
    f"⏳ Sweep exp : <code>{SWEEP_EXPIRY_BARS} × 1m bars</code>\n"
    f"💰 Capital   : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — retrying")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"\n===== CYCLE {cycle} | {datetime.utcnow().strftime('%H:%M:%S UTC')} =====")

        symbols_checked = 0
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if not symbol:
                continue
            symbols_checked += 1
            print(f"--- Row {row + 1}: {symbol} ---")
            try:
                check_and_trade(symbol, row, df, state)
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")
                continue

        print(f"===== CYCLE {cycle} DONE — {symbols_checked} symbols =====")
        save_state(state)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        consecutive_errors += 1
        print(f"BOT ERROR ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}")
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            send_telegram(
                f"🚨 <b>Bot Crashed</b>\n"
                f"❌ <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive errors"
            )
            raise SystemExit(1)
        time.sleep(60)