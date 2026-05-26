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
# STRATEGY PARAMETERS  (Daily Trend/Engulfing — LONG + SHORT)
#
# TIMEFRAMES:
#   - Daily Trend     : 2 completed daily candles
#   - Entry logic     : 5m candles  (last 2 completed candles only)
#   - TP wick detect  : 1m candles
#
# DAILY BIAS (Last 2 completed daily candles) — priority ordered:
#   LONG  → Bullish Engulfing, Piercing Line, Bullish Harami,
#            Hammer, Dragonfly Doji (after bearish/doji D1),
#            Two Red Days
#   SHORT → Bearish Engulfing, Dark Cloud Cover, Bearish Harami,
#            Shooting Star, Gravestone Doji (after bullish/doji D1),
#            Two Green Days
#
# ENTRY (5m):
#   C1 = candles_5m[-2] — reversal candle matching bias
#   C2 = candles_5m[-1] — body ≥ 70% of range, closes beyond C1 close
#   SL  = C1 low (long) / C1 high (short), min MIN_SL_PCT% from entry
#   TP  = entry ± TP_PCT%
# =============================================================================

TP_PCT            = 1.25
MIN_SL_PCT        = 0.5
MIN_BODY_PCT      = 70

DOJI_BODY_RATIO   = 0.10
PIN_BODY_RATIO    = 0.35
WICK_MIN_RATIO    = 0.60
HARAMI_MAX_BODY_RATIO = 0.40

CANDLES_DAILY     = 5
CANDLES_5M        = 10     # only need last few completed 5m candles
CANDLES_1M        = 5

RESOLUTION_DAILY  = "1D"
RESOLUTION_5M     = "5"
RESOLUTION_1M     = "1"

CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_5M  = 300
CANDLE_SECONDS_1M  = 60

SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "daily_trend_state.json"


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
        "in_position":        False,
        "direction":          None,
        "entry_price":        None,
        "tp_level":           None,
        "sl_price":           None,
        "last_entry_ts":      0,
        "current_day_str":    None,
        "daily_bias":         None,
        "daily_bias_pattern": None,
        "last_c2_ts":         0,    # ts of last C2 candle we acted on (dedup)
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
                print(f"[TELEGRAM] Non-200: {r.status_code}")
                return
    except Exception as e:
        print(f"[TELEGRAM] Failed: {e}")


# =====================================================
# GLOBAL BATCH FETCHERS
# =====================================================

def get_all_positions():
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "page": "1", "size": "100",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        r = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/positions",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[API ERROR] positions: HTTP {r.status_code}")
            return None
        data      = r.json()
        positions = data if isinstance(data, list) else data.get("data", [])
        if not isinstance(positions, list):
            return None
        active = []
        for p in positions:
            qty = str(p.get("size") or p.get("active_pos") or p.get("net_size") or "0")
            if abs(float(qty)) > 0:
                active.append(p)
        return active
    except Exception as e:
        print(f"[API ERROR] get_all_positions: {e}")
        return None


def get_all_open_orders():
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "status": "open,partially_filled",
            "page": "1", "size": "100",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        r = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[API ERROR] orders: HTTP {r.status_code}")
            return None
        data   = r.json()
        orders = data if isinstance(data, list) else data.get("data", [])
        if not isinstance(orders, list):
            return None
        return orders
    except Exception as e:
        print(f"[API ERROR] get_all_open_orders: {e}")
        return None


# =====================================================
# HELPERS
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    return len(s.split(".")[1]) if "." in s else 0


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


# ── Candle body checks ────────────────────────────────────────────────────────

def is_strong_bullish(o, h, l, c):
    if c <= o:
        return False
    rng = h - l
    if rng == 0:
        return False
    return ((c - o) / rng * 100) >= MIN_BODY_PCT


def is_strong_bearish(o, h, l, c):
    if c >= o:
        return False
    rng = h - l
    if rng == 0:
        return False
    return ((o - c) / rng * 100) >= MIN_BODY_PCT


# =====================================================
# DAILY BIAS — 10-PATTERN ANALYSIS
# =====================================================

def get_daily_bias(d1, d2):
    d1_o = float(d1["open"]);  d1_h = float(d1["high"])
    d1_l = float(d1["low"]);   d1_c = float(d1["close"])
    d2_o = float(d2["open"]);  d2_h = float(d2["high"])
    d2_l = float(d2["low"]);   d2_c = float(d2["close"])

    d1_rng  = d1_h - d1_l if d1_h != d1_l else 1e-10
    d2_rng  = d2_h - d2_l if d2_h != d2_l else 1e-10
    d1_body = abs(d1_c - d1_o)
    d2_body = abs(d2_c - d2_o)

    d1_bearish = d1_c < d1_o
    d1_bullish = d1_c > d1_o
    d2_bearish = d2_c < d2_o
    d2_bullish = d2_c > d2_o

    d1_body_mid = (d1_o + d1_c) / 2

    d2_upper_wick  = d2_h - max(d2_o, d2_c)
    d2_lower_wick  = min(d2_o, d2_c) - d2_l
    d2_body_ratio  = d2_body / d2_rng
    d2_upper_ratio = d2_upper_wick / d2_rng
    d2_lower_ratio = d2_lower_wick / d2_rng

    # ── LONG ──────────────────────────────────────────────────────────────
    if (d1_bearish and d2_bullish
            and d2_o <= d1_c and d2_c >= d1_o
            and d2_body >= d1_body * 0.80):
        return "long", "Bullish Engulfing"

    if (d1_bearish and d2_bullish
            and d2_o <= d1_c
            and d2_c > d1_body_mid
            and d2_c < d1_o):
        return "long", "Piercing Line"

    if (d1_bearish and d2_bullish
            and d1_c <= d2_o <= d1_o
            and d1_c <= d2_c <= d1_o
            and d2_body <= d1_body * HARAMI_MAX_BODY_RATIO):
        return "long", "Bullish Harami"

    if not d1_bullish:
        if d2_body_ratio <= DOJI_BODY_RATIO and d2_lower_ratio >= WICK_MIN_RATIO:
            return "long", "Dragonfly Doji"
        if (d2_body_ratio <= PIN_BODY_RATIO
                and d2_lower_ratio >= WICK_MIN_RATIO
                and d2_upper_ratio <= 0.15
                and d2_c >= (d2_h + d2_l) / 2):
            return "long", "Hammer"

    if d1_bearish and d2_bearish:
        return "long", "Two Red Days"

    # ── SHORT ─────────────────────────────────────────────────────────────
    if (d1_bullish and d2_bearish
            and d2_o >= d1_c and d2_c <= d1_o
            and d2_body >= d1_body * 0.80):
        return "short", "Bearish Engulfing"

    if (d1_bullish and d2_bearish
            and d2_o >= d1_c
            and d2_c < d1_body_mid
            and d2_c > d1_o):
        return "short", "Dark Cloud Cover"

    if (d1_bullish and d2_bearish
            and d1_o <= d2_c <= d1_c
            and d1_o <= d2_o <= d1_c
            and d2_body <= d1_body * HARAMI_MAX_BODY_RATIO):
        return "short", "Bearish Harami"

    if not d1_bearish:
        if d2_body_ratio <= DOJI_BODY_RATIO and d2_upper_ratio >= WICK_MIN_RATIO:
            return "short", "Gravestone Doji"
        if (d2_body_ratio <= PIN_BODY_RATIO
                and d2_upper_ratio >= WICK_MIN_RATIO
                and d2_lower_ratio <= 0.15
                and d2_c <= (d2_h + d2_l) / 2):
            return "short", "Shooting Star"

    if d1_bullish and d2_bullish:
        return "short", "Two Green Days"

    return None, None


# ── 5m C1 reversal checks ─────────────────────────────────────────────────────

def is_bullish_reversal_c1(o, h, l, c, prev_o=None, prev_c=None):
    rng = h - l
    if rng == 0:
        return False, None
    body        = abs(c - o)
    body_ratio  = body / rng
    lower_wick  = min(o, c) - l
    upper_wick  = h - max(o, c)
    lower_ratio = lower_wick / rng
    upper_ratio = upper_wick / rng

    if body_ratio <= DOJI_BODY_RATIO and lower_ratio >= WICK_MIN_RATIO:
        return True, "Dragonfly Doji"

    if (body_ratio <= PIN_BODY_RATIO
            and lower_ratio >= WICK_MIN_RATIO
            and upper_ratio <= 0.15
            and c >= (h + l) / 2):
        return True, "Hammer/Pin Bar"

    if (prev_o is not None and prev_c is not None
            and prev_c < prev_o
            and c > o
            and o <= prev_c
            and c >= prev_o):
        return True, "Bullish Engulfing"

    return False, None


def is_bearish_reversal_c1(o, h, l, c, prev_o=None, prev_c=None):
    rng = h - l
    if rng == 0:
        return False, None
    body        = abs(c - o)
    body_ratio  = body / rng
    lower_wick  = min(o, c) - l
    upper_wick  = h - max(o, c)
    lower_ratio = lower_wick / rng
    upper_ratio = upper_wick / rng

    if body_ratio <= DOJI_BODY_RATIO and upper_ratio >= WICK_MIN_RATIO:
        return True, "Gravestone Doji"

    if (body_ratio <= PIN_BODY_RATIO
            and upper_ratio >= WICK_MIN_RATIO
            and lower_ratio <= 0.15
            and c <= (h + l) / 2):
        return True, "Shooting Star/Pin Bar"

    if (prev_o is not None and prev_c is not None
            and prev_c > prev_o
            and c < o
            and o >= prev_c
            and c <= prev_o):
        return True, "Bearish Engulfing"

    return False, None


def fetch_candles(symbol, num_candles, resolution_str, candle_seconds):
    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    params   = {
        "pair":       pair_api,
        "from":       now - (num_candles + 5) * candle_seconds,
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


def get_recent_high(symbol):
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL,
                  "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get(
            "https://public.coindcx.com/market_data/candlesticks",
            params=params, timeout=REQUEST_TIMEOUT,
        ).json().get("data", [])
        return max(float(c["high"]) for c in candles) if candles else None
    except Exception:
        return None


def get_recent_low(symbol):
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL,
                  "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get(
            "https://public.coindcx.com/market_data/candlesticks",
            params=params, timeout=REQUEST_TIMEOUT,
        ).json().get("data", [])
        return min(float(c["low"]) for c in candles) if candles else None
    except Exception:
        return None


# ── Quantity ──────────────────────────────────────────────────────────────────

def get_quantity_step(symbol):
    try:
        pair = fut_pair(symbol)
        url  = (f"https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
                f"?pair={pair}&margin_currency_short_name=USDT")
        instrument = requests.get(url, timeout=REQUEST_TIMEOUT).json()["instrument"]
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
# PLACE ORDERS
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision):
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
        print(f"  [ERROR] order failed: {e}")
        return False

    print(f"  [API] {symbol}: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False

    send_telegram(
        f"🟢 <b>NEW LONG (DAILY TREND) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)  ← C1 low\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


def place_short_order(symbol, entry_price, tp_price, sl_price, precision):
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
        print(f"  [ERROR] order failed: {e}")
        return False

    print(f"  [API] {symbol}: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] short rejected: {result}")
        send_telegram(f"❌ <b>SHORT REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False

    send_telegram(
        f"🔴 <b>NEW SHORT (DAILY TREND) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (-{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (+" + f"{sl_pct}%)  ← C1 high\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)

    # ── 1. Daily candles → bias ───────────────────────────────────────────
    daily = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
    if daily and (now_ms - int(daily[-1]["time"])) < CANDLE_SECONDS_DAY * 1000:
        daily = daily[:-1]
    if len(daily) < 2:
        print(f"  [{symbol}] SKIP — not enough completed daily candles")
        return

    d1 = daily[-2]
    d2 = daily[-1]
    precision          = get_precision(float(d2["close"]))
    bias, bias_pattern = get_daily_bias(d1, d2)

    # ── 2. State init / backfill ──────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset ──────────────────────────────────────────────────
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — Bias: {bias or 'NONE'} ({bias_pattern or '-'})")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"]    = today_str
    st["daily_bias"]         = bias
    st["daily_bias_pattern"] = bias_pattern

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
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        is_long    = (direction == "long" or
                      (direction is None and last_close and tp_stored > last_close))

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
    position = next((p for p in global_positions if p.get("pair") == pair_name), None)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or 0)
            qty_str  = str(position.get("size") or position.get("active_pos") or
                           position.get("net_size") or "0")
            st["in_position"] = True
            st["direction"]   = "long" if float(qty_str) > 0 else "short"
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

    has_order = any(o.get("pair") == pair_name for o in global_orders)
    if has_order:
        print(f"  [{symbol}] SKIP — open order on book")
        return

    # ── 6. Bias check ─────────────────────────────────────────────────────
    if not bias:
        print(f"  [{symbol}] SKIP — no daily bias pattern")
        save_state(all_state)
        return

    # ── 7. Fetch last 3 completed 5m candles (prev, C1, C2) ──────────────
    candles_5m = fetch_candles(symbol, CANDLES_5M, RESOLUTION_5M, CANDLE_SECONDS_5M)
    # Drop in-progress bar
    if candles_5m and (now_ms - int(candles_5m[-1]["time"])) < CANDLE_SECONDS_5M * 1000:
        candles_5m = candles_5m[:-1]

    if len(candles_5m) < 3:
        print(f"  [{symbol}] SKIP — not enough 5m candles ({len(candles_5m)})")
        save_state(all_state)
        return

    prev_c = candles_5m[-3]   # candle before C1 (needed for Engulfing check on C1)
    c1     = candles_5m[-2]
    c2     = candles_5m[-1]

    c2_ts = int(c2["time"])

    # Dedup: if we already acted on this C2 candle, skip
    if c2_ts <= st.get("last_c2_ts", 0):
        print(f"  [{symbol}] SKIP — C2 already processed (ts={c2_ts})")
        save_state(all_state)
        return

    c1_o = float(c1["open"]);  c1_h = float(c1["high"])
    c1_l = float(c1["low"]);   c1_c = float(c1["close"])
    c2_o = float(c2["open"]);  c2_h = float(c2["high"])
    c2_l = float(c2["low"]);   c2_c = float(c2["close"])
    prev_o = float(prev_c["open"]); prev_cl = float(prev_c["close"])

    print(f"  [{symbol}] Bias={bias.upper()} ({bias_pattern}) | "
          f"C1: O={round(c1_o,precision)} H={round(c1_h,precision)} "
          f"L={round(c1_l,precision)} C={round(c1_c,precision)} | "
          f"C2: O={round(c2_o,precision)} C={round(c2_c,precision)}")

    # ── 8. Check C1 + C2 ─────────────────────────────────────────────────
    entry_price  = None
    sl_price_val = None
    tp_price_val = None
    entry_path   = None

    if bias == "long":
        c1_match, c1_pattern = is_bullish_reversal_c1(c1_o, c1_h, c1_l, c1_c, prev_o, prev_cl)
        if not c1_match:
            print(f"  [{symbol}] C1 not a bullish reversal — skip")
            save_state(all_state)
            return
        body_pct = round((c2_c - c2_o) / (c2_h - c2_l) * 100, 1) if c2_h != c2_l else 0
        if is_strong_bullish(c2_o, c2_h, c2_l, c2_c) and c2_c > c1_c:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-LONG confirmed body={body_pct}%")
            entry_price  = c2_c
            natural_sl   = c1_l
            min_sl       = entry_price * (1 - MIN_SL_PCT / 100)
            sl_price_val = min(natural_sl, min_sl)
            tp_price_val = entry_price * (1 + TP_PCT / 100)
            entry_path   = "long_trend"
        else:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-LONG failed body={body_pct}% C2={round(c2_c,precision)} C1={round(c1_c,precision)}")

    elif bias == "short":
        c1_match, c1_pattern = is_bearish_reversal_c1(c1_o, c1_h, c1_l, c1_c, prev_o, prev_cl)
        if not c1_match:
            print(f"  [{symbol}] C1 not a bearish reversal — skip")
            save_state(all_state)
            return
        body_pct = round((c2_o - c2_c) / (c2_h - c2_l) * 100, 1) if c2_h != c2_l else 0
        if is_strong_bearish(c2_o, c2_h, c2_l, c2_c) and c2_c < c1_c:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-SHORT confirmed body={body_pct}%")
            entry_price  = c2_c
            natural_sl   = c1_h
            min_sl       = entry_price * (1 + MIN_SL_PCT / 100)
            sl_price_val = max(natural_sl, min_sl)
            tp_price_val = entry_price * (1 - TP_PCT / 100)
            entry_path   = "short_trend"
        else:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-SHORT failed body={body_pct}% C2={round(c2_c,precision)} C1={round(c1_c,precision)}")

    if entry_path is None:
        save_state(all_state)
        return

    # ── 9. Validate SL ────────────────────────────────────────────────────
    if entry_path == "long_trend" and sl_price_val >= entry_price:
        print(f"  [{symbol}] SKIP — invalid long SL (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return
    if entry_path == "short_trend" and sl_price_val <= entry_price:
        print(f"  [{symbol}] SKIP — invalid short SL (entry={entry_price} SL={sl_price_val})")
        save_state(all_state)
        return

    # ── 10. Place order ───────────────────────────────────────────────────
    if entry_path == "long_trend":
        placed = place_long_order(symbol, entry_price, tp_price_val, sl_price_val, precision)
    else:
        placed = place_short_order(symbol, entry_price, tp_price_val, sl_price_val, precision)

    if placed:
        st["in_position"]   = True
        st["direction"]     = "long" if entry_path == "long_trend" else "short"
        st["entry_price"]   = round(entry_price,  precision)
        st["tp_level"]      = round(tp_price_val, precision)
        st["sl_price"]      = round(sl_price_val, precision)
        st["last_entry_ts"] = c2_ts
        st["last_c2_ts"]    = c2_ts
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    # Always mark this C2 as seen regardless of order result
    st["last_c2_ts"] = c2_ts
    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Daily Trend Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy   : <code>Daily 2-Candle Bias + 5m C1/C2 Reversal</code>\n"
    f"\n"
    f"📈 LONG Bias  :\n"
    f"  <code>① Bullish Engulfing  ② Piercing Line</code>\n"
    f"  <code>③ Bullish Harami     ④ Hammer / Dragonfly Doji</code>\n"
    f"  <code>⑤ Two Red Days</code>\n"
    f"\n"
    f"📉 SHORT Bias :\n"
    f"  <code>① Bearish Engulfing  ② Dark Cloud Cover</code>\n"
    f"  <code>③ Bearish Harami     ④ Shooting Star / Gravestone Doji</code>\n"
    f"  <code>⑤ Two Green Days</code>\n"
    f"\n"
    f"⚡ C1 (5m)    : <code>Doji / Hammer / Pin Bar / Shooting Star / Engulfing</code>\n"
    f"⚡ C2 (5m)    : <code>body ≥ {MIN_BODY_PCT}%, closes beyond C1 close</code>\n"
    f"🔁 Scan       : <code>Every {SCAN_INTERVAL}s</code>\n"
    f"🎯 TP         : <code>entry ± {TP_PCT}%</code>\n"
    f"🛑 SL         : <code>C1 low/high, min {MIN_SL_PCT}% from entry</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()
        if df.empty:
            print("[WARN] Sheet returned empty — retrying")
            time.sleep(SCAN_INTERVAL)
            continue

        global_positions = get_all_positions()
        global_orders    = get_all_open_orders()

        if global_positions is None or global_orders is None:
            print("[WARN] API fetch failed — skipping cycle to protect state")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"\n===== CYCLE {cycle} | {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} "
              f"| positions={len(global_positions)} orders={len(global_orders)} =====")

        symbols_checked = 0
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if not symbol:
                continue
            symbols_checked += 1
            print(f"--- Row {row + 1}: {symbol} ---")
            try:
                check_and_trade(symbol, row, df, state, global_positions, global_orders)
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