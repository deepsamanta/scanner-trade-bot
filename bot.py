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
# STRATEGY PARAMETERS
#
# DAILY BIAS  : Last 4 completed daily candles → pattern detection
# ENTRY       : 15m candles — C1 reversal + C2 confirmation
# TP          : Fixed +3% from entry
# SL          : Fixed -1.5% from entry
#
# DEPLOYMENT GUARD: No trades on the day the bot is first started.
#                   Trading begins from the NEXT calendar day (00:00 UTC).
#
# DAILY BIAS PATTERNS (d1=oldest, d2, d3, d4=most recent completed)
#
#   LONG  (priority order):
#     1. Morning Star        — d2 solid bearish, d3 small-body star, d4 bullish > d2 midpoint
#     2. Three Down + Hammer — d2+d3 bearish, d4 is Hammer or Dragonfly Doji
#     3. Bullish Engulfing   — d3 bearish, d4 bullish body ≥ 80% of d3
#     4. Piercing Line       — d3 bearish, d4 opens ≤ d3 close, closes above d3 midpoint
#     5. Bullish Harami      — d3 bearish, d4 small bullish body inside d3 body
#     6. Hammer / Doji on d4 — after 2+ consecutive red days (d2+d3 both bearish)
#     7. Three Black Crows   — d2+d3+d4 all bearish (exhaustion)
#     8. Four Red Days       — d1+d2+d3+d4 all bearish (extreme exhaustion)
#
#   SHORT (priority order):
#     1. Evening Star         — d2 solid bullish, d3 small-body star, d4 bearish < d2 midpoint
#     2. Three Up + Shoot     — d2+d3 bullish, d4 is Shooting Star or Gravestone Doji
#     3. Bearish Engulfing    — d3 bullish, d4 bearish body ≥ 80% of d3
#     4. Dark Cloud Cover     — d3 bullish, d4 opens ≥ d3 close, closes below d3 midpoint
#     5. Bearish Harami       — d3 bullish, d4 small bearish body inside d3 body
#     6. Shooting Star / Doji — after 2+ consecutive green days (d2+d3 both bullish)
#     7. Three White Soldiers — d2+d3+d4 all bullish (exhaustion)
#     8. Four Green Days      — d1+d2+d3+d4 all bullish (extreme exhaustion)
#
# ENTRY (15m):
#   C1 = 15m[-2] — reversal candle matching bias (Doji, Hammer, Shooting Star, Engulfing)
#   C2 = 15m[-1] — body ≥ 70% of range, closes beyond C1 close
#   Entry = limit order at C2 close
#   TP    = entry ± 3%  |  SL = entry ∓ 1.5%
# =============================================================================

TP_PCT         = 3.0
SL_PCT         = 1.5
MIN_BODY_PCT   = 70      # C2 body must be ≥ 70% of range

# Daily pattern thresholds
STAR_BODY_RATIO   = 0.30   # d3 body ≤ 30% of range → qualifies as "star"
SOLID_BODY_RATIO  = 0.40   # d2 body ≥ 40% of range → qualifies as "solid" candle
HARAMI_MAX_RATIO  = 0.40   # d4 body ≤ 40% of d3 body for Harami
ENGULF_MIN_RATIO  = 0.80   # d4 body ≥ 80% of d3 body for Engulfing

# 15m C1 thresholds
DOJI_BODY_RATIO  = 0.10
PIN_BODY_RATIO   = 0.35
WICK_MIN_RATIO   = 0.60

CANDLES_DAILY    = 8    # fetch 8, use last 4 completed
CANDLES_15M      = 10
CANDLES_1M       = 5

RESOLUTION_DAILY = "1D"
RESOLUTION_15M   = "15"
RESOLUTION_1M    = "1"

CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_15M = 900
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
        "last_c2_ts":         0,
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
# DAILY BIAS — 4-CANDLE PATTERN ANALYSIS
# d1=oldest, d2, d3, d4=most recent completed daily candle
# =====================================================

def get_daily_bias(d1, d2, d3, d4):
    """
    Analyse last 4 completed daily candles.
    Returns (bias, pattern_name): bias = 'long' | 'short' | None
    """
    def _parse(d):
        o = float(d["open"]); h = float(d["high"])
        l = float(d["low"]);  c = float(d["close"])
        rng  = h - l if h != l else 1e-10
        body = abs(c - o)
        return o, h, l, c, rng, body, c < o, c > o   # bearish, bullish flags

    d1_o, d1_h, d1_l, d1_c, d1_rng, d1_body, d1_bear, d1_bull = _parse(d1)
    d2_o, d2_h, d2_l, d2_c, d2_rng, d2_body, d2_bear, d2_bull = _parse(d2)
    d3_o, d3_h, d3_l, d3_c, d3_rng, d3_body, d3_bear, d3_bull = _parse(d3)
    d4_o, d4_h, d4_l, d4_c, d4_rng, d4_body, d4_bear, d4_bull = _parse(d4)

    d3_body_mid = (d3_o + d3_c) / 2
    d2_body_mid = (d2_o + d2_c) / 2

    d4_upper_wick  = d4_h - max(d4_o, d4_c)
    d4_lower_wick  = min(d4_o, d4_c) - d4_l
    d4_body_ratio  = d4_body / d4_rng
    d4_upper_ratio = d4_upper_wick / d4_rng
    d4_lower_ratio = d4_lower_wick / d4_rng

    d3_body_ratio  = d3_body / d3_rng
    d2_body_ratio  = d2_body / d2_rng

    # ── LONG patterns (priority order) ────────────────────────────────────

    # 1. Morning Star (3-candle):
    #    d2 solid bearish → d3 small-body star (indecision) → d4 bullish closing above d2 midpoint
    if (d2_bear and d2_body_ratio >= SOLID_BODY_RATIO
            and d3_body_ratio <= STAR_BODY_RATIO
            and d4_bull and d4_c > d2_body_mid):
        return "long", "Morning Star"

    # 2. Three Down + Hammer/Doji on d4:
    #    d2+d3 both bearish, d4 shows bullish reversal wick pattern
    if d2_bear and d3_bear:
        # Dragonfly Doji on d4
        if d4_body_ratio <= DOJI_BODY_RATIO and d4_lower_ratio >= WICK_MIN_RATIO:
            return "long", "Three Down + Dragonfly Doji"
        # Hammer on d4
        if (d4_body_ratio <= PIN_BODY_RATIO
                and d4_lower_ratio >= WICK_MIN_RATIO
                and d4_upper_ratio <= 0.15
                and d4_c >= (d4_h + d4_l) / 2):
            return "long", "Three Down + Hammer"

    # 3. Bullish Engulfing (d3+d4):
    #    d3 bearish, d4 bullish body engulfs d3 body (≥ 80%)
    if (d3_bear and d4_bull
            and d4_o <= d3_c and d4_c >= d3_o
            and d4_body >= d3_body * ENGULF_MIN_RATIO):
        return "long", "Bullish Engulfing"

    # 4. Piercing Line (d3+d4):
    #    d3 bearish, d4 opens at/below d3 close, closes above d3 body midpoint (not full engulf)
    if (d3_bear and d4_bull
            and d4_o <= d3_c
            and d4_c > d3_body_mid
            and d4_c < d3_o):
        return "long", "Piercing Line"

    # 5. Bullish Harami (d3+d4):
    #    d3 bearish, d4 small bullish body inside d3 body
    if (d3_bear and d4_bull
            and d3_c <= d4_o <= d3_o
            and d3_c <= d4_c <= d3_o
            and d4_body <= d3_body * HARAMI_MAX_RATIO):
        return "long", "Bullish Harami"

    # 6. Hammer / Dragonfly Doji on d4 after 2 consecutive red days
    if d2_bear and d3_bear and not d2_bull:
        if d4_body_ratio <= DOJI_BODY_RATIO and d4_lower_ratio >= WICK_MIN_RATIO:
            return "long", "Two Red + Dragonfly Doji"
        if (d4_body_ratio <= PIN_BODY_RATIO
                and d4_lower_ratio >= WICK_MIN_RATIO
                and d4_upper_ratio <= 0.15
                and d4_c >= (d4_h + d4_l) / 2):
            return "long", "Two Red + Hammer"

    # 7. Three Black Crows (d2+d3+d4 all bearish) — exhaustion, mean-reversion long
    if d2_bear and d3_bear and d4_bear:
        return "long", "Three Black Crows"

    # 8. Four Red Days (d1+d2+d3+d4 all bearish) — extreme exhaustion
    if d1_bear and d2_bear and d3_bear and d4_bear:
        return "long", "Four Red Days"

    # ── SHORT patterns (priority order) ───────────────────────────────────

    # 1. Evening Star (3-candle):
    #    d2 solid bullish → d3 small-body star → d4 bearish closing below d2 midpoint
    if (d2_bull and d2_body_ratio >= SOLID_BODY_RATIO
            and d3_body_ratio <= STAR_BODY_RATIO
            and d4_bear and d4_c < d2_body_mid):
        return "short", "Evening Star"

    # 2. Three Up + Shooting Star / Gravestone Doji on d4
    if d2_bull and d3_bull:
        # Gravestone Doji on d4
        if d4_body_ratio <= DOJI_BODY_RATIO and d4_upper_ratio >= WICK_MIN_RATIO:
            return "short", "Three Up + Gravestone Doji"
        # Shooting Star on d4
        if (d4_body_ratio <= PIN_BODY_RATIO
                and d4_upper_ratio >= WICK_MIN_RATIO
                and d4_lower_ratio <= 0.15
                and d4_c <= (d4_h + d4_l) / 2):
            return "short", "Three Up + Shooting Star"

    # 3. Bearish Engulfing (d3+d4)
    if (d3_bull and d4_bear
            and d4_o >= d3_c and d4_c <= d3_o
            and d4_body >= d3_body * ENGULF_MIN_RATIO):
        return "short", "Bearish Engulfing"

    # 4. Dark Cloud Cover (d3+d4)
    if (d3_bull and d4_bear
            and d4_o >= d3_c
            and d4_c < d3_body_mid
            and d4_c > d3_o):
        return "short", "Dark Cloud Cover"

    # 5. Bearish Harami (d3+d4)
    if (d3_bull and d4_bear
            and d3_o <= d4_c <= d3_c
            and d3_o <= d4_o <= d3_c
            and d4_body <= d3_body * HARAMI_MAX_RATIO):
        return "short", "Bearish Harami"

    # 6. Shooting Star / Gravestone Doji on d4 after 2 consecutive green days
    if d2_bull and d3_bull and not d2_bear:
        if d4_body_ratio <= DOJI_BODY_RATIO and d4_upper_ratio >= WICK_MIN_RATIO:
            return "short", "Two Green + Gravestone Doji"
        if (d4_body_ratio <= PIN_BODY_RATIO
                and d4_upper_ratio >= WICK_MIN_RATIO
                and d4_lower_ratio <= 0.15
                and d4_c <= (d4_h + d4_l) / 2):
            return "short", "Two Green + Shooting Star"

    # 7. Three White Soldiers (d2+d3+d4 all bullish) — exhaustion, mean-reversion short
    if d2_bull and d3_bull and d4_bull:
        return "short", "Three White Soldiers"

    # 8. Four Green Days — extreme exhaustion
    if d1_bull and d2_bull and d3_bull and d4_bull:
        return "short", "Four Green Days"

    return None, None


# =====================================================
# 15m C1 REVERSAL CHECKS
# =====================================================

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


# =====================================================
# CANDLE FETCHER
# =====================================================

def fetch_candles(symbol, num_candles, resolution_str, candle_seconds):
    url    = "https://public.coindcx.com/market_data/candlesticks"
    now    = int(time.time())
    params = {
        "pair":       fut_pair(symbol),
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


# =====================================================
# QUANTITY
# =====================================================

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
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
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
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


def place_short_order(symbol, entry_price, tp_price, sl_price, precision):
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
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
        f"🛑 SL    : <code>{sl}</code>  (+{sl_pct}%)\n"
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
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── DEPLOYMENT GUARD ──────────────────────────────────────────────────
    # If today == the day the bot was first started, skip all trading.
    # This prevents entering mid-day on an incomplete/unknown candle context.
    bot_start_date = all_state.get("bot_start_date")
    if bot_start_date and today_str == bot_start_date:
        print(f"  [{symbol}] SKIP — deployment day ({bot_start_date}), trading starts tomorrow")
        return

    # ── 1. Daily candles → 4-candle bias ─────────────────────────────────
    daily = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
    # Drop in-progress bar
    if daily and (now_ms - int(daily[-1]["time"])) < CANDLE_SECONDS_DAY * 1000:
        daily = daily[:-1]

    if len(daily) < 4:
        print(f"  [{symbol}] SKIP — not enough completed daily candles ({len(daily)})")
        return

    d1, d2, d3, d4    = daily[-4], daily[-3], daily[-2], daily[-1]
    precision          = get_precision(float(d4["close"]))
    bias, bias_pattern = get_daily_bias(d1, d2, d3, d4)

    # ── 2. State init / backfill ──────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset (00:00 UTC) ──────────────────────────────────────
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

    # ── 7. Fetch last 3 completed 15m candles (prev, C1, C2) ─────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    # Drop in-progress bar
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]

    if len(candles_15m) < 3:
        print(f"  [{symbol}] SKIP — not enough 15m candles ({len(candles_15m)})")
        save_state(all_state)
        return

    prev_c = candles_15m[-3]
    c1     = candles_15m[-2]
    c2     = candles_15m[-1]
    c2_ts  = int(c2["time"])

    # Dedup — already acted on this C2
    if c2_ts <= st.get("last_c2_ts", 0):
        print(f"  [{symbol}] SKIP — C2 already processed")
        save_state(all_state)
        return

    c1_o = float(c1["open"]);  c1_h = float(c1["high"])
    c1_l = float(c1["low"]);   c1_c = float(c1["close"])
    c2_o = float(c2["open"]);  c2_h = float(c2["high"])
    c2_l = float(c2["low"]);   c2_c = float(c2["close"])
    p_o  = float(prev_c["open"]); p_c = float(prev_c["close"])

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
        c1_match, c1_pattern = is_bullish_reversal_c1(c1_o, c1_h, c1_l, c1_c, p_o, p_c)
        if not c1_match:
            print(f"  [{symbol}] C1 not a bullish reversal — skip")
            save_state(all_state)
            return
        body_pct = round((c2_c - c2_o) / (c2_h - c2_l) * 100, 1) if c2_h != c2_l else 0
        if is_strong_bullish(c2_o, c2_h, c2_l, c2_c) and c2_c > c1_c:
            entry_price  = c2_c
            sl_price_val = entry_price * (1 - SL_PCT / 100)
            tp_price_val = entry_price * (1 + TP_PCT / 100)
            entry_path   = "long_trend"
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-LONG confirmed body={body_pct}%")
        else:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-LONG failed body={body_pct}% "
                  f"C2={round(c2_c,precision)} C1={round(c1_c,precision)}")

    elif bias == "short":
        c1_match, c1_pattern = is_bearish_reversal_c1(c1_o, c1_h, c1_l, c1_c, p_o, p_c)
        if not c1_match:
            print(f"  [{symbol}] C1 not a bearish reversal — skip")
            save_state(all_state)
            return
        body_pct = round((c2_o - c2_c) / (c2_h - c2_l) * 100, 1) if c2_h != c2_l else 0
        if is_strong_bearish(c2_o, c2_h, c2_l, c2_c) and c2_c < c1_c:
            entry_price  = c2_c
            sl_price_val = entry_price * (1 + SL_PCT / 100)
            tp_price_val = entry_price * (1 - TP_PCT / 100)
            entry_path   = "short_trend"
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-SHORT confirmed body={body_pct}%")
        else:
            print(f"  [{symbol}] C1=[{c1_pattern}] C2-SHORT failed body={body_pct}% "
                  f"C2={round(c2_c,precision)} C1={round(c1_c,precision)}")

    # Always mark C2 as seen
    st["last_c2_ts"] = c2_ts

    if entry_path is None:
        save_state(all_state)
        return

    # ── 9. Place order ────────────────────────────────────────────────────
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
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

# ── Record deployment date (once, on first ever start) ────────────────────────
_boot_state = load_state()
if "bot_start_date" not in _boot_state:
    _boot_state["bot_start_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    save_state(_boot_state)
    print(f"[BOOT] Deployment date set: {_boot_state['bot_start_date']} — no trades today")
else:
    print(f"[BOOT] Deployment date already recorded: {_boot_state['bot_start_date']}")

send_telegram(
    f"✅ <b>Daily Trend Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🚫 Deployment guard : <code>No trades on {_boot_state['bot_start_date']} (UTC)</code>\n"
    f"📐 Strategy         : <code>4-Day Bias + 15m C1/C2 Reversal</code>\n"
    f"\n"
    f"📈 LONG Bias  :\n"
    f"  <code>① Morning Star</code>\n"
    f"  <code>② Three Down + Hammer/Doji</code>\n"
    f"  <code>③ Bullish Engulfing  ④ Piercing Line</code>\n"
    f"  <code>⑤ Bullish Harami     ⑥ Two Red + Hammer/Doji</code>\n"
    f"  <code>⑦ Three Black Crows  ⑧ Four Red Days</code>\n"
    f"\n"
    f"📉 SHORT Bias :\n"
    f"  <code>① Evening Star</code>\n"
    f"  <code>② Three Up + Shooting Star/Doji</code>\n"
    f"  <code>③ Bearish Engulfing  ④ Dark Cloud Cover</code>\n"
    f"  <code>⑤ Bearish Harami     ⑥ Two Green + Shooting Star/Doji</code>\n"
    f"  <code>⑦ Three White Soldiers  ⑧ Four Green Days</code>\n"
    f"\n"
    f"⚡ C1 (15m)   : <code>Doji / Hammer / Pin Bar / Shooting Star / Engulfing</code>\n"
    f"⚡ C2 (15m)   : <code>body ≥ {MIN_BODY_PCT}%, closes beyond C1 close</code>\n"
    f"🎯 TP         : <code>+{TP_PCT}% fixed</code>\n"
    f"🛑 SL         : <code>-{SL_PCT}% fixed</code>\n"
    f"🔁 Scan       : <code>Every {SCAN_INTERVAL}s</code>\n"
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
            print("[WARN] API fetch failed — skipping cycle")
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