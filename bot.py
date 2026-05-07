import math
import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import os
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY: LuxAlgo "Trendlines with Breaks" + Anti-Fakeout (SHORT only)
#
# CORE:
#   - On 4h closed bars, fit decaying trendlines from pivot highs (upper) and
#     pivot lows (lower) using ATR-based slope decay.
#   - SHORT signal = redB (close breaks below the lower trendline that bar)
#     filtered by: body break, strong-bar, volume confirmation, ATR distance,
#     and per-bar dedupe (cooldown).
#   - Entry  = bar close
#   - SL     = lowerLvl × (1 + SL_BUFFER_TL_PCT%)   — above broken trendline
#   - TP     = entry    × (1 − TP_PCT_FIXED%)       — fixed % below entry
# =============================================================================

# ─── Trendline core ──────────────────────────────────────────────────────────
TL_LENGTH        = 14            # swing lookback (pivot strength + atr period)
TL_SLOPE_MULT    = 1.0           # slope multiplier
TL_CALC_METHOD   = "Atr"         # 'Atr' | 'Stdev' | 'Linreg'

# ─── SL / TP geometry (fixed) ────────────────────────────────────────────────
# TP : entry × (1 − TP_PCT_FIXED/100)            → fixed % below entry
# SL : lower_lvl × (1 + SL_BUFFER_TL_PCT/100)    → fixed % above the broken
#                                                  trendline (now resistance)
TP_PCT_FIXED     = 3.0
SL_BUFFER_TL_PCT = 1.5

# ─── Anti-fakeout filters ────────────────────────────────────────────────────
TL_USE_BODY_BREAK = True
TL_USE_STRONG_BAR = True
TL_MIN_BODY_PCT   = 50.0
TL_USE_VOLUME     = True
TL_VOL_MULT       = 1.2
TL_USE_ATR_DIST   = True
TL_ATR_MULT       = 0.25
TL_USE_COOLDOWN   = True
TL_COOLDOWN_BARS  = 5

# ─── Path A — Retest after failed break ──────────────────────────────────────
# Arms when redB fires this bar BUT filters fail. Watches subsequent bars for
# a retest of lowerLvl from below (high pokes back up to the line) followed by
# a close below it. Filters are NOT required on the entry bar.
PATH_A_ENABLED              = True
PATH_A_MAX_WAIT_BARS        = 10     # disarm if no retest within N bars
PATH_A_RETEST_TOLERANCE_PCT = 0.3    # bar high reaches within −0.3% of lowerLvl
PATH_A_INVALIDATION_PCT     = 1.0    # close > lowerLvl × (1 + 1.0%) → disarm

# ─── Path B — Acceptance / grind-down ────────────────────────────────────────
# Tracks consecutive 4h closes below lowerLvl. Once >= threshold, considered
# "accepted below". Enters on first subsequent bearish bar (close < open) below
# the line. Fully independent of whether the core break ever fired. Resets if
# any close reclaims lowerLvl.
PATH_B_ENABLED         = True
PATH_B_ACCEPTANCE_BARS = 3

# ─── Risk sanity ─────────────────────────────────────────────────────────────
# If lowerLvl is far above current price, SL = lowerLvl × (1+SL_BUFFER_TL_PCT%)
# can be huge, producing a terrible implied RR with the fixed 3% TP. Skip any
# setup whose SL distance from entry exceeds this cap.
MAX_SL_DISTANCE_PCT = 8.0

# ─── Timeframe / scan ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY = "240"
CANDLE_SECONDS     = 4 * 3600
SCAN_INTERVAL      = 1800
TL_CANDLES_NEEDED  = 200

# ─── Misc / IO ───────────────────────────────────────────────────────────────
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "short_bot_state.json"
# =============================================================================


# =====================================================
# GOOGLE SHEETS — with periodic re-auth
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
        sheet.update(f"B{row + 1}", [[str(value)]])
        print(f"[SHEET] Row {row + 1} col B -> {value}")
    except Exception as e:
        print("Sheet update error:", e)


def update_sheet_sl(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update(f"C{row + 1}", [[str(value)]])
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
        # ── Trendline-break state ─────────────────────
        "tl_last_signal_ts": None,   # ts of last bar that produced a SHORT entry
        # ── Path A: pending retest after failed break ─
        "path_a_armed":      False,
        "path_a_arm_ts":     None,
        "path_a_bars_armed": 0,
        # ── Path B: acceptance / grind-down ───────────
        "path_b_consecutive_below": 0,
        # ── Position state ────────────────────────────
        "in_position":  False,
        "entry_path":   None,
        "entry_price":  None,
        "tp_level":     None,
        "sl_price":     None,
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
# SIGN REQUEST / TELEGRAM / PRECISION
# =====================================================

def sign_request(body):
    payload   = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(
        bytes(COINDCX_SECRET, encoding="utf-8"),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type":     "application/json",
        "X-AUTH-APIKEY":    COINDCX_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    return payload, headers


def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        r = requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
        if r.status_code != 200:
            print(f"[TELEGRAM] Non-200 response {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# CANDLE FETCH (4h primary)
# =====================================================

def fetch_candles(symbol, num_candles_needed, resolution_str=None, candle_seconds=None):
    if resolution_str is None:
        resolution_str = RESOLUTION_PRIMARY
    if candle_seconds is None:
        candle_seconds = CANDLE_SECONDS

    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
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
# RECENT LOW (TP wick detection between scans)
# =====================================================

def get_recent_low(symbol):
    try:
        pair_api = fut_pair(symbol)
        url  = "https://public.coindcx.com/market_data/candlesticks"
        now  = int(time.time())
        params = {
            "pair":       pair_api,
            "from":       now - SCAN_INTERVAL,
            "to":         now,
            "resolution": "1",
            "pcode":      "f",
        }
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = response.json().get("data", [])
        if not candles:
            return None
        return min(float(c["low"]) for c in candles)
    except Exception as e:
        print(f"[RECENT LOW] {symbol} error: {e}")
        return None


# =====================================================
# INDICATORS (helpers for trendline state)
# =====================================================

def calc_true_range(highs, lows, closes):
    n = len(closes)
    if n == 0:
        return []
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1]),
        ))
    return tr


def calc_rma(values, period):
    """Wilder's RMA — matches Pine ta.atr / ta.rma."""
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    initial = sum(values[:period]) / period
    out[period - 1] = initial
    for i in range(period, n):
        out[i] = (out[i-1] * (period - 1) + values[i]) / period
    return out


def calc_sma(values, period):
    n = len(values)
    out = [None] * n
    s = 0.0
    for i in range(n):
        s += values[i]
        if i >= period:
            s -= values[i - period]
        if i >= period - 1:
            out[i] = s / period
    return out


def calc_stdev(values, period):
    """Population stdev — matches Pine ta.stdev default."""
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    sma = calc_sma(values, period)
    for i in range(period - 1, n):
        m = sma[i]
        var = sum((values[j] - m) ** 2 for j in range(i - period + 1, i + 1)) / period
        out[i] = math.sqrt(var)
    return out


# =====================================================
# TRENDLINE STATE (LuxAlgo Trendlines with Breaks)
# =====================================================

def compute_trendline_state(candles, length=TL_LENGTH, mult=TL_SLOPE_MULT, method=TL_CALC_METHOD):
    """
    Streaming replication of the Pine logic. Returns per-bar arrays plus latest
    pivot references. Last index = most recent CLOSED bar.
    """
    n = len(candles)
    if n < length * 3 + 5:
        return None

    opens   = [float(c["open"])   for c in candles]
    highs   = [float(c["high"])   for c in candles]
    lows    = [float(c["low"])    for c in candles]
    closes  = [float(c["close"])  for c in candles]
    volumes = [float(c.get("volume", 0)) for c in candles]

    # Per-bar slope value
    if method == "Stdev":
        sd = calc_stdev(closes, length)
        slopes = [(s / length * mult) if s is not None else 0.0 for s in sd]
    elif method == "Linreg":
        # simplified linreg-slope proxy (rare path); fall back to ATR shape
        tr = calc_true_range(highs, lows, closes)
        atr_len = calc_rma(tr, length)
        slopes = [(a / length * mult) if a is not None else 0.0 for a in atr_len]
    else:  # 'Atr'
        tr = calc_true_range(highs, lows, closes)
        atr_len = calc_rma(tr, length)
        slopes = [(a / length * mult) if a is not None else 0.0 for a in atr_len]

    # Pivot confirmations: ta.pivothigh(length, length) — pivot at bar (i-length)
    # is reported at bar i. We populate the *confirmation bar*.
    #
    # IMPORTANT: pivots are detected against BODY extremes, not wicks. A bar's
    # candidate value is min(open, close) for a pivot low and max(open, close)
    # for a pivot high. This prevents single-bar wick spikes from resetting the
    # trendline anchor — the anchor only moves when an actual close-based swing
    # forms.
    body_highs = [max(opens[k], closes[k]) for k in range(n)]
    body_lows  = [min(opens[k], closes[k]) for k in range(n)]

    ph_event = [None] * n
    pl_event = [None] * n
    for i in range(length, n - length):
        confirm = i + length
        if confirm >= n:
            break
        bh, bl = body_highs[i], body_lows[i]
        is_ph, is_pl = True, True
        for k in range(1, length + 1):
            if body_highs[i-k] >= bh or body_highs[i+k] >= bh:
                is_ph = False
                break
        if is_ph:
            ph_event[confirm] = bh
        for k in range(1, length + 1):
            if body_lows[i-k] <= bl or body_lows[i+k] <= bl:
                is_pl = False
                break
        if is_pl:
            pl_event[confirm] = bl

    upper = None
    lower = None
    slope_ph = 0.0
    slope_pl = 0.0
    last_ph = None
    last_pl = None
    last_pl_idx = None     # bar index of the most recent CONFIRMED pivot low
    last_ph_idx = None     # bar index of the most recent CONFIRMED pivot high
    upos = 0
    dnos = 0

    green_b   = [False] * n
    red_b     = [False] * n
    upper_lvl = [None]  * n
    lower_lvl = [None]  * n

    for i in range(n):
        slope = slopes[i]
        ph    = ph_event[i]
        pl    = pl_event[i]

        if ph is not None: slope_ph = slope
        if pl is not None: slope_pl = slope

        if ph is not None:
            upper = ph
        elif upper is not None:
            upper = upper - slope_ph

        if pl is not None:
            lower = pl
        elif lower is not None:
            lower = lower + slope_pl

        if ph is not None:
            last_ph = ph
            last_ph_idx = i - length     # actual swing bar (confirmation is `length` bars later)
        if pl is not None:
            last_pl = pl
            last_pl_idx = i - length     # actual swing bar (confirmation is `length` bars later)

        prev_upos, prev_dnos = upos, dnos
        c = closes[i]

        if upper is not None:
            if ph is not None:
                upos = 0
            elif c > upper - slope_ph * length:
                upos = 1

        if lower is not None:
            if pl is not None:
                dnos = 0
            elif c < lower + slope_pl * length:
                dnos = 1

        green_b[i] = upos > prev_upos
        red_b[i]   = dnos > prev_dnos

        if upper is not None: upper_lvl[i] = upper - slope_ph * length
        if lower is not None: lower_lvl[i] = lower + slope_pl * length

    return {
        "opens":     opens,
        "highs":     highs,
        "lows":      lows,
        "closes":    closes,
        "volumes":   volumes,
        "green_b":   green_b,
        "red_b":     red_b,
        "upper_lvl": upper_lvl,
        "lower_lvl": lower_lvl,
        "last_ph":     last_ph,
        "last_pl":     last_pl,
        "last_ph_idx": last_ph_idx,
        "last_pl_idx": last_pl_idx,
        "lower_now":   lower,            # running anchor (pivot value + slope × bars_since)
        "slope_pl_now": slope_pl,        # current per-bar slope
    }


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
        url      = BASE_URL + "/exchange/v1/derivatives/futures/positions"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        positions = response.json()
        if not isinstance(positions, list):
            return []
        return [p for p in positions if float(p.get("active_pos", 0)) != 0]
    except Exception as e:
        print("get_open_positions error:", e)
        return []


def get_position_by_pair(symbol):
    positions = get_open_positions()
    pair = fut_pair(symbol)
    for p in positions:
        if p.get("pair") == pair:
            return p
    return None


def has_open_order(symbol):
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "status":                     "open,partially_filled",
            "side":                       "sell",
            "page":                       "1",
            "size":                       "50",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        orders   = response.json()

        if not isinstance(orders, list):
            print(f"[has_open_order] {symbol} unexpected response: {orders}")
            return False

        pair = fut_pair(symbol)
        for o in orders:
            if o.get("pair") == pair:
                return True
        return False
    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


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
        response   = requests.get(url, timeout=REQUEST_TIMEOUT)
        data       = response.json()
        instrument = data["instrument"]
        quantity_increment = Decimal(str(instrument["quantity_increment"]))
        min_quantity       = Decimal(str(instrument["min_quantity"]))
        return max(quantity_increment, min_quantity)
    except Exception:
        return Decimal("1")


def compute_qty(entry_price, symbol):
    step     = get_quantity_step(symbol)
    capital  = Decimal(str(CAPITAL_USDT))
    leverage = Decimal(str(LEVERAGE))
    exposure = capital * leverage
    raw_qty  = exposure / Decimal(str(entry_price))
    qty = (raw_qty / step).quantize(Decimal("1")) * step
    if qty <= 0:
        qty = step
    qty = qty.quantize(step)
    return float(qty)


# =====================================================
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, entry_path, trigger_details=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)

    if sl <= entry:
        print(f"[SKIP] {symbol} [{entry_path}] SL {sl} not above entry {entry}")
        send_telegram(
            f"⚠️ <b>SHORT SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>SL {sl} not above entry {entry}</code>"
        )
        return False
    if tp >= entry:
        print(f"[SKIP] {symbol} [{entry_path}] TP {tp} not below entry {entry}")
        return False

    qty = compute_qty(entry_price, symbol)
    tp_pct_display = round(((entry - tp) / entry) * 100, 2) if entry else 0
    sl_pct_display = round(((sl - entry) / entry) * 100, 2) if entry else 0

    print(
        f"[SHORT TRADE] {symbol} SELL ({entry_path}) | Entry {entry} | "
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
        response = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        result = response.json()
    except Exception as e:
        print(f"[ERROR] {symbol} order request failed: {e}")
        return False

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} short order not placed: {result}")
        send_telegram(
            f"❌ <b>SHORT ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path     : <code>{entry_path}</code>\n"
            f"📍 Entry    : <code>{entry}</code>\n"
            f"🎯 TP       : <code>{tp}</code>\n"
            f"🛑 SL       : <code>{sl}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return False

    msg = (
        f"🔴 <b>NEW SHORT ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry  : <code>{entry}</code>\n"
        f"🎯 TP     : <code>{tp}</code>  (-{tp_pct_display}%)\n"
        f"🛑 SL     : <code>{sl}</code>  (+{sl_pct_display}%)\n"
        f"📦 Qty    : <code>{qty}</code>\n"
        f"💰 Margin : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    if trigger_details:
        msg += f"\n━━━━━━━━━━━━━━━━━━\n🧠 <b>Trigger conditions ({entry_path})</b>"
        for label, value in trigger_details:
            msg += f"\n{label} : <code>{value}</code>"
    send_telegram(msg)
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    candles = fetch_candles(symbol, TL_CANDLES_NEEDED)

    if len(candles) < TL_LENGTH * 3 + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    # Drop in-progress 4h bar
    if len(candles) >= 2:
        now_ms = int(time.time() * 1000)
        last_candle_time = int(candles[-1]["time"])
        bar_elapsed_ms = now_ms - last_candle_time
        if bar_elapsed_ms < CANDLE_SECONDS * 1000:
            candles = candles[:-1]

    if len(candles) < TL_LENGTH * 3 + 5:
        print(f"[SKIP] {symbol} — insufficient closed candles ({len(candles)})")
        return

    precision  = get_precision(candles[-1]["close"])
    last_close = float(candles[-1]["close"])
    last_ts    = int(candles[-1]["time"])

    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # =========================================================================
    # TP COMPLETED MONITORING
    # =========================================================================
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED marker, not re-entering")
        save_state(all_state)
        return

    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        tp_hit, hit_kind, hit_price = False, None, None
        if last_close <= tp_stored:
            tp_hit, hit_kind, hit_price = True, "close", last_close
        if not tp_hit:
            recent_low = get_recent_low(symbol)
            if recent_low is not None and recent_low <= tp_stored:
                tp_hit, hit_kind, hit_price = True, "wick", recent_low
        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} — {hit_kind} {hit_price} ≤ stored TP {tp_stored}")
            send_telegram(
                f"🎯 <b>TP HIT ({hit_kind}) — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 {hit_kind.capitalize():8}: <code>{hit_price}</code>\n"
                f"🎯 TP       : <code>{tp_stored}</code>\n"
                f"✅ Marked <b>TP COMPLETED</b>"
            )
            if st.get("in_position"):
                all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close)
            st["in_position"] = True
            st["entry_path"]  = st.get("entry_path") or "unknown"
            st["entry_price"] = entry_px
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")
        save_state(all_state)
        return

    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")
        send_telegram(
            f"✅ <b>POSITION CLOSED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path   : <code>{st.get('entry_path')}</code>\n"
            f"📍 Entry  : <code>{st.get('entry_price')}</code>\n"
            f"🎯 TP was : <code>{st.get('tp_level')}</code>\n"
            f"🛑 SL was : <code>{st.get('sl_price')}</code>"
        )
        last_sig = st.get("tl_last_signal_ts")          # preserve cooldown anchor
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["tl_last_signal_ts"] = last_sig
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # COMPUTE TRENDLINE STATE
    # =========================================================================
    state = compute_trendline_state(candles)
    if state is None:
        print(f"[SKIP] {symbol} — trendline state not ready")
        return

    i         = len(candles) - 1
    red_b     = state["red_b"][i]
    lower_lvl = state["lower_lvl"][i]
    last_ph   = state["last_ph"]
    last_pl   = state["last_pl"]

    # ── Build "TL anchor" descriptor ──────────────────────────────────────────
    # Shows which pivot low is anchoring the lower trendline at this bar, the
    # slope being applied, and how many bars of decay have accumulated.
    last_pl_idx  = state["last_pl_idx"]
    slope_pl_now = state["slope_pl_now"]
    if last_pl_idx is not None and last_pl is not None:
        bars_since_pl = i - last_pl_idx
        pl_ts_ms      = int(candles[last_pl_idx]["time"])
        # CoinDCX returns ms timestamps; hours since pivot:
        pl_age_hours  = (last_ts - pl_ts_ms) / 1000 / 3600
        decay         = slope_pl_now * bars_since_pl
        tl_anchor_str = (
            f"PL {round(last_pl, precision)} @ -{bars_since_pl}b "
            f"(~{int(pl_age_hours)}h ago) + slope {slope_pl_now:.2e}/bar × "
            f"{bars_since_pl}b = decay {round(decay, precision)} → lowerLvl {round(lower_lvl, precision)}"
        )
    else:
        tl_anchor_str = "n/a (no confirmed pivot low yet)"

    # =========================================================================
    # ENTRY EVALUATION — three independent paths
    #
    #   Path 0  ("tl_break")  — redB this bar  AND  ALL filters pass
    #                           (body break, strong bar, volume, ATR distance)
    #   Path A  ("tl_retest") — armed by an earlier redB whose filters failed;
    #                           fires when a later bar's high retests lowerLvl
    #                           from below and closes back beneath it.
    #                           Filters are NOT required.
    #   Path B  ("tl_accept") — independent of Path 0 / A. Tracks consecutive
    #                           closes below lowerLvl; once the count reaches
    #                           PATH_B_ACCEPTANCE_BARS, fires on the first
    #                           bearish bar (close < open) below the line.
    #                           Filters are NOT required.
    #
    # Cooldown applies globally. SL / TP geometry is identical across paths
    # (entry = close, SL = lowerLvl × (1+SL_BUFFER_TL_PCT%), TP = entry × (1−TP_PCT_FIXED%)).
    # =========================================================================
    if lower_lvl is None or last_ph is None:
        print(
            f"[SCAN] {symbol} | close={last_close} | trendline state incomplete "
            f"(lowerLvl={lower_lvl}, lastPH={last_ph})"
        )
        save_state(all_state)
        return

    last_open   = state["opens"][i]
    last_high   = state["highs"][i]
    last_low    = state["lows"][i]
    last_volume = state["volumes"][i]

    # ── Cooldown gate (shared across all paths) ──────────────────────────────
    last_sig_ts = st.get("tl_last_signal_ts")
    if not TL_USE_COOLDOWN or last_sig_ts is None:
        f_cooldown      = True
        bars_since_disp = "∞"
    else:
        bars_since      = max(0, (last_ts - last_sig_ts) // (CANDLE_SECONDS * 1000))
        f_cooldown      = bars_since >= TL_COOLDOWN_BARS
        bars_since_disp = int(bars_since)

    # ── Filters — ONLY used by Path 0 (core break) ──────────────────────────
    body_break_dn = max(last_open, last_close) < lower_lvl
    f_body = (not TL_USE_BODY_BREAK) or body_break_dn

    bar_range = last_high - last_low
    bar_body  = abs(last_close - last_open)
    body_pct  = (bar_body / bar_range * 100) if bar_range > 0 else 0
    f_strong  = (not TL_USE_STRONG_BAR) or body_pct >= TL_MIN_BODY_PCT

    vol_sma = calc_sma(state["volumes"], 20)
    f_vol   = (not TL_USE_VOLUME) or (vol_sma[i] is not None and last_volume > vol_sma[i] * TL_VOL_MULT)

    tr     = calc_true_range(state["highs"], state["lows"], state["closes"])
    atr14  = calc_rma(tr, 14)
    f_atr  = (not TL_USE_ATR_DIST) or (atr14[i] is not None and last_close < lower_lvl - atr14[i] * TL_ATR_MULT)

    filters_pass = f_body and f_strong and f_vol and f_atr

    # ── Path 0 candidate ─────────────────────────────────────────────────────
    core_sig = red_b and filters_pass

    # ── Path A candidate (retest) — NO filters ───────────────────────────────
    path_a_armed     = st.get("path_a_armed", False)
    path_a_bars      = st.get("path_a_bars_armed", 0)
    retest_floor     = lower_lvl * (1 - PATH_A_RETEST_TOLERANCE_PCT / 100.0)
    retested         = last_high >= retest_floor                # high climbed back near/over line
    closed_below_tl  = last_close < lower_lvl                   # rejected back beneath it
    retest_sig       = PATH_A_ENABLED and path_a_armed and retested and closed_below_tl

    # ── Path B candidate (acceptance) — NO filters ───────────────────────────
    # Strictly post-break: the redB bar itself routes to Path 0 (if filters pass)
    # or to Path A arming (if filters fail), never to Path B.
    prev_consec_b = st.get("path_b_consecutive_below", 0)
    new_consec_b  = prev_consec_b + 1 if closed_below_tl else 0
    accept_sig    = (PATH_B_ENABLED
                     and not red_b
                     and new_consec_b >= PATH_B_ACCEPTANCE_BARS
                     and last_close < last_open)                # bearish continuation bar

    # ── Pick the first path that fires ───────────────────────────────────────
    chosen, trigger_details = None, None
    if f_cooldown:
        if core_sig:
            chosen = "tl_break"
        elif retest_sig:
            chosen = "tl_retest"
        elif accept_sig:
            chosen = "tl_accept"

    print(
        f"[TL-EVAL] {symbol} | redB={red_b} filters={filters_pass} "
        f"(body={f_body} strong={f_strong}({round(body_pct,1)}%) vol={f_vol} atr={f_atr}) | "
        f"Path A armed={path_a_armed}({path_a_bars}b) retest={retest_sig} | "
        f"Path B count={new_consec_b}/{PATH_B_ACCEPTANCE_BARS} accept={accept_sig} | "
        f"cool={f_cooldown}({bars_since_disp}b) → chosen={chosen} | "
        f"lowerLvl={round(lower_lvl, precision)} lastPH={round(last_ph, precision)}"
    )

    # =========================================================================
    # ENTRY (if a path was chosen)
    # =========================================================================
    if chosen is not None:
        entry_price = last_close
        sl_price    = lower_lvl * (1 + SL_BUFFER_TL_PCT / 100)
        tp_price    = entry_price * (1 - TP_PCT_FIXED / 100)
        risk        = sl_price - entry_price

        if risk <= 0:
            print(
                f"[SKIP] {symbol} [{chosen}] invalid risk: SL {round(sl_price, precision)} "
                f"≤ entry {round(entry_price, precision)} (lowerLvl too close / below entry)"
            )
        else:
            sl_distance_pct = (sl_price - entry_price) / entry_price * 100

            # ── Risk sanity gate ────────────────────────────────────────────
            # Skip if SL is too far above entry (RR becomes terrible with the
            # fixed 3% TP). Disarm the path that produced the bad geometry so
            # we don't retry the same setup every bar.
            if sl_distance_pct > MAX_SL_DISTANCE_PCT:
                reason = f"SL distance {sl_distance_pct:.2f}% &gt; cap {MAX_SL_DISTANCE_PCT}%"
                print(
                    f"[SKIP-RISK] {symbol} [{chosen}] {reason} | "
                    f"entry={round(entry_price, precision)} sl={round(sl_price, precision)} "
                    f"tp={round(tp_price, precision)} lastPH={round(last_ph, precision)}"
                )
                send_telegram(
                    f"⏭️ <b>SHORT SKIPPED — {symbol}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"🛤 Path        : <code>{chosen}</code>\n"
                    f"📍 Entry       : <code>{round(entry_price, precision)}</code>\n"
                    f"🛑 SL          : <code>{round(sl_price, precision)}</code>  "
                    f"(+{sl_distance_pct:.2f}%)\n"
                    f"🎯 TP would be : <code>{round(tp_price, precision)}</code>\n"
                    f"🔺 lastPH      : <code>{round(last_ph, precision)}</code>\n"
                    f"❌ Reason      : <code>{reason}</code>"
                )
                # Disarm the offending path so we don't retry the same setup
                if chosen == "tl_retest":
                    st["path_a_armed"]      = False
                    st["path_a_arm_ts"]     = None
                    st["path_a_bars_armed"] = 0
                elif chosen == "tl_accept":
                    st["path_b_consecutive_below"] = 0
                # (For tl_break we don't disarm anything — the next redB with a
                # fresher lastPH may produce a tradeable setup.)
                save_state(all_state)
                return

            # Race-condition guards
            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            # Build path-specific trigger details for telegram
            if chosen == "tl_break":
                vol_thresh = (vol_sma[i] * TL_VOL_MULT) if vol_sma[i] is not None else None
                atr_thresh = (lower_lvl - atr14[i] * TL_ATR_MULT) if atr14[i] is not None else None
                trigger_details = [
                    ("📉 redB",         "True"),
                    ("🧱 lowerLvl",     round(lower_lvl, precision)),
                    ("🔺 lastPH",       round(last_ph, precision)),
                    ("🪜 TL anchor",    tl_anchor_str),
                    ("🪵 Body break",   f"{f_body}  (max(o,c)={round(max(last_open,last_close), precision)} &lt; lowerLvl)"),
                    ("💪 Strong bar",   f"{f_strong}  (body {round(body_pct,1)}% ≥ {TL_MIN_BODY_PCT}%)"),
                    ("📊 Volume",       f"{f_vol}  (vol={round(last_volume,2)} vs SMA20×{TL_VOL_MULT}={round(vol_thresh,2) if vol_thresh else 'N/A'})"),
                    ("📐 ATR distance", f"{f_atr}  (close &lt; lowerLvl − ATR14×{TL_ATR_MULT} = {round(atr_thresh, precision) if atr_thresh else 'N/A'})"),
                    ("⏳ Cooldown",     f"{f_cooldown}  ({bars_since_disp} bars since last entry, need ≥{TL_COOLDOWN_BARS})"),
                    ("⚖️ Risk",         f"SL +{sl_distance_pct:.2f}% / TP −{TP_PCT_FIXED}% (RR {TP_PCT_FIXED/sl_distance_pct:.2f}×)"),
                ]
            elif chosen == "tl_retest":
                trigger_details = [
                    ("🧱 lowerLvl",        round(lower_lvl, precision)),
                    ("🔺 lastPH",          round(last_ph, precision)),
                    ("🪜 TL anchor",       tl_anchor_str),
                    ("⏱ Bars armed",      f"{path_a_bars} of {PATH_A_MAX_WAIT_BARS}"),
                    ("📍 Retest high",     f"{round(last_high, precision)}  (floor {round(retest_floor, precision)}, tol ±{PATH_A_RETEST_TOLERANCE_PCT}%)"),
                    ("📉 Closed below TL", f"{round(last_close, precision)} &lt; {round(lower_lvl, precision)}"),
                    ("🚫 Filters",         "skipped (Path A independent)"),
                    ("⏳ Cooldown",         f"{f_cooldown}  ({bars_since_disp} bars since last entry, need ≥{TL_COOLDOWN_BARS})"),
                    ("⚖️ Risk",            f"SL +{sl_distance_pct:.2f}% / TP −{TP_PCT_FIXED}% (RR {TP_PCT_FIXED/sl_distance_pct:.2f}×)"),
                ]
            else:  # tl_accept
                trigger_details = [
                    ("🧱 lowerLvl",         round(lower_lvl, precision)),
                    ("🔺 lastPH",           round(last_ph, precision)),
                    ("🪜 TL anchor",        tl_anchor_str),
                    ("📊 Consec. below TL", f"{new_consec_b}  (≥ {PATH_B_ACCEPTANCE_BARS})"),
                    ("🕯 Bearish bar",      f"close {round(last_close, precision)} &lt; open {round(last_open, precision)}"),
                    ("🚫 Filters",          "skipped (Path B independent)"),
                    ("⏳ Cooldown",          f"{f_cooldown}  ({bars_since_disp} bars since last entry, need ≥{TL_COOLDOWN_BARS})"),
                    ("⚖️ Risk",             f"SL +{sl_distance_pct:.2f}% / TP −{TP_PCT_FIXED}% (RR {TP_PCT_FIXED/sl_distance_pct:.2f}×)"),
                ]

            placed = place_short_order(
                symbol, entry_price, tp_price, sl_price, precision, chosen, trigger_details
            )
            if placed:
                # Cooldown anchor + clear all arm states
                st["tl_last_signal_ts"]        = last_ts
                st["path_a_armed"]             = False
                st["path_a_arm_ts"]            = None
                st["path_a_bars_armed"]        = 0
                st["path_b_consecutive_below"] = new_consec_b   # keep accurate; cooldown blocks re-fire
                # Position state
                st["in_position"] = True
                st["entry_path"]  = chosen
                st["entry_price"] = round(entry_price, precision)
                st["tp_level"]    = round(tp_price,    precision)
                st["sl_price"]    = round(sl_price,    precision)
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["sl_price"])
                save_state(all_state)
                return

    # =========================================================================
    # NO ENTRY — update arm states for next bar
    # =========================================================================
    # Path A arming / lifecycle
    if PATH_A_ENABLED:
        if red_b and not filters_pass:
            # New break with failed filters — arm (or re-arm) Path A
            st["path_a_armed"]      = True
            st["path_a_arm_ts"]     = last_ts
            st["path_a_bars_armed"] = 0
            print(
                f"[PATH-A] {symbol} — ARMED (redB but filters failed: "
                f"body={f_body} strong={f_strong} vol={f_vol} atr={f_atr})"
            )
        elif st.get("path_a_armed"):
            st["path_a_bars_armed"] = st.get("path_a_bars_armed", 0) + 1
            # Invalidation — close reclaimed the trendline
            if last_close > lower_lvl * (1 + PATH_A_INVALIDATION_PCT / 100):
                print(
                    f"[PATH-A] {symbol} — DISARM (close {last_close} reclaimed lowerLvl "
                    f"{round(lower_lvl, precision)} by >{PATH_A_INVALIDATION_PCT}%)"
                )
                st["path_a_armed"]      = False
                st["path_a_arm_ts"]     = None
                st["path_a_bars_armed"] = 0
            elif st["path_a_bars_armed"] >= PATH_A_MAX_WAIT_BARS:
                print(f"[PATH-A] {symbol} — DISARM (timed out at {PATH_A_MAX_WAIT_BARS} bars)")
                st["path_a_armed"]      = False
                st["path_a_arm_ts"]     = None
                st["path_a_bars_armed"] = 0

    # Path B counter — always tracked
    st["path_b_consecutive_below"] = new_consec_b

    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>SHORT Bot Started — TL Break (3 paths)</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy : <code>LuxAlgo Trendlines with Breaks (SHORT)</code>\n"
    f"⏱ Analysis : <code>4h closed bars</code>\n"
    f"🔁 Scan     : <code>Every 30 minutes</code>\n"
    f"📏 Length   : <code>{TL_LENGTH} (slope mult {TL_SLOPE_MULT}, method {TL_CALC_METHOD})</code>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🅿️ <b>Path 0 (tl_break)</b> — <code>redB + ALL filters</code>\n"
    f"  └ body={TL_USE_BODY_BREAK}, strong≥{TL_MIN_BODY_PCT}%={TL_USE_STRONG_BAR}, "
    f"vol×{TL_VOL_MULT}={TL_USE_VOLUME}, atr×{TL_ATR_MULT}={TL_USE_ATR_DIST}\n"
    f"🅰️ <b>Path A (tl_retest)</b> — <code>retest of lowerLvl from below, NO filters</code>\n"
    f"  └ tol ±{PATH_A_RETEST_TOLERANCE_PCT}%, max wait {PATH_A_MAX_WAIT_BARS}b, "
    f"invalidate &gt;{PATH_A_INVALIDATION_PCT}% reclaim\n"
    f"🅱️ <b>Path B (tl_accept)</b> — <code>≥{PATH_B_ACCEPTANCE_BARS} consec closes below TL + bearish bar, NO filters</code>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🎯 TP       : <code>entry × (1 − {TP_PCT_FIXED}%)</code>  (fixed)\n"
    f"🛑 SL       : <code>lowerLvl × (1 + {SL_BUFFER_TL_PCT}%)</code>  (all paths)\n"
    f"⏳ Cooldown : <code>{TL_COOLDOWN_BARS} × 4h bars (shared)</code>\n"
    f"💰 Capital  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()
        if df.empty:
            print("[WARN] Sheet empty — possible auth issue, retrying in 30 min")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"----- SHORT TRADE SCAN — CYCLE {cycle} -----")

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
                f"🚨 <b>Short Bot Crashed — Restarting</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ Error : <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive errors — triggering restart"
            )
            raise SystemExit(1)

        time.sleep(60)