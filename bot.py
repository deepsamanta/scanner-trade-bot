import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# ─── TUNEABLE CONSTANTS ────────────────────────────────────────────────────────
EMA_PERIOD       = 21
MIN_RR           = 0.05

# ─── DYNAMIC TP SETTINGS (SHORT) ──────────────────────────────────────────────
SWING_LOOKBACK     = 3        # candles each side to confirm a swing low
SUPPORT_CANDLES    = 200      # how many candles to scan for swing body lows
MIN_TP_PCT         = 0.012    # minimum TP: 1.2% below entry
MAX_TP_PCT         = 0.05     # maximum TP: 5% below entry (cap)
FALLBACK_TP_PCT    = 0.01     # fallback fixed TP if no support found: 1%

# ─── STOP LOSS ────────────────────────────────────────────────────────────────
SL_PCT           = 0.055      # 5.5% fixed above entry

# ─── LINEAR REGRESSION SLOPE ──────────────────────────────────────────────────
LINREG_LOOKBACK  = 4          # candles for slope curve

# ─── 4H TREND FILTER ──────────────────────────────────────────────────────────
# Before placing a short, verify that the 4H timeframe also shows bearish momentum.
# The linreg slope on 4H EMA must be NEGATIVE (downward-bending curve on 4H).
LINREG_4H_LOOKBACK = 5        # number of 4H candles to use for the slope check

# ─── CONSOLIDATION FILTER ─────────────────────────────────────────────────────
# Before a valid short, price must have spent enough time ABOVE the EMA
# (rallied / consolidated above it). This confirms a proper retest, not a
# mid-air entry on an already extended move down.
FILTER_LOOKBACK  = 50         # how many candles to check
MIN_ABOVE_PERC   = 65         # min % of those candles that must be ABOVE EMA

# ─── EMA PROXIMITY FILTER ─────────────────────────────────────────────────────
# Even after the crossover down, don't sell if price has already run too far below EMA.
MAX_EMA_DISTANCE_PCT = 0.03   # 2% max distance below EMA

# ─── SCAN INTERVAL ────────────────────────────────────────────────────────────
SCAN_INTERVAL    = 900        # 15 minutes in seconds

# ─── REQUEST TIMEOUTS (seconds) ───────────────────────────────────────────────
REQUEST_TIMEOUT  = 15
TELEGRAM_TIMEOUT = 10

# ─── GSPREAD RE-AUTH INTERVAL ─────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60
# ──────────────────────────────────────────────────────────────────────────────


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


# =====================================================
# READ / WRITE SHEET
# =====================================================

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
        hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type":     "application/json",
        "X-AUTH-APIKEY":    COINDCX_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    return payload, headers


# =====================================================
# TELEGRAM NOTIFICATION
# =====================================================

def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }
        requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


# =====================================================
# PRECISION HELPER
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATOR HELPERS
# =====================================================

def compute_ema(closes, period):
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    return values


def compute_linreg_slope(values):
    """
    Exact Python port of the Pine Script f_true_slope() function:

        for i = 0 to len - 1
            x = len - i        # x counts DOWN: len, len-1, ..., 1
            y = src[i]         # src[0] = most recent bar
        slope = (n·ΣXY - ΣX·ΣY) / (n·ΣX2 - (ΣX)^2)

    'values' must be ordered newest -> oldest (index 0 = most recent).
    Call as: compute_linreg_slope(list(reversed(ema_values[-N:])))

    A NEGATIVE slope means the EMA curve is genuinely bending downward (bearish).
    """
    n      = len(values)
    sum_x  = 0.0
    sum_y  = 0.0
    sum_xy = 0.0
    sum_x2 = 0.0

    for i in range(n):
        x       = n - i
        y       = values[i]
        sum_x  += x
        sum_y  += y
        sum_xy += x * y
        sum_x2 += x * x

    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 0.0

    return (n * sum_xy - sum_x * sum_y) / denom


# =====================================================
# 4H TREND FILTER — bearish: slope negative + consolidation ABOVE EMA
# =====================================================

def get_4h_data(symbol):
    """
    Fetches 4H candles, computes the 21 EMA on them, then returns:
      - slope_ok        : bool  — linreg slope on last 4 EMA values is NEGATIVE (bearish)
      - slope_4h        : float — raw slope value (for logging)
      - is_consolidating: bool  — >= MIN_ABOVE_PERC% of last FILTER_LOOKBACK 4H
                                  candles closed ABOVE the 4H EMA (bearish retest logic)
      - perc_above_4h   : float — actual % above for logging

    On error, returns fail-open defaults so the trade is not blocked.
    """
    try:
        pair_api = fut_pair(symbol)
        url      = "https://public.coindcx.com/market_data/candlesticks"
        now      = int(time.time())

        fetch_candles = EMA_PERIOD + FILTER_LOOKBACK + LINREG_4H_LOOKBACK + 10
        fetch_seconds = fetch_candles * 4 * 3600

        params = {
            "pair":       pair_api,
            "from":       now - fetch_seconds,
            "to":         now,
            "resolution": "240",
            "pcode":      "f",
        }

        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = sorted(response.json()["data"], key=lambda x: x["time"])

        min_required = EMA_PERIOD + FILTER_LOOKBACK + LINREG_4H_LOOKBACK
        if len(candles) < min_required:
            print(f"[4H] {symbol} — not enough 4H candles ({len(candles)}, need {min_required}), skipping filters (allow trade)")
            return True, None, True, 0.0

        # ── 21 EMA on 4H closes ───────────────────────────────────────────────
        closes_4h = [float(c["close"]) for c in candles]
        ema_4h    = compute_ema(closes_4h, EMA_PERIOD)

        # ── Linreg slope on last LINREG_4H_LOOKBACK 4H EMA values ────────────
        # For shorts: slope must be NEGATIVE
        ema_4h_window = list(reversed(ema_4h[-LINREG_4H_LOOKBACK:]))
        slope_4h      = compute_linreg_slope(ema_4h_window)
        slope_ok      = slope_4h < 0   # ← INVERTED: negative = bearish

        # ── Consolidation: % of last FILTER_LOOKBACK 4H candles ABOVE 4H EMA ─
        # For shorts: price must have spent time above EMA before breaking down
        bars_above_4h = sum(
            1 for i in range(1, FILTER_LOOKBACK + 1)
            if float(candles[-(i + 1)]["close"]) > float(ema_4h[-(i + 1)])
        ) if len(candles) > FILTER_LOOKBACK + 1 else 0
        perc_above_4h    = (bars_above_4h / FILTER_LOOKBACK) * 100
        is_consolidating = perc_above_4h >= MIN_ABOVE_PERC   # ← INVERTED: above EMA

        return slope_ok, slope_4h, is_consolidating, perc_above_4h

    except Exception as e:
        print(f"[4H] {symbol} — fetch error: {e} — skipping filters (allow trade)")
        return True, None, True, 0.0


# =====================================================
# DYNAMIC TP — NEAREST SUPPORT (SWING LOW BODY)
# =====================================================

def find_nearest_support(candles, entry_price, precision):
    """
    Scans the last SUPPORT_CANDLES candles for swing lows based on
    candle BODY BOTTOMS — min(open, close) — ignoring wicks.

    A swing body low = candle whose body bottom is lower than SWING_LOOKBACK
    candles on both its left and right side.

    Returns the nearest (highest) body-based swing low BELOW entry, capped
    between MIN_TP_PCT and MAX_TP_PCT below entry.
    Falls back to FALLBACK_TP_PCT (1%) below entry if none found.
    """
    recent_candles = candles[-SUPPORT_CANDLES:]
    body_bottoms   = [min(float(c["open"]), float(c["close"])) for c in recent_candles]
    n  = len(body_bottoms)
    lb = SWING_LOOKBACK

    swing_body_lows = []

    for i in range(lb, n - lb):
        current = body_bottoms[i]
        left    = body_bottoms[i - lb : i]
        right   = body_bottoms[i + 1 : i + lb + 1]

        # Swing low: current body bottom is lower than all neighbours
        if all(current < b for b in left) and all(current < b for b in right):
            swing_body_lows.append(current)

    min_tp_price = entry_price * (1 - MAX_TP_PCT)   # furthest allowed TP (lower)
    max_tp_price = entry_price * (1 - MIN_TP_PCT)   # closest allowed TP (higher)

    # Valid: body lows that sit between MIN_TP_PCT and MAX_TP_PCT below entry
    valid = [b for b in swing_body_lows if min_tp_price <= b <= max_tp_price]

    if valid:
        nearest = max(valid)   # nearest = highest value = closest to entry from below
        tp      = round(nearest, precision)
        print(f"[BODY SUPPORT TP] Found swing body low at {tp} (from {len(valid)} candidates)")
        return tp, "body_support"

    fallback_tp = round(entry_price * (1 - FALLBACK_TP_PCT), precision)
    print(f"[BODY SUPPORT TP] No valid swing body low found — using fallback {FALLBACK_TP_PCT*100:.1f}% TP = {fallback_tp}")
    return fallback_tp, "fallback"


# =====================================================
# OPEN POSITIONS
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
        return [p for p in positions if float(p.get("active_pos", 0)) != 0]
    except Exception as e:
        print("get_open_positions error:", e)
        return []


def get_position_tp(symbol):
    try:
        positions = get_open_positions()
        pair = fut_pair(symbol)
        for pos in positions:
            if pos.get("pair") == pair:
                tp = pos.get("take_profit_trigger")
                if tp:
                    return float(tp)
        return None
    except Exception:
        return None


def get_position_entry(symbol):
    try:
        positions = get_open_positions()
        pair = fut_pair(symbol)
        for pos in positions:
            if pos.get("pair") == pair:
                ep = pos.get("entry_price") or pos.get("avg_price")
                if ep:
                    return float(ep)
        return None
    except Exception:
        return None


# =====================================================
# OPEN ORDER CHECK
# =====================================================

def has_open_order(symbol):
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "page":                       1,
            "size":                       50,
            "margin_currency_short_name": "USDT",
            "status":                     ["initial", "open", "partially_filled"],
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        orders   = response.json()

        pair = fut_pair(symbol)
        if isinstance(orders, list):
            for o in orders:
                if o.get("pair") == pair:
                    return True
        return False

    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


# =====================================================
# TP CHECK — recent LOW over last 15 minutes (SHORT)
# =====================================================

def get_recent_low(symbol):
    """
    Fetches 1-min candles over the last 15 minutes to check if price
    touched TP via a wick downward since the last cycle.
    """
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
        candles  = response.json()["data"]
        lows     = [float(c["low"]) for c in candles]
        return min(lows)   # ← INVERTED: check minimum low for short TP
    except Exception:
        return None


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
# PLACE SHORT ORDER — with dynamic TP (INVERTED)
# =====================================================

def place_short_order(symbol, entry_price, precision, candles):
    entry   = round(entry_price, precision)
    sl_base = round(entry * (1 + SL_PCT), precision)   # ← SL is ABOVE entry for shorts

    tp, tp_type = find_nearest_support(candles, entry, precision)

    reward = entry - tp    # ← For shorts: reward = entry - TP (TP is below)
    risk   = sl_base - entry  # ← For shorts: risk = SL - entry (SL is above)

    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>SHORT SIGNAL SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason  : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>  ({tp_type})\n"
            f"🛑 SL      : <code>{sl_base}</code>  (above entry)"
        )
        return None, None

    qty = compute_qty(entry_price, symbol)

    tp_pct = round(((entry - tp) / entry) * 100, 2)   # ← % drop to TP

    print(
        f"[SHORT TRADE] {symbol} SELL | Entry {entry} | TP {tp} (-{tp_pct}% — {tp_type}) "
        f"| SL {sl_base} (+{SL_PCT*100:.0f}%) | RR {round(reward / risk, 2)} | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "sell",              # ← INVERTED: sell for short
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry,
            "total_quantity":    qty,
            "leverage":          LEVERAGE,
            "take_profit_price": tp,
            "stop_loss_price":   sl_base,
        },
    }

    payload, headers = sign_request(body)
    response = requests.post(
        BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
        data=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    result = response.json()

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} short order not placed: {result}")
        send_telegram(
            f"❌ <b>SHORT ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>  ({tp_type})\n"
            f"🛑 SL      : <code>{sl_base}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return None, None

    try:
        order        = result[0] if isinstance(result, list) else result["order"]
        tp_confirmed = order.get("take_profit_price", tp)
    except Exception:
        tp_confirmed = tp

    send_telegram(
        f"🔴 <b>NEW SHORT (SELL) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (-{tp_pct}% — {tp_type})\n"
        f"🛑 SL      : <code>{sl_base}</code>  (+{int(SL_PCT * 100)}% above entry)\n"
        f"📊 RR      : <code>{round(reward / risk, 2)}</code>\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )

    return tp_confirmed, sl_base


# =====================================================
# MAIN LOGIC
# =====================================================

def check_and_trade(symbol, row, df):

    pair     = fut_pair(symbol)
    pair_api = pair
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())

    params = {
        "pair":       pair_api,
        "from":       now - 360000,
        "to":         now,
        "resolution": "30",
        "pcode":      "f",
    }

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = sorted(response.json()["data"], key=lambda x: x["time"])
    except Exception as e:
        print(f"[ERROR] {symbol} candle fetch failed: {e}")
        return

    if len(candles) < EMA_PERIOD + LINREG_LOOKBACK + 1:
        return

    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    last_close = float(candles[-1]["close"])

    ema_values = compute_ema(closes, EMA_PERIOD)
    del closes

    ema_now  = ema_values[-1]

    # ── Linear regression slope — Pine Script formula (newest first) ──────────
    ema_window         = list(reversed(ema_values[-LINREG_LOOKBACK:]))
    ema_slope          = compute_linreg_slope(ema_window)
    ema_slope_negative = ema_slope < 0   # ← INVERTED: need negative slope for short
    slope_dir          = "negative" if ema_slope_negative else "positive"

    # =========================================================================
    # GATE 1 — Open position check
    # =========================================================================
    positions = get_open_positions()
    for pos in positions:
        if pos.get("pair") == pair:
            print(f"[ACTIVE TRADE] {symbol} — position open on CoinDCX, skipping")
            tp_live = get_position_tp(symbol)
            if tp_live:
                update_sheet_tp(row, tp_live)
            return

    # =========================================================================
    # GATE 2 — Open order check
    # =========================================================================
    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled order on book, skipping")
        return

    # ── TP monitoring ─────────────────────────────────────────────────────────
    tp_raw = df.iloc[row, 1]

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} TP COMPLETED")
        return

    try:
        tp_stored = float(tp_raw)

        # ← INVERTED: TP hit when price drops TO or BELOW stored TP
        if last_close <= tp_stored:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} price {last_close} <= TP {tp_stored}")
            return

        recent_low = get_recent_low(symbol)
        if recent_low and recent_low <= tp_stored:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} recent low {recent_low} <= TP {tp_stored}")
            return

    except Exception:
        pass

    # =========================================================================
    # STRATEGY CONDITIONS (ALL INVERTED FOR SHORT)
    # =========================================================================

    # ← INVERTED: for shorts, measure how far price is BELOW EMA
    ema_distance_pct = (ema_now - last_close) / ema_now if ema_now > 0 else 0
    price_near_ema   = ema_distance_pct <= MAX_EMA_DISTANCE_PCT   # not too far below
    price_below_ema  = last_close < ema_now   # ← INVERTED: price must be BELOW EMA

    # ── Fetch 4H data (slope + consolidation) — single API call ─────────────
    slope_4h_ok, slope_4h_val, is_consolidating, perc_above_4h = get_4h_data(symbol)
    slope_4h_str = (
        f"{round(slope_4h_val, 6)} ({'✅' if slope_4h_ok else '❌'})"
        if slope_4h_val is not None else "N/A"
    )

    print(
        f"[SCAN] {symbol} | Price {last_close} | 21 EMA {round(ema_now, precision)} | "
        f"slope30m {round(ema_slope, precision)} ({slope_dir}) | "
        f"slope4H {slope_4h_str} | "
        f"4H_above_EMA {round(perc_above_4h, 1)}% (need {MIN_ABOVE_PERC}%) | "
        f"dist {round(ema_distance_pct * 100, 2)}% | "
        f"below_ema={price_below_ema} slope30m_neg={ema_slope_negative} consol4H={is_consolidating} near={price_near_ema}"
    )

    # ← INVERTED: price must be BELOW EMA
    if not price_below_ema:
        return

    # ← INVERTED: 30m slope must be negative
    if not ema_slope_negative:
        print(f"[SKIP] {symbol} — 30m slope is positive/flat (need negative for short)")
        return

    # ← INVERTED: price must not have dropped too far below EMA already
    if not price_near_ema:
        print(f"[SKIP] {symbol} — price {round(ema_distance_pct*100,2)}% below EMA, exceeds {MAX_EMA_DISTANCE_PCT*100}% max — waiting for retest")
        return

    # =========================================================================
    # GATE 3 — 4H SLOPE + 4H CONSOLIDATION FILTER (INVERTED)
    # =========================================================================
    if not slope_4h_ok:
        print(
            f"[SKIP] {symbol} — 4H linreg slope positive/flat "
            f"(slope={slope_4h_str}) — higher timeframe not bearish, skipping"
        )
        return

    if not is_consolidating:
        print(
            f"[SKIP] {symbol} — 4H consolidation filter failed "
            f"({round(perc_above_4h, 1)}% of last {FILTER_LOOKBACK} 4H candles above EMA, need {MIN_ABOVE_PERC}%)"
        )
        return

    print(
        f"[SIGNAL] {symbol} | all SHORT conditions met ✓ "
        f"| slope30m {round(ema_slope, precision)} ✓ (negative) "
        f"| slope4H {slope_4h_str} ✓ (negative) "
        f"| 4H_above_EMA {round(perc_above_4h, 1)}% ✓ "
        f"| dist {round(ema_distance_pct*100,2)}% ✓ "
        f"| Price {last_close} | EMA {round(ema_now, precision)} "
        f"| SL {round(last_close * (1 + SL_PCT), precision)}"
    )

    # =========================================================================
    # FINAL GUARD — re-check everything right before placing
    # =========================================================================
    live_positions = get_open_positions()
    for pos in live_positions:
        if pos.get("pair") == pair:
            print(f"[SKIP] {symbol} — open position detected just before placement, aborting")
            return

    if has_open_order(symbol):
        print(f"[SKIP] {symbol} — unfilled open order detected just before placement, aborting")
        return

    # ← INVERTED: re-confirm price is still below EMA at placement time
    if last_close >= ema_now:
        print(
            f"[SKIP] {symbol} — last close {last_close} not below "
            f"21 EMA {round(ema_now, precision)} at placement, aborting"
        )
        return

    tp_confirmed, sl_placed = place_short_order(
        symbol, last_close, precision, candles
    )
    if tp_confirmed:
        update_sheet_tp(row, tp_confirmed)
    if sl_placed:
        update_sheet_sl(row, sl_placed)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>SHORT Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy  : <code>21 EMA Breakdown Short</code>\n"
    f"⏱ Timeframe  : <code>30 Min</code>\n"
    f"📉 Entry     : <code>Price &lt; 21 EMA | 30m slope &lt; 0 | dist &lt;{int(MAX_EMA_DISTANCE_PCT*100)}%</code>\n"
    f"✅ Filter    : <code>Pumped coins added manually to sheet</code>\n"
    f"📊 4H Filter : <code>LinReg slope on last {LINREG_4H_LOOKBACK} × 4H EMA values &lt; 0 | {MIN_ABOVE_PERC}% of last {FILTER_LOOKBACK} 4H candles above 4H EMA</code>\n"
    f"🎯 TP        : <code>Dynamic — nearest swing body support (1.2%–5%) below entry | fallback 1%</code>\n"
    f"🛑 SL        : <code>{int(SL_PCT * 100)}% fixed above entry</code>\n"
    f"💰 Capital   : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
    f"🕐 Scanning every 15 minutes..."
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 15 min")
            time.sleep(SCAN_INTERVAL)
            continue

        cycle += 1
        consecutive_errors = 0

        print(f"----- SHORT SCAN — CYCLE {cycle} -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_and_trade(symbol, row, df)

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