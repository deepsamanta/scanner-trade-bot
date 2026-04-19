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

# ─── STRATEGY CONSTANTS (200 EMA FALLING — TREND-FOLLOWING SHORT) ─────────────
EMA_PERIOD         = 200          # 200 EMA
LOOKBACK_CANDLES   = 300          # Candles to check for exhaustion
MIN_ABOVE_PCT      = 60.0         # ≥60% of last 200 candles must have closed ABOVE EMA
SLOPE_BARS         = 20           # Bars used to measure EMA slope
MAX_SLOPE_PCT      = -0.09        # EMA must be mildly falling: slope < -0.09% over SLOPE_BARS
MAX_EMA_DIST_PCT   = 3.0          # Price must be within 3% BELOW EMA at entry (don't chase late)

# ─── TP / SL ──────────────────────────────────────────────────────────────────
TP_PCT             = 0.025        # 2.5% below entry (fixed)
SL_ABOVE_EMA_PCT   = 0.01         # 1% ABOVE the EMA

# ─── SAFETY (reward/risk floor) ───────────────────────────────────────────────
MIN_RR             = 1.5          # Skip trade if TP/SL reward:risk falls below this

# ─── TIMEFRAME ────────────────────────────────────────────────────────────────
RESOLUTION         = "15"         # 15 minute candles
SCAN_INTERVAL      = 900          # 15 minutes in seconds

# ─── CANDLE FETCH WINDOW ──────────────────────────────────────────────────────
# Need at least EMA_PERIOD + LOOKBACK_CANDLES + SLOPE_BARS + buffer candles.
# 200 + 150 + 20 + 30 = 400 minimum. Fetch 600 to be safe.
CANDLE_FETCH_BARS  = 600
CANDLE_FETCH_SECS  = CANDLE_FETCH_BARS * 15 * 60   # 540,000 sec

# ─── REQUEST TIMEOUTS (seconds) ───────────────────────────────────────────────
REQUEST_TIMEOUT    = 15
TELEGRAM_TIMEOUT   = 10

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
# INDICATOR HELPER — EMA
# =====================================================

def compute_ema(closes, period):
    """
    Returns a list aligned such that:
      values[0]  corresponds to closes[period-1]
      values[-1] corresponds to closes[-1]
    i.e. values[-k] corresponds to closes[-k] for k in [1..len(values)]
    """
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    return values


# =====================================================
# SAFE API RESPONSE UNWRAPPER
# =====================================================

def unwrap_list_response(raw, list_keys, context=""):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in list_keys:
            if key in raw and isinstance(raw[key], list):
                return raw[key]
        print(f"[WARN]{' ' + context if context else ''}: unexpected dict keys: {list(raw.keys())} | raw: {str(raw)[:200]}")
        return []
    print(f"[WARN]{' ' + context if context else ''}: unexpected response type {type(raw)}")
    return []


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
        raw      = response.json()

        positions = unwrap_list_response(
            raw,
            list_keys=["positions", "data", "result"],
            context="get_open_positions"
        )
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
        raw      = response.json()

        orders = unwrap_list_response(
            raw,
            list_keys=["orders", "data", "result"],
            context=f"has_open_order({symbol})"
        )

        pair = fut_pair(symbol)
        for o in orders:
            if o.get("pair") == pair:
                return True
        return False

    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


# =====================================================
# TP CHECK — recent LOW for last 15 minutes (SHORT)
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
        candles  = response.json()["data"]
        lows     = [float(c["low"]) for c in candles]
        return min(lows) if lows else None
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
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, ema_now, precision):
    entry = round(entry_price, precision)
    tp    = round(entry   * (1 - TP_PCT),           precision)
    sl    = round(ema_now * (1 + SL_ABOVE_EMA_PCT), precision)

    # Sanity: SL must be above entry (for a short)
    if sl <= entry:
        print(f"[SKIP] {symbol} computed SL {sl} not above entry {entry} — aborting")
        send_telegram(
            f"⚠️ <b>SHORT SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>SL {sl} not above entry {entry}</code>\n"
            f"📍 EMA    : <code>{round(ema_now, precision)}</code>"
        )
        return None, None

    reward = entry - tp
    risk   = sl - entry

    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>SHORT SIGNAL SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>  (-{TP_PCT*100:.2f}%)\n"
            f"🛑 SL     : <code>{sl}</code>  ({SL_ABOVE_EMA_PCT*100:.2f}% above EMA {round(ema_now, precision)})"
        )
        return None, None

    qty = compute_qty(entry_price, symbol)

    sl_pct_from_entry = round(((sl - entry) / entry) * 100, 2)

    print(
        f"[SHORT TRADE] {symbol} SELL | Entry {entry} | TP {tp} (-{TP_PCT*100:.2f}%) "
        f"| SL {sl} (+{sl_pct_from_entry}% from entry | {SL_ABOVE_EMA_PCT*100:.2f}% above EMA) "
        f"| RR {round(reward / risk, 2)} | Qty {qty}"
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
            f"📍 Entry    : <code>{entry}</code>\n"
            f"🎯 TP       : <code>{tp}</code>\n"
            f"🛑 SL       : <code>{sl}</code>\n"
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
        f"📍 Entry  : <code>{entry}</code>\n"
        f"📈 EMA200 : <code>{round(ema_now, precision)}</code>\n"
        f"🎯 TP     : <code>{tp}</code>  (-{TP_PCT*100:.2f}% from entry)\n"
        f"🛑 SL     : <code>{sl}</code>  ({SL_ABOVE_EMA_PCT*100:.2f}% above EMA | +{sl_pct_from_entry}% from entry)\n"
        f"📊 RR     : <code>{round(reward / risk, 2)}</code>\n"
        f"📦 Qty    : <code>{qty}</code>\n"
        f"💰 Margin : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )

    return tp_confirmed, sl


# =====================================================
# MAIN LOGIC — 200 EMA EXHAUSTION SHORT (15m)
# =====================================================

def check_and_trade(symbol, row, df, placed_this_cycle):

    # =========================================================================
    # IN-CYCLE GUARD — prevents duplicate orders within the same scan cycle
    # =========================================================================
    if symbol in placed_this_cycle:
        print(f"[SKIP] {symbol} — already traded this cycle (in-memory guard)")
        return

    pair     = fut_pair(symbol)
    pair_api = pair
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())

    params = {
        "pair":       pair_api,
        "from":       now - CANDLE_FETCH_SECS,
        "to":         now,
        "resolution": RESOLUTION,
        "pcode":      "f",
    }

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = sorted(response.json()["data"], key=lambda x: x["time"])
    except Exception as e:
        print(f"[ERROR] {symbol} candle fetch failed: {e}")
        return

    min_required = EMA_PERIOD + LOOKBACK_CANDLES + SLOPE_BARS + 5
    if len(candles) < min_required:
        print(f"[SKIP] {symbol} — not enough candles ({len(candles)} < {min_required})")
        return

    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    last_close = closes[-1]

    ema_values = compute_ema(closes, EMA_PERIOD)
    # ema_values[-k] aligns with closes[-k]

    ema_now  = ema_values[-1]
    ema_prev = ema_values[-2]

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
            placed_this_cycle.add(symbol)
            return

    # =========================================================================
    # GATE 2 — Open order check
    # =========================================================================
    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled order on book, skipping")
        placed_this_cycle.add(symbol)
        return

    # =========================================================================
    # TP MONITORING (based on sheet-stored TP)
    # =========================================================================
    tp_raw = df.iloc[row, 1]

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} TP COMPLETED")
        return

    try:
        tp_stored = float(tp_raw)

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
    # STRATEGY CONDITIONS — 200 EMA FALLING SHORT
    # =========================================================================

    # ── Condition 1: % of last LOOKBACK_CANDLES candles closed ABOVE 200 EMA ──
    last_n_closes = closes[-LOOKBACK_CANDLES:]
    last_n_emas   = ema_values[-LOOKBACK_CANDLES:]
    above_count   = sum(1 for c, e in zip(last_n_closes, last_n_emas) if c > e)
    above_pct     = (above_count / LOOKBACK_CANDLES) * 100.0
    trend_qualifies = above_pct >= MIN_ABOVE_PCT

    # ── Condition 2: EMA is mildly falling (slope < MAX_SLOPE_PCT) ────────────
    ema_slope_ref = ema_values[-(SLOPE_BARS + 1)]   # EMA value SLOPE_BARS bars ago
    if ema_slope_ref == 0:
        ema_slope_pct = 0.0
    else:
        ema_slope_pct = ((ema_now - ema_slope_ref) / ema_slope_ref) * 100.0
    ema_falling = ema_slope_pct < MAX_SLOPE_PCT

    # ── Condition 3: Price CROSSES DOWN through the 200 EMA this bar ──────────
    prev_close = closes[-2]
    cross_down = (prev_close >= ema_prev) and (last_close < ema_now)

    # ── Condition 4: Price within MAX_EMA_DIST_PCT below EMA (fresh entry) ────
    if ema_now > 0:
        ema_dist_pct = ((ema_now - last_close) / ema_now) * 100.0
    else:
        ema_dist_pct = 0.0
    price_near_ema = 0 <= ema_dist_pct <= MAX_EMA_DIST_PCT

    print(
        f"[SCAN] {symbol} | Price {last_close} | EMA200 {round(ema_now, precision)} | "
        f"Above% {round(above_pct, 1)}/{MIN_ABOVE_PCT} | "
        f"Slope {round(ema_slope_pct, 3)}% (need <{MAX_SLOPE_PCT}%) | "
        f"Dist {round(ema_dist_pct, 2)}% below EMA (max {MAX_EMA_DIST_PCT}%) | "
        f"trend_ok={trend_qualifies} falling={ema_falling} crossdown={cross_down} near={price_near_ema}"
    )

    if not trend_qualifies:
        print(f"[SKIP] {symbol} — only {round(above_pct, 1)}% of last {LOOKBACK_CANDLES} candles above EMA (need ≥{MIN_ABOVE_PCT}%)")
        return

    if not ema_falling:
        print(f"[SKIP] {symbol} — EMA not falling enough (slope {round(ema_slope_pct, 3)}%, need < {MAX_SLOPE_PCT}%)")
        return

    if not cross_down:
        print(f"[SKIP] {symbol} — no crossunder this bar (prev {prev_close} vs prev_ema {round(ema_prev, precision)}, "
              f"curr {last_close} vs curr_ema {round(ema_now, precision)})")
        return

    if not price_near_ema:
        print(f"[SKIP] {symbol} — price too far below EMA ({round(ema_dist_pct, 2)}%, max {MAX_EMA_DIST_PCT}%) — chasing, skipping")
        return

    print(
        f"[SIGNAL] {symbol} | all SHORT conditions met ✓ "
        f"| Above% {round(above_pct, 1)} ✓ "
        f"| Slope {round(ema_slope_pct, 3)}% ✓ (falling) "
        f"| crossunder ✓ "
        f"| Dist {round(ema_dist_pct, 2)}% ✓ "
        f"| Price {last_close} | EMA {round(ema_now, precision)}"
    )

    # =========================================================================
    # FINAL GUARD — re-check everything right before placing
    # =========================================================================
    live_positions = get_open_positions()
    for pos in live_positions:
        if pos.get("pair") == pair:
            print(f"[SKIP] {symbol} — open position detected just before placement, aborting")
            placed_this_cycle.add(symbol)
            return

    if has_open_order(symbol):
        print(f"[SKIP] {symbol} — unfilled open order detected just before placement, aborting")
        placed_this_cycle.add(symbol)
        return

    if last_close >= ema_now:
        print(
            f"[SKIP] {symbol} — last close {last_close} not below "
            f"EMA200 {round(ema_now, precision)} at placement, aborting"
        )
        return

    tp_confirmed, sl_placed = place_short_order(symbol, last_close, ema_now, precision)
    if tp_confirmed:
        placed_this_cycle.add(symbol)
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
    f"✅ <b>SHORT Bot Started — 200 EMA Falling</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy  : <code>200 EMA Falling Short</code>\n"
    f"⏱ Timeframe : <code>15 Min</code>\n"
    f"📉 Entry     : <code>Price crosses DOWN through EMA200</code>\n"
    f"🔎 Filter 1  : <code>≥{MIN_ABOVE_PCT:.0f}% of last {LOOKBACK_CANDLES} candles closed ABOVE EMA</code>\n"
    f"🔎 Filter 2  : <code>EMA falling — slope &lt; {MAX_SLOPE_PCT}% over {SLOPE_BARS} bars</code>\n"
    f"🔎 Filter 3  : <code>Price within {MAX_EMA_DIST_PCT}% below EMA (fresh entry)</code>\n"
    f"🎯 TP        : <code>{TP_PCT*100:.2f}% below entry (fixed)</code>\n"
    f"🛑 SL        : <code>{SL_ABOVE_EMA_PCT*100:.2f}% above EMA200</code>\n"
    f"📊 Min RR    : <code>{MIN_RR}</code>\n"
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

        # Tracks symbols traded THIS cycle — reset every cycle
        placed_this_cycle = set()

        print(f"----- SHORT SCAN — CYCLE {cycle} -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_and_trade(symbol, row, df, placed_this_cycle)

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