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
EMA_FAST_PERIOD       = 50      # 50 EMA  — entry trigger level
EMA_SLOW_PERIOD       = 100     # 100 EMA — macro context
TP_PCT                = 0.031   # TP = entry * (1 - 0.031) → fixed 3.1% below entry
SL_PCT                = 0.10    # SL = entry * 1.10 → fixed 10% above entry
MIN_RR                = 0.1     # very wide SL so RR will always be low — keep permissive
EMA50_SLOPE_BARS      = 5      # candles to measure 50 EMA slope (must be negative)
EMA100_SLOPE_BARS     = 5      # candles to measure 100 EMA slope (must be near flat)
EMA100_FLAT_THRESHOLD = 0.0009  # 100 EMA slope as % of price — below this = flat
# ──────────────────────────────────────────────────────────────────────────────


# =====================================================
# GOOGLE SHEETS CONNECTION
# =====================================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds  = Credentials.from_service_account_file("service_account.json", scopes=scope)
client = gspread.authorize(creds)
sheet  = client.open_by_key(SHEET_ID).sheet1


# =====================================================
# READ / WRITE SHEET
# =====================================================

def get_sheet_data():
    try:
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
        sheet.update(f"B{row + 1}", [[str(value)]])
        print(f"[SHEET] Row {row + 1} col B -> {value}")
    except Exception as e:
        print("Sheet update error:", e)


def update_sheet_sl(row, value):
    """Column C stores live SL for trailing stop tracking."""
    try:
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
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


# =====================================================
# PRECISION HELPER
# =====================================================

def get_precision(raw_candle_close):
    """
    Derive decimal precision from the raw API string.
    Avoids float noise e.g. 0.004823 becoming 0.0048229999999999997.
    """
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATOR HELPERS
# =====================================================

def compute_ema(closes, period):
    """Returns list of EMA values, one per close starting from index period-1."""
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    return values


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
        response = requests.post(url, data=payload, headers=headers)
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
    """Return entry price of an open position, or None."""
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
# ACTIVE ORDERS CHECK
# =====================================================

def has_active_order(symbol):
    """
    Returns True if there is any open/pending order for this pair
    that has not yet been filled or cancelled.
    """
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
        response = requests.post(url, data=payload, headers=headers)
        orders   = response.json()

        pair = fut_pair(symbol)
        if isinstance(orders, list):
            for o in orders:
                if o.get("pair") == pair:
                    return True
        return False

    except Exception as e:
        print(f"has_active_order error ({symbol}):", e)
        return False


# =====================================================
# WICK TP DETECTION
# =====================================================

def get_recent_low(symbol):
    try:
        pair_api = fut_pair(symbol)
        url  = "https://public.coindcx.com/market_data/candlesticks"
        now  = int(time.time())
        params = {
            "pair":       pair_api,
            "from":       now - 180,
            "to":         now,
            "resolution": "1",
            "pcode":      "f",
        }
        response = requests.get(url, params=params)
        candles  = response.json()["data"]
        lows     = [float(c["low"]) for c in candles]
        return min(lows)
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
        response   = requests.get(url)
        data       = response.json()
        instrument = data["instrument"]
        quantity_increment = Decimal(str(instrument["quantity_increment"]))
        min_quantity       = Decimal(str(instrument["min_quantity"]))
        return max(quantity_increment, min_quantity)
    except Exception:
        return Decimal("1")


def compute_qty(entry_price, symbol):
    """
    Fixed sizing: always spend exactly CAPITAL_USDT * LEVERAGE.
    qty = (CAPITAL_USDT * LEVERAGE) / entry_price
    """
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
# PLACE ORDER
# =====================================================

def place_order(side, symbol, entry_price, precision):
    entry = round(entry_price, precision)

    # ── TP: fixed 3.1% below entry ───────────────────────────────────────────
    tp = round(entry * (1 - TP_PCT), precision)

    # ── SL: fixed 10% above entry ─────────────────────────────────────────────
    sl_base = round(entry * (1 + SL_PCT), precision)

    # ── Reward / Risk gate ───────────────────────────────────────────────────
    reward = entry - tp
    risk   = sl_base - entry
    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>SIGNAL SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>\n"
            f"🛑 SL     : <code>{sl_base}</code>"
        )
        return None, None

    qty = compute_qty(entry_price, symbol)

    print(
        f"[TRADE] {symbol} SELL | Entry {entry} | TP {tp} | SL {sl_base} "
        f"| RR {round(reward / risk, 2)} | Qty {qty}"
    )

    # ── Order body — matches official CoinDCX API spec ────────────────────────
    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              side,
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
    )
    result = response.json()

    print(f"[API] {symbol} response: {result}")

    # ── Bail out if exchange rejected ─────────────────────────────────────────
    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} order not placed: {result}")
        send_telegram(
            f"❌ <b>ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>\n"
            f"🛑 SL     : <code>{sl_base}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return None, None

    try:
        order        = result[0] if isinstance(result, list) else result["order"]
        tp_confirmed = order.get("take_profit_price", tp)
    except Exception:
        tp_confirmed = tp

    # ── Telegram notification ─────────────────────────────────────────────────
    send_telegram(
        f"🔴 <b>NEW SHORT — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (-{TP_PCT * 100:.1f}%)\n"
        f"🛑 SL      : <code>{sl_base}</code>  (+{int(SL_PCT * 100)}% fixed)\n"
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
        "resolution": "30",         # ← 30 min candles (was 15)
        "pcode":      "f",
    }

    response = requests.get(url, params=params)
    candles  = sorted(response.json()["data"], key=lambda x: x["time"])

    # Need at least 100 candles for slow EMA
    if len(candles) < EMA_SLOW_PERIOD + 5:
        return

    # ── Precision from raw API string ────────────────────────────────────────
    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    last_close = float(candles[-1]["close"])

    # ── Compute both EMAs ────────────────────────────────────────────────────
    ema50_values  = compute_ema(closes, EMA_FAST_PERIOD)
    ema100_values = compute_ema(closes, EMA_SLOW_PERIOD)

    ema50  = round(ema50_values[-1],  precision)
    ema100 = round(ema100_values[-1], precision)

    # ── Check active position FIRST — from CoinDCX API ───────────────────────
    positions = get_open_positions()
    for pos in positions:
        if pos.get("pair") == pair:
            print(f"[ACTIVE TRADE] {symbol} — position open on CoinDCX, skipping")
            tp_live = get_position_tp(symbol)
            if tp_live:
                update_sheet_tp(row, tp_live)
            return

    # ── Check active order — from CoinDCX API ────────────────────────────────
    if has_active_order(symbol):
        print(f"[ACTIVE ORDER] {symbol} — order on book on CoinDCX, skipping")
        return

    # ── TP monitoring — check every 30s if stored TP has been hit ────────────
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

    # ── SLOPE FILTERS ─────────────────────────────────────────────────────────
    #
    #   Filter A — 50 EMA must be bending DOWN
    #   slope = ema50[-1] - ema50[-10]  must be negative
    #
    ema50_slope = ema50_values[-1] - ema50_values[-EMA50_SLOPE_BARS]
    if ema50_slope >= 0:
        print(f"[SKIP] {symbol} 50 EMA slope not down ({round(ema50_slope, precision)}) | 50 EMA {ema50} | 100 EMA {ema100} | Price {last_close}")
        return

    #   Filter B — 100 EMA must NOT be rising steeply
    #   Flat OR declining = fine. Only block if steeply rising.
    #
    ema100_slope     = ema100_values[-1] - ema100_values[-EMA100_SLOPE_BARS]
    ema100_slope_pct = ema100_slope / last_close
    if ema100_slope_pct > EMA100_FLAT_THRESHOLD:
        print(f"[SKIP] {symbol} 100 EMA still rising (slope +{round(ema100_slope_pct * 100, 4)}%) | 50 EMA {ema50} | 100 EMA {ema100} | Price {last_close}")
        return

    # ── STRATEGY CONDITIONS ───────────────────────────────────────────────────
    #
    #   1. macroBullish = ema50 > ema100  (50 EMA above 100 EMA)
    #   2. below_ema50  = price < ema50   (price below 50 EMA)
    #

    macro_bullish = ema50 > ema100
    below_ema50   = last_close < ema50

    if not macro_bullish:
        print(f"[SKIP] {symbol} 50 EMA {ema50} not above 100 EMA {ema100} | Price {last_close}")
        return

    if not below_ema50:
        print(f"[SKIP] {symbol} price {last_close} not below 50 EMA {ema50} | 100 EMA {ema100}")
        return

    print(
        f"[SIGNAL] {symbol} | Price {last_close} | 50 EMA {ema50} | 100 EMA {ema100} "
        f"| 50 slope {round(ema50_slope, precision)} | 100 slope {round(ema100_slope_pct * 100, 4)}% "
        f"| TP {round(last_close * (1 - TP_PCT), precision)} "
        f"| SL {round(last_close * (1 + SL_PCT), precision)}"
    )

    # ── Place trade ───────────────────────────────────────────────────────────
    tp_confirmed, sl_placed = place_order(
        "sell", symbol, last_close, precision
    )
    if tp_confirmed:
        update_sheet_tp(row, tp_confirmed)
    if sl_placed:
        update_sheet_sl(row, sl_placed)


# =====================================================
# MAIN LOOP
# =====================================================

cycle = 0

send_telegram(
    f"✅ <b>Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy : <code>50/100 Pullback Short</code>\n"
    f"⏱ Timeframe : <code>30 Min</code>\n"
    f"📉 Entry    : <code>Price below 50 EMA</code>\n"
    f"✅ Context  : <code>50 EMA above 100 EMA</code>\n"
    f"🎯 TP       : <code>{TP_PCT * 100:.1f}% fixed</code>\n"
    f"🛑 SL       : <code>{int(SL_PCT * 100)}% fixed above entry</code>\n"
    f"💰 Capital  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
    f"🕐 Scanning every 30 seconds..."
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            time.sleep(30)
            continue

        cycle += 1
        if cycle % 10 == 0:
            print("----- TRADE SCAN (5 MIN) -----")
        else:
            print("----- TP / SL MONITOR (30s) -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_and_trade(symbol, row, df)

        time.sleep(30)

    except Exception as e:
        print("BOT ERROR:", e)
        time.sleep(60)