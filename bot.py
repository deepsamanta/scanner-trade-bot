import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# ─── TUNEABLE CONSTANTS ────────────────────────────────────────────────────────
EMA_PERIOD        = 200
ATR_PERIOD        = 14          # candles for ATR
ATR_TP_MULT       = 2.5         # TP  = entry - ATR * mult   (was fixed 5 %)
ATR_SL_MULT       = 1.2         # SL  = prev_high + ATR * mult
MIN_RR            = 1.8         # skip trade if reward/risk < this
EMA_ENTRY_PCT_HI  = 0.995       # entry zone upper bound  (0.5 % below EMA)
EMA_ENTRY_PCT_LO  = 0.970       # entry zone lower bound  (3 %  below EMA)
RSI_PERIOD        = 14
RSI_MIN           = 40          # shorts: only enter when RSI <= this
RSI_MAX           = 60          # hard upper cap (momentum not yet reversing)
EMA_SLOPE_BARS    = 5           # candles to measure EMA slope
VOL_SPIKE_MULT    = 1.3         # current vol must be > avg_vol * this
TRAIL_TRIGGER_PCT = 0.015       # move SL to break-even once price drops 1.5 %
# ──────────────────────────────────────────────────────────────────────────────


# =====================================================
# GOOGLE SHEETS CONNECTION
# =====================================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file("service_account.json", scopes=scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1


# =====================================================
# READ / WRITE SHEET
# =====================================================

def get_sheet_data():
    try:
        data = sheet.get_all_values()
        df = pd.DataFrame(data)
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
    payload = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(
        COINDCX_SECRET.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": COINDCX_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    return payload, headers


# =====================================================
# INDICATOR HELPERS
# =====================================================

def compute_ema(closes, period):
    """Returns list of EMA values aligned with closes[period-1:]."""
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    values = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    return values


def compute_atr(candles, period=ATR_PERIOD):
    """Average True Range over last `period` candles."""
    trs = []
    for i in range(1, len(candles)):
        high  = float(candles[i]["high"])
        low   = float(candles[i]["low"])
        prev_close = float(candles[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def compute_rsi(closes, period=RSI_PERIOD):
    """Wilder RSI."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def volume_spike(candles, lookback=20, mult=VOL_SPIKE_MULT):
    """True if the latest candle volume > mult * average of prior `lookback` candles."""
    if len(candles) < lookback + 1:
        return True  # can't judge → don't block
    vols = [float(c.get("volume", 0)) for c in candles]
    avg_vol = sum(vols[-(lookback + 1):-1]) / lookback
    cur_vol = vols[-1]
    if avg_vol == 0:
        return True
    return cur_vol >= avg_vol * mult


def bearish_candle(candle):
    """Latest candle must close below its open (bearish body)."""
    return float(candle["close"]) < float(candle["open"])


# =====================================================
# OPEN POSITIONS
# =====================================================

def get_open_positions():
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "page": "1",
            "size": "50",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        url = BASE_URL + "/exchange/v1/derivatives/futures/positions"
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
# WICK TP DETECTION
# =====================================================

def get_recent_low(symbol):
    try:
        pair_api = fut_pair(symbol)
        url = "https://public.coindcx.com/market_data/candlesticks"
        now = int(time.time())
        params = {
            "pair": pair_api,
            "from": now - 180,
            "to": now,
            "resolution": "1",
            "pcode": "f",
        }
        response = requests.get(url, params=params)
        candles = response.json()["data"]
        lows = [float(c["low"]) for c in candles]
        return min(lows)
    except Exception:
        return None


# =====================================================
# TRAILING STOP UPDATE
# =====================================================

def maybe_update_trailing_sl(symbol, row, df, precision):
    """
    If price has dropped TRAIL_TRIGGER_PCT below entry, move SL to break-even.
    Reads current SL from column C. Updates exchange order and sheet.
    """
    try:
        entry = get_position_entry(symbol)
        if entry is None:
            return

        pair_api = fut_pair(symbol)
        url = "https://public.coindcx.com/market_data/candlesticks"
        now = int(time.time())
        params = {"pair": pair_api, "from": now - 60, "to": now, "resolution": "1", "pcode": "f"}
        resp = requests.get(url, params=params)
        last_price = float(resp.json()["data"][-1]["close"])

        trail_target = entry * (1 - TRAIL_TRIGGER_PCT)
        if last_price > trail_target:
            return  # not far enough in profit yet

        sl_raw = df.iloc[row, 2] if df.shape[1] > 2 else ""
        try:
            current_sl = float(sl_raw)
        except Exception:
            current_sl = None

        breakeven_sl = round(entry * 1.0003, precision)  # tiny buffer above entry

        if current_sl is not None and current_sl <= breakeven_sl:
            return  # already at or better than break-even

        # Push updated SL to exchange
        body = {
            "timestamp": int(time.time() * 1000),
            "pair": pair_api,
            "stop_loss_price": breakeven_sl,
        }
        payload, headers = sign_request(body)
        requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/positions/update_sl",
            data=payload,
            headers=headers,
        )
        update_sheet_sl(row, breakeven_sl)
        print(f"[TRAIL] {symbol} SL moved to break-even {breakeven_sl}")

    except Exception as e:
        print(f"[TRAIL] {symbol} error: {e}")


# =====================================================
# QUANTITY
# =====================================================

def get_quantity_step(symbol):
    try:
        pair = fut_pair(symbol)
        url = (
            f"https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
            f"?pair={pair}&margin_currency_short_name=USDT"
        )
        response = requests.get(url)
        data = response.json()
        instrument = data["instrument"]
        quantity_increment = Decimal(str(instrument["quantity_increment"]))
        min_quantity = Decimal(str(instrument["min_quantity"]))
        return max(quantity_increment, min_quantity)
    except Exception:
        return Decimal("1")


def compute_qty(entry_price, symbol, atr):
    """
    Risk-adjusted sizing: risk exactly 1 % of capital per trade.
    qty = (capital * risk_pct) / (ATR * ATR_SL_MULT)
    Falls back to leverage-based sizing when ATR unavailable.
    """
    step = get_quantity_step(symbol)
    capital = Decimal(str(CAPITAL_USDT))
    leverage = Decimal(str(LEVERAGE))

    if atr and atr > 0:
        risk_amount = capital * Decimal("0.01")          # 1 % of capital at risk
        sl_distance = Decimal(str(atr)) * Decimal(str(ATR_SL_MULT))
        raw_qty = risk_amount / sl_distance
    else:
        exposure = capital * leverage
        raw_qty = exposure / Decimal(str(entry_price))

    qty = (raw_qty / step).quantize(Decimal("1")) * step
    if qty <= 0:
        qty = step
    qty = qty.quantize(step)
    return float(qty)


# =====================================================
# PLACE ORDER
# =====================================================

def place_order(side, symbol, entry_price, candles, atr):
    precision = len(str(entry_price).split(".")[1]) if "." in str(entry_price) else 0
    entry = round(entry_price, precision)

    # ── Dynamic TP / SL via ATR ──────────────────────────────────────────────
    if atr:
        tp = round(entry - atr * ATR_TP_MULT, precision)
        sl_base = round(float(candles[-2]["high"]) + atr * ATR_SL_MULT, precision)
    else:
        tp = round(entry * 0.95, precision)
        sl_base = round(float(candles[-2]["high"]) * 1.001, precision)

    # ── Reward / Risk gate ───────────────────────────────────────────────────
    reward = entry - tp
    risk   = sl_base - entry
    if risk <= 0 or (reward / risk) < MIN_RR:
        print(f"[SKIP] {symbol} RR {round(reward/risk, 2) if risk>0 else 'inf'} < {MIN_RR}")
        return None, None

    qty = compute_qty(entry_price, symbol, atr)

    print(
        f"[TRADE] {symbol} SELL | Entry {entry} | TP {tp} | SL {sl_base} "
        f"| RR {round(reward/risk, 2)} | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side": side,
            "pair": fut_pair(symbol),
            "order_type": "limit_order",
            "price": entry,
            "total_quantity": qty,
            "leverage": LEVERAGE,
            "take_profit_price": tp,
            "stop_loss_price": sl_base,
            "position_margin_type": "crossed",
        },
    }

    payload, headers = sign_request(body)
    response = requests.post(
        BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
        data=payload,
        headers=headers,
    )
    result = response.json()

    try:
        tp_confirmed = result["order"]["take_profit_trigger"]
    except Exception:
        tp_confirmed = tp

    return tp_confirmed, sl_base


# =====================================================
# MAIN LOGIC
# =====================================================

def check_ema_and_trade(symbol, row, df, allow_trade):
    pair_api = fut_pair(symbol)
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(time.time())

    params = {
        "pair": pair_api,
        "from": now - 360000,
        "to": now,
        "resolution": "15",
        "pcode": "f",
    }

    response = requests.get(url, params=params)
    candles = sorted(response.json()["data"], key=lambda x: x["time"])
    closes  = [float(c["close"]) for c in candles]

    if len(closes) < EMA_PERIOD + RSI_PERIOD + 5:
        return

    # ── Indicators ───────────────────────────────────────────────────────────
    ema_values  = compute_ema(closes, EMA_PERIOD)
    atr         = compute_atr(candles)
    rsi         = compute_rsi(closes)

    last_close  = closes[-1]
    prev_close  = closes[-2]

    precision = len(str(last_close).split(".")[1]) if "." in str(last_close) else 0
    ema        = round(ema_values[-1], precision)

    # ── Filter 1: EMA slope must be declining ────────────────────────────────
    if len(ema_values) > EMA_SLOPE_BARS:
        slope = ema_values[-1] - ema_values[-EMA_SLOPE_BARS]
        if slope >= 0:
            print(f"[SKIP] {symbol} EMA slope not down ({round(slope, 6)})")
            return

    # ── Filter 2: Previous candle closed below EMA ───────────────────────────
    if prev_close >= ema:
        print(f"[SKIP] {symbol} prev close {prev_close} >= EMA {ema}")
        return

    # ── Filter 3: RSI confirmation (bearish momentum, not oversold bounce) ───
    if rsi is not None:
        if rsi < RSI_MIN or rsi > RSI_MAX:
            print(f"[SKIP] {symbol} RSI {rsi} outside [{RSI_MIN}, {RSI_MAX}]")
            return

    # ── Filter 4: Volume spike on signal candle ───────────────────────────────
    if not volume_spike(candles):
        print(f"[SKIP] {symbol} no volume spike")
        return

    # ── Filter 5: Current candle must be bearish ─────────────────────────────
    if not bearish_candle(candles[-1]):
        print(f"[SKIP] {symbol} latest candle not bearish")
        return

    ema_upper = round(ema * EMA_ENTRY_PCT_HI, precision)
    ema_lower = round(ema * EMA_ENTRY_PCT_LO, precision)

    print(
        f"[CHECK] {symbol} | Price {last_close} | EMA {ema} "
        f"| ATR {round(atr, precision) if atr else 'N/A'} | RSI {rsi}"
    )

    # ── TP completed check ───────────────────────────────────────────────────
    tp_raw = df.iloc[row, 1]

    if str(tp_raw).upper() == "TP COMPLETED":
        return

    try:
        tp = float(tp_raw)
        if last_close <= tp:
            update_sheet_tp(row, "TP COMPLETED")
            return
        recent_low = get_recent_low(symbol)
        if recent_low and recent_low <= tp:
            update_sheet_tp(row, "TP COMPLETED")
            return
    except Exception:
        pass

    # ── Active position: update TP in sheet + trailing SL ────────────────────
    positions = get_open_positions()
    pair      = fut_pair(symbol)

    for pos in positions:
        if pos.get("pair") == pair:
            print(f"[ACTIVE] {symbol}")
            tp_live = get_position_tp(symbol)
            if tp_live:
                update_sheet_tp(row, tp_live)
            maybe_update_trailing_sl(symbol, row, df, precision)
            return

    # ── New trade entry ───────────────────────────────────────────────────────
    if allow_trade and ema_lower <= last_close <= ema_upper:
        tp_confirmed, sl_placed = place_order("sell", symbol, last_close, candles, atr)
        if tp_confirmed:
            update_sheet_tp(row, tp_confirmed)
        if sl_placed:
            update_sheet_sl(row, sl_placed)


# =====================================================
# MAIN LOOP
# =====================================================

cycle = 0

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            time.sleep(30)
            continue

        allow_trade = (cycle % 10 == 0)

        if allow_trade:
            print("----- TRADE SCAN (5 MIN) -----")
        else:
            print("----- TP / SL MONITOR (30s) -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_ema_and_trade(symbol, row, df, allow_trade)

        cycle += 1
        time.sleep(30)

    except Exception as e:
        print("BOT ERROR:", e)
        time.sleep(60)