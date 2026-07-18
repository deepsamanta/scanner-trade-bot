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
# STRATEGY: Multi-Stage Momentum Breakout — All 430 Instruments
#
# Sheet now contains ALL CoinDCX USDT futures instruments (~430).
# Each cycle the bot self-selects the best MAX_OPEN_TRADES setups via 4 stages:
#
# STAGE 1 — UNIVERSE FILTER  (cycle-level, single ticker API call):
#   • 24h USD volume >= MIN_24H_VOL_USDT  — ensures meaningful liquidity
#   • Exclude stablecoins and wrapped tokens
#   • BTC daily regime gate: close > 200 EMA -> bull mode; else no new longs
#   Typical result: 430 -> ~40-80 instruments.
#
# STAGE 2 — STRUCTURAL SCREEN  (per-symbol, candle-based):
#   • 4H trend: last close > 50-period EMA on 4H  (trade only with trend)
#   • 1H ATR compression: ATR(14) on 1H < ATR_COMPRESS_PCT% of price
#       (tight coiling = energy building before the next move)
#   • Not already extended: max high-low swing in last 5 days < PUMP_SKIP_PCT%
#   Typical result: 40-80 -> ~10-20 instruments.
#
# STAGE 3 — ENTRY SIGNAL  (per-symbol, 15m candle-based):
#   • 15m breakout: last closed candle close > max close of prior BREAKOUT_BARS bars
#   • 15m volume spike: breakout candle volume >= VOL_SPIKE_MULT x avg of those bars
#   • 1H HTF confirmation: latest 1H candle bullish AND 1H volume rising
#   Typical result: 10-20 -> ~3-8 candidates.
#
# STAGE 4 — RANKING  (cycle-level, all Stage-3 survivors scored):
#   Weighted score across 4 factors:
#     (1) Volume spike ratio    30 pts  — strength of the volume surge
#     (2) Breakout strength     20 pts  — how far above the 20-bar high
#     (3) 4H EMA50 proximity    20 pts  — prefer near EMA, not extended
#     (4) 24h USD volume rank   30 pts  — prefer more liquid instruments
#   -> Top MAX_OPEN_TRADES symbols get actual entries
#      (slots = MAX_OPEN_TRADES - currently open positions).
#
# ENTRY  : Limit order at breakout 15m candle close
# TP     : entry x (1 + TP_PCT / 100)
# SL     : entry x (1 - SL_PCT / 100)
# =============================================================================

# ── Trade params ──────────────────────────────────────────────────────────────
TP_PCT             = 8    # take-profit %
SL_PCT             = 4    # stop-loss %
MAX_OPEN_TRADES    = 5    # max concurrent positions

# ── Universe filter ───────────────────────────────────────────────────────────
MIN_24H_VOL_USDT   = 1_000_000   # $1M 24h USD volume floor

STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "UST", "LUSD",
    "FDUSD", "PYUSD", "USDD", "USDN", "GUSD", "SUSD", "CUSD", "USDX", "OUSD",
}
WRAPPED = {"WBTC", "WETH", "WBNB", "WMATIC", "WAVAX", "WSOL", "WFTM"}

# ── Strategy params ───────────────────────────────────────────────────────────
EMA200_DAILY_LEN   = 200   # BTC daily 200 EMA for regime gate
EMA50_4H_LEN       = 50    # 4H trend filter
ATR_LEN            = 14    # 1H ATR for compression check
ATR_COMPRESS_PCT   = 2.5   # max ATR% for market to be "coiling"
BREAKOUT_BARS      = 20    # 15m bars that define the prior high
VOL_SPIKE_MULT     = 1.5   # breakout candle volume >= this x prior avg
HTF_VOL_BARS       = 2     # bars per window for 1H volume comparison
PUMP_LOOKBACK_BARS = 480   # 5 days in 15m candles
PUMP_SKIP_PCT      = 15    # skip if 5-day swing >= this %

# ── Candle counts ─────────────────────────────────────────────────────────────
CANDLES_15M        = 550   # pump lookback (480) + BREAKOUT_BARS + buffer
CANDLES_4H         = 65    # EMA50_4H_LEN + buffer
CANDLES_1H         = 30    # ATR(14) + HTF vol (4) + buffer
CANDLES_1M         = 5

# ── Resolutions ───────────────────────────────────────────────────────────────
RESOLUTION_15M     = "15"
RESOLUTION_1M      = "1"
RESOLUTION_DAILY   = "1D"
RESOLUTION_1H      = "60"
RESOLUTION_4H      = "240"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1M  = 60
CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

# ── Timing ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "atl_bot_state.json"


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
        "in_position":     False,
        "direction":       None,
        "entry_price":     None,
        "tp_level":        None,
        "sl_price":        None,
        "last_entry_ts":   0,
        "current_day_str": None,
        "last_candle_ts":  0,
        "tp_completed":    False,
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


# =====================================================
# MATH UTILITIES
# =====================================================

def compute_ema(values, length):
    """Standard EMA. Returns None if insufficient data."""
    if len(values) < length:
        return None
    k   = 2 / (length + 1)
    ema = sum(values[:length]) / length
    for v in values[length:]:
        ema = v * k + ema * (1 - k)
    return ema


def compute_atr(candles, length):
    """Wilder's ATR (RMA smoothing). Returns None if insufficient data."""
    if len(candles) < length + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h  = float(candles[i]["high"])
        l  = float(candles[i]["low"])
        pc = float(candles[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < length:
        return None
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


# =====================================================
# STAGE 1 — UNIVERSE FILTER
# =====================================================

def fetch_24h_tickers():
    """Single API call — returns all CoinDCX 24h ticker data keyed by market."""
    try:
        r = requests.get(BASE_URL + "/exchange/ticker", timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            print(f"[TICKER] HTTP {r.status_code}")
            return {}
        data = r.json()
        return {t["market"]: t for t in data} if isinstance(data, list) else {}
    except Exception as e:
        print(f"[TICKER] Error: {e}")
        return {}


def get_btc_regime():
    """
    Fetch BTC daily candles, compute 200 EMA.
    Returns (is_bull: bool, close: float, ema200: float).
    """
    try:
        candles = fetch_candles("BTCUSDT", 220, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
        if len(candles) < EMA200_DAILY_LEN:
            print(f"[BTC REGIME] Only {len(candles)} daily candles — defaulting to bull")
            return True, 0.0, 0.0
        closes = [float(c["close"]) for c in candles]
        ema200 = compute_ema(closes, EMA200_DAILY_LEN)
        is_bull = closes[-1] > ema200 if ema200 else True
        print(f"[BTC REGIME] close={closes[-1]:,.2f}  EMA200={ema200:,.2f}  bull={is_bull}")
        return is_bull, closes[-1], ema200 or 0.0
    except Exception as e:
        print(f"[BTC REGIME] Error: {e} — defaulting to bull")
        return True, 0.0, 0.0


def is_excluded(symbol):
    """True if stablecoin or wrapped token."""
    base = symbol.replace("USDT", "")
    return base in STABLECOINS or base in WRAPPED


def build_eligible_universe(all_symbols_rows, tickers):
    """
    Stage 1: apply volume floor + exclusions.
    Returns list of (symbol, row, vol_24h_usd) sorted by volume desc.
    """
    eligible    = []
    skip_stable = 0
    skip_vol    = 0

    for symbol, row in all_symbols_rows:
        if is_excluded(symbol):
            skip_stable += 1
            continue
        ticker = tickers.get(fut_pair(symbol), {})
        try:
            vol_usd = float(ticker.get("volume", 0)) * float(ticker.get("last_price", 0))
        except (TypeError, ValueError):
            vol_usd = 0.0

        if vol_usd < MIN_24H_VOL_USDT:
            skip_vol += 1
            continue

        eligible.append((symbol, row, vol_usd))

    eligible.sort(key=lambda x: x[2], reverse=True)
    print(
        f"[UNIVERSE] {len(all_symbols_rows)} total  |  "
        f"-{skip_stable} stables/wrapped  |  "
        f"-{skip_vol} low-vol  |  "
        f"-> {len(eligible)} eligible"
    )
    return eligible


# =====================================================
# STAGES 2-3 — STRUCTURAL SCREEN + ENTRY SIGNAL
# =====================================================

def check_4h_trend(candles_4h):
    """
    Stage 2a — Trend gate.
    Last 4H close > 50-period EMA. Returns (ok, close, ema50).
    """
    if len(candles_4h) < EMA50_4H_LEN:
        return False, 0.0, 0.0
    closes = [float(c["close"]) for c in candles_4h]
    ema50  = compute_ema(closes, EMA50_4H_LEN)
    if ema50 is None:
        return False, 0.0, 0.0
    last_close = closes[-1]
    return last_close > ema50, round(last_close, 8), round(ema50, 8)


def check_1h_compression(candles_1h):
    """
    Stage 2b — ATR compression.
    1H ATR(14) < ATR_COMPRESS_PCT% of price -> market is coiling.
    Returns (ok, atr_pct, atr).
    """
    atr = compute_atr(candles_1h, ATR_LEN)
    if atr is None:
        return False, 0.0, 0.0
    last_close = float(candles_1h[-1]["close"])
    atr_pct    = (atr / last_close) * 100 if last_close > 0 else 999.0
    return atr_pct < ATR_COMPRESS_PCT, round(atr_pct, 4), round(atr, 8)


def check_not_extended(candles_15m):
    """
    Stage 2c — Pump guard.
    If max high-to-low swing in last PUMP_LOOKBACK_BARS 15m candles
    >= PUMP_SKIP_PCT%, the move already happened — skip.
    Returns already_extended (bool).
    """
    if len(candles_15m) < PUMP_LOOKBACK_BARS:
        return False
    window = candles_15m[-PUMP_LOOKBACK_BARS:]
    lo     = min(float(c["low"])  for c in window)
    hi     = max(float(c["high"]) for c in window)
    if lo <= 0:
        return False
    return ((hi - lo) / lo) * 100 >= PUMP_SKIP_PCT


def check_15m_breakout(candles_15m):
    """
    Stage 3a — 15m breakout + volume spike.
    • close[-1] > max(close of prior BREAKOUT_BARS candles)
    • volume[-1] >= VOL_SPIKE_MULT x avg(volume of prior BREAKOUT_BARS candles)
    Returns (ok, curr_close, prev_high, vol_ratio).
    """
    needed = BREAKOUT_BARS + 1
    if len(candles_15m) < needed:
        return False, 0.0, 0.0, 0.0

    curr      = candles_15m[-1]
    prev_bars = candles_15m[-(BREAKOUT_BARS + 1):-1]

    curr_close = float(curr["close"])
    curr_vol   = float(curr["volume"])
    prev_high  = max(float(c["close"]) for c in prev_bars)
    avg_vol    = sum(float(c["volume"]) for c in prev_bars) / len(prev_bars) if prev_bars else 0

    vol_ratio  = curr_vol / avg_vol if avg_vol > 0 else 0.0
    price_ok   = curr_close > prev_high
    vol_ok     = vol_ratio >= VOL_SPIKE_MULT

    return (price_ok and vol_ok), round(curr_close, 8), round(prev_high, 8), round(vol_ratio, 2)


def check_1h_confirmation(candles_1h):
    """
    Stage 3b — 1H HTF confirmation.
    • 1H volume rising (last HTF_VOL_BARS > preceding HTF_VOL_BARS)
    • Latest closed 1H candle bullish (close > open)
    Returns (ok, avg_recent, avg_prev, is_bullish).
    """
    needed = HTF_VOL_BARS * 2
    if len(candles_1h) < needed:
        return False, 0.0, 0.0, False

    recent_vols = [float(c["volume"]) for c in candles_1h[-HTF_VOL_BARS:]]
    prev_vols   = [float(c["volume"]) for c in candles_1h[-(HTF_VOL_BARS * 2):-HTF_VOL_BARS]]
    avg_recent  = sum(recent_vols) / len(recent_vols)
    avg_prev    = sum(prev_vols)   / len(prev_vols)
    vol_rising  = avg_recent > avg_prev

    last       = candles_1h[-1]
    is_bullish = float(last["close"]) > float(last["open"])

    return (vol_rising and is_bullish), round(avg_recent, 2), round(avg_prev, 2), is_bullish


# =====================================================
# STAGE 4 — SCORING
# =====================================================

def score_candidate(vol_ratio, breakout_strength_pct, ema_proximity_pct, vol_24h_usd):
    """
    0-100 scale. Weights:
      (1) Volume spike ratio   30 pts  (5x vol = max)
      (2) Breakout strength    20 pts  (5% above high = max)
      (3) 4H EMA50 proximity   20 pts  (0% above EMA = max, 10%+ = 0)
      (4) 24h USD liquidity    30 pts  ($50M+ = max)
    """
    s1 = min(vol_ratio / 5.0,               1.0) * 30
    s2 = min(breakout_strength_pct / 5.0,   1.0) * 20
    s3 = max(0, 1 - ema_proximity_pct / 10) * 20
    s4 = min(vol_24h_usd / 50_000_000,      1.0) * 30
    return round(s1 + s2 + s3 + s4, 4)


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
# PLACE ORDER (LONG ONLY)
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    expected_tp = round(entry * (1 + TP_PCT / 100), precision)
    if tp != expected_tp:
        print(f"  [WARN] TP mismatch corrected: {tp} -> {expected_tp}")
        tp = expected_tp

    tp_pct = round(((tp - entry) / entry) * 100, 2)
    sl_pct = round(((entry - sl) / entry) * 100, 2)
    print(f"  [LONG] Entry={entry}  TP={tp}(+{tp_pct}%)  SL={sl}(-{sl_pct}%)  Qty={qty}")

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
        return False, None, None

    print(f"  [API] {symbol}: {result}")
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None

    send_telegram(
        f"🟢 <b>NEW LONG (MOMENTUM BREAKOUT) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True, entry, tp


# =====================================================
# EXECUTE ENTRY — called only for top-ranked candidates
# =====================================================

def execute_entry(cand, all_state):
    """
    Place order for a pre-qualified, top-ranked candidate and update state.
    cand keys: symbol, row, entry_price, tp_price, sl_price, precision, curr_ts
    """
    symbol      = cand["symbol"]
    row         = cand["row"]
    entry_price = cand["entry_price"]
    tp_price    = cand["tp_price"]
    sl_price    = cand["sl_price"]
    precision   = cand["precision"]
    curr_ts     = cand["curr_ts"]

    st = all_state.setdefault(symbol, init_symbol_state())

    placed, confirmed_entry, confirmed_tp = place_long_order(
        symbol, entry_price, tp_price, sl_price, precision
    )

    if placed:
        st["in_position"]   = True
        st["direction"]     = "long"
        st["entry_price"]   = confirmed_entry
        st["tp_level"]      = confirmed_tp
        st["sl_price"]      = round(sl_price, precision)
        st["last_entry_ts"] = curr_ts
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders, vol_24h=0.0):
    """
    Runs ALL state management (TP hit, position reconciliation, day reset, dedup).
    If symbol qualifies for a new entry, returns a candidate dict for ranking.
    Does NOT place orders — execute_entry() handles that after ranking.
    Returns: candidate dict | None
    """
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch 15m candles ──────────────────────────────────────────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]

    min_15m = PUMP_LOOKBACK_BARS + BREAKOUT_BARS + 5
    if len(candles_15m) < min_15m:
        print(f"  [{symbol}] SKIP — insufficient 15m candles ({len(candles_15m)} < {min_15m})")
        return None

    # ── 2. State init / backfill ──────────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset ──────────────────────────────────────────────────────
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — resetting daily state")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    precision = get_precision(float(candles_15m[-1]["close"]))

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────────
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""

    if tp_raw.upper() == "TP COMPLETED" or st.get("tp_completed") is True:
        print(f"  [{symbol}] SKIP — TP COMPLETED")
        if st.get("in_position"):
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
        return None

    # ── 5. Resolve TP target from state then sheet fallback ───────────────────
    tp_stored = st.get("tp_level")
    if not tp_stored:
        try:
            v = float(tp_raw)
            if v > 0:
                tp_stored      = v
                st["tp_level"] = v
        except (ValueError, TypeError):
            tp_stored = None

    if tp_stored and tp_stored > 0:
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit     = False
        hit_kind   = None
        hit_price  = None

        tp_threshold = tp_stored * 0.9999

        if last_close and last_close >= tp_threshold:
            tp_hit, hit_kind, hit_price = True, "close", last_close
        if not tp_hit:
            rh = get_recent_high(symbol)
            if rh and rh >= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "wick", rh

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"  [{symbol}] TP HIT ({hit_kind}) price={hit_price} target={tp_stored}")
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
            return None

    # ── 6. Reconcile with exchange ────────────────────────────────────────────
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
        return None   # already in position — no new entry

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
        return None

    # ── 7. Candle dedup guard ─────────────────────────────────────────────────
    curr    = candles_15m[-1]
    curr_ts = int(curr["time"])
    curr_c  = float(curr["close"])

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — candle already processed")
        save_state(all_state)
        return None

    # ── 8. Fetch 4H and 1H candles ───────────────────────────────────────────
    candles_4h = fetch_candles(symbol, CANDLES_4H, RESOLUTION_4H, CANDLE_SECONDS_4H)
    candles_1h = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)

    if candles_4h and (now_ms - int(candles_4h[-1]["time"])) < CANDLE_SECONDS_4H * 1000:
        candles_4h = candles_4h[:-1]
    if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
        candles_1h = candles_1h[:-1]

    # ── 9. STAGE 2 — Structural screen ───────────────────────────────────────
    trend_ok, close_4h, ema50_4h = check_4h_trend(candles_4h)
    print(f"  [{symbol}] 4h_trend={trend_ok}  close={close_4h}  ema50={ema50_4h}")

    compress_ok, atr_pct, _ = check_1h_compression(candles_1h)
    print(f"  [{symbol}] compression={compress_ok}  atr_pct={atr_pct}%")

    extended = check_not_extended(candles_15m)
    print(f"  [{symbol}] already_extended={extended}")

    if not trend_ok or not compress_ok or extended:
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None

    # ── 10. STAGE 3 — Entry signal ────────────────────────────────────────────
    bo_ok, bo_close, prev_high, vol_ratio = check_15m_breakout(candles_15m)
    print(f"  [{symbol}] breakout={bo_ok}  close={bo_close}  "
          f"prev_high={prev_high}  vol_ratio={vol_ratio}x")

    htf_ok, htf_recent, htf_prev, htf_bull = check_1h_confirmation(candles_1h)
    print(f"  [{symbol}] 1h_confirm={htf_ok}  "
          f"avg_recent={htf_recent}  avg_prev={htf_prev}  bullish={htf_bull}")

    st["last_candle_ts"] = curr_ts

    if not bo_ok or not htf_ok:
        save_state(all_state)
        return None

    # ── 11. STAGE 4 — Build candidate dict with score ─────────────────────────
    breakout_pct = ((bo_close - prev_high) / prev_high * 100) if prev_high > 0 else 0.0
    ema_prox_pct = ((bo_close / ema50_4h  - 1) * 100)        if ema50_4h  > 0 else 0.0

    entry_price  = round(curr_c, precision)
    tp_price     = round(entry_price * (1 + TP_PCT / 100), precision)
    sl_price     = round(entry_price * (1 - SL_PCT / 100), precision)

    candidate_score = score_candidate(vol_ratio, breakout_pct, ema_prox_pct, vol_24h)

    print(f"  [{symbol}] CANDIDATE  score={candidate_score}  "
          f"entry={entry_price}  tp={tp_price}  sl={sl_price}")

    return {
        "symbol":       symbol,
        "row":          row,
        "score":        candidate_score,
        "entry_price":  entry_price,
        "tp_price":     tp_price,
        "sl_price":     sl_price,
        "precision":    precision,
        "curr_ts":      curr_ts,
        "vol_ratio":    vol_ratio,
        "breakout_pct": round(breakout_pct, 4),
        "ema_prox_pct": round(ema_prox_pct, 4),
    }


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Momentum Breakout Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy : <code>Multi-Stage Momentum Breakout (All Instruments)</code>\n"
    f"\n"
    f"🔍 Stage 1 (Universe):\n"
    f"  <code>• 24h vol &gt;= ${MIN_24H_VOL_USDT:,} USD</code>\n"
    f"  <code>• Exclude stables / wrapped tokens</code>\n"
    f"  <code>• BTC daily close &gt; 200 EMA (bull regime)</code>\n"
    f"\n"
    f"🔍 Stage 2 (Structure):\n"
    f"  <code>• 4H close &gt; 50 EMA</code>\n"
    f"  <code>• 1H ATR(14) &lt; {ATR_COMPRESS_PCT}% (compression)</code>\n"
    f"  <code>• 5-day swing &lt; {PUMP_SKIP_PCT}% (not extended)</code>\n"
    f"\n"
    f"🔍 Stage 3 (Signal):\n"
    f"  <code>• 15m close &gt; {BREAKOUT_BARS}-bar high + {VOL_SPIKE_MULT}x vol spike</code>\n"
    f"  <code>• 1H bullish candle + rising volume</code>\n"
    f"\n"
    f"📊 Stage 4 (Rank): <code>Top {MAX_OPEN_TRADES} by weighted score</code>\n"
    f"🎯 TP : <code>+{TP_PCT}%</code>  |  🛑 SL: <code>-{SL_PCT}%</code>\n"
    f"🔁 Scan: <code>Every {SCAN_INTERVAL}s</code>  |  "
    f"💰 <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
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

        print(f"\n===== CYCLE {cycle} | "
              f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} | "
              f"positions={len(global_positions)} orders={len(global_orders)} =====")

        # ── Build full symbol list from sheet ────────────────────────────────
        all_symbols_rows = []
        row_index        = {}   # symbol -> sheet row (for force-include)
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if symbol:
                all_symbols_rows.append((symbol, row))
                row_index[symbol] = row

        # ── STAGE 1: BTC regime + universe filter ────────────────────────────
        btc_bull, btc_close, btc_ema200 = get_btc_regime()
        if not btc_bull:
            print(f"[REGIME] Bear market (BTC {btc_close:,.2f} < EMA200 {btc_ema200:,.2f}) "
                  f"— no new longs this cycle")

        tickers  = fetch_24h_tickers()
        eligible = build_eligible_universe(all_symbols_rows, tickers) if btc_bull else []

        # Force-include symbols with active tracked state so TP monitoring and
        # reconciliation still run even if the symbol was filtered by Stage 1
        # (e.g. volume dropped, bear regime kicked in after entry).
        # vol_24h=0 means they score 0 and will never win the ranking for new entries.
        eligible_set = {s for s, _, _ in eligible}
        for sym, sym_st in state.items():
            if (sym_st.get("in_position") or sym_st.get("tp_level")) and sym not in eligible_set:
                r = row_index.get(sym)
                if r is not None:
                    eligible.append((sym, r, 0.0))
                    eligible_set.add(sym)
                    print(f"[FORCE-INCLUDE] {sym} — active state, monitoring only")

        # ── Slot calculation ─────────────────────────────────────────────────
        active_count    = len(global_positions)
        slots_available = max(0, MAX_OPEN_TRADES - active_count)
        print(f"[SLOTS] {active_count} open / {MAX_OPEN_TRADES} max -> {slots_available} slot(s)")

        # ── STAGES 2-3: Score each eligible symbol, collect candidates ───────
        candidates = []

        for symbol, row, vol_24h in eligible:
            print(f"--- {symbol} (24h_vol=${vol_24h:,.0f}) ---")
            try:
                cand = check_and_trade(
                    symbol, row, df, state, global_positions, global_orders, vol_24h
                )
                if cand:
                    candidates.append(cand)
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")
                continue

        # ── STAGE 4: Rank and execute top N ──────────────────────────────────
        candidates.sort(key=lambda x: x["score"], reverse=True)

        print(f"\n[RANKING] {len(candidates)} candidate(s) | {slots_available} slot(s) available")
        for i, c in enumerate(candidates):
            tag = f"EXECUTE #{i + 1}" if i < slots_available else "SKIP"
            print(f"  [{tag}] {c['symbol']}  score={c['score']}  "
                  f"vol_ratio={c['vol_ratio']}x  "
                  f"breakout={c['breakout_pct']}%  "
                  f"ema_prox={c['ema_prox_pct']}%")

        for cand in candidates[:slots_available]:
            try:
                execute_entry(cand, state)
            except Exception as e:
                print(f"  [{cand['symbol']}] ENTRY ERROR: {e}")

        # ── Telegram cycle summary (only when there are candidates) ───────────
        if candidates:
            executed = candidates[:slots_available]
            skipped  = candidates[slots_available:]
            msg = (
                f"📊 <b>Cycle {cycle} — Ranking Summary</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔍 Eligible : <code>{len(eligible)}</code>  "
                f"✅ Qualified: <code>{len(candidates)}</code>\n"
                f"🟢 Executed : <code>{len(executed)}</code>\n"
            )
            for c in executed:
                msg += (f"  • {c['symbol']}  "
                        f"score={c['score']}  entry={c['entry_price']}\n")
            if skipped:
                msg += (f"⏭ Skipped : "
                        f"<code>{', '.join(c['symbol'] for c in skipped)}</code>")
            send_telegram(msg)

        print(f"===== CYCLE {cycle} DONE — {len(eligible)} symbols scanned =====")
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