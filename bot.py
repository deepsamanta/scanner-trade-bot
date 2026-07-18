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
# STRATEGY: Multi-Stage Momentum Breakout/Breakdown — Long + Short + ATR TP/SL
#
# Sheet contains ALL CoinDCX USDT futures instruments (~430).
# BTC daily 200 EMA determines direction each cycle:
#   BTC > 200 EMA  ->  LONG mode   (breakout + uptrend + compression)
#   BTC < 200 EMA  ->  SHORT mode  (breakdown + downtrend + compression)
#
# STAGE 1 — UNIVERSE FILTER  (per-symbol, from 15m candle volume):
#   • Exclude stablecoins and wrapped tokens  (done upfront, no API call)
#   • 24h USD volume computed individually from last 96 x 15m candles
#   • Discard if computed vol < MIN_24H_VOL_USDT
#
# STAGE 2 — STRUCTURAL SCREEN  (per-symbol, candle-based):
#   LONG : 4H close > 50 EMA   |  SHORT: 4H close < 50 EMA
#   Both : 1H ATR(14) < ATR_COMPRESS_PCT%
#   Both : 5-day range < RANGE_SKIP_PCT%
#
# STAGE 3 — ENTRY SIGNAL  (per-symbol, 15m candle-based):
#   LONG : 15m close > 20-bar high + VOL_SPIKE_MULT x vol + 1H bullish
#   SHORT: 15m close < 20-bar low  + VOL_SPIKE_MULT x vol + 1H bearish
#
# STAGE 4 — RANKING  (cycle-level):
#   Weighted score: vol spike (30) + move strength (20) + EMA proximity (20) + liquidity (30)
#   Top MAX_OPEN_TRADES get actual entries.
#
# TP/SL — ATR-BASED:
#   TP dist = max(ATR x ATR_TP_MULT,  entry x MIN_TP_PCT%)
#   SL dist = max(ATR x ATR_SL_MULT,  entry x MIN_SL_PCT%)   -> R:R = 2:1
# =============================================================================

# ── Trade params ──────────────────────────────────────────────────────────────
MAX_OPEN_TRADES   = 12

# ── ATR-based TP/SL ──────────────────────────────────────────────────────────
ATR_TP_MULT       = 3.0
ATR_SL_MULT       = 1.5
MIN_TP_PCT        = 4.0
MIN_SL_PCT        = 2.0

# ── Universe filter ───────────────────────────────────────────────────────────
MIN_24H_VOL_USDT  = 1_000_000

STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX","UST","LUSD",
    "FDUSD","PYUSD","USDD","USDN","GUSD","SUSD","CUSD","USDX","OUSD",
}
WRAPPED = {"WBTC","WETH","WBNB","WMATIC","WAVAX","WSOL","WFTM"}

# ── Strategy params ───────────────────────────────────────────────────────────
EMA200_DAILY_LEN  = 200
EMA50_4H_LEN      = 50
ATR_LEN           = 14
ATR_COMPRESS_PCT  = 2.5
BREAKOUT_BARS     = 20
VOL_SPIKE_MULT    = 1.5
HTF_VOL_BARS      = 2
RANGE_LOOKBACK    = 480
RANGE_SKIP_PCT    = 15

# ── Candle counts ─────────────────────────────────────────────────────────────
CANDLES_15M       = 550
CANDLES_4H        = 65
CANDLES_1H        = 30
CANDLES_1M        = 5

# ── Resolutions ───────────────────────────────────────────────────────────────
RESOLUTION_15M    = "15"
RESOLUTION_1M     = "1"
RESOLUTION_DAILY  = "1D"
RESOLUTION_1H     = "60"
RESOLUTION_4H     = "240"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1M  = 60
CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

SCAN_INTERVAL          = 300
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
                print(f"[TELEGRAM] Rate limited — waiting {retry_after}s")
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
    if len(values) < length:
        return None
    k   = 2 / (length + 1)
    ema = sum(values[:length]) / length
    for v in values[length:]:
        ema = v * k + ema * (1 - k)
    return ema


def compute_atr(candles, length):
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


def compute_24h_vol_usd(candles_15m):
    """
    Compute approximate 24h USD volume from the last 96 x 15m candles.
    volume (base asset per candle) x close price = USD value per candle.
    No separate API call needed — uses already-fetched 15m data.
    """
    if not candles_15m:
        return 0.0
    last_96 = candles_15m[-96:] if len(candles_15m) >= 96 else candles_15m
    return sum(float(c["volume"]) * float(c["close"]) for c in last_96)


def compute_atr_tp_sl(entry, candles_15m, direction, precision):
    """
    ATR-based TP/SL with % floors.
    TP = entry +/- max(ATR x ATR_TP_MULT,  entry x MIN_TP_PCT%)
    SL = entry -/+ max(ATR x ATR_SL_MULT,  entry x MIN_SL_PCT%)
    """
    atr = compute_atr(candles_15m[-50:], ATR_LEN) if len(candles_15m) >= ATR_LEN + 1 else None

    if atr and atr > 0:
        tp_dist = max(atr * ATR_TP_MULT, entry * MIN_TP_PCT / 100)
        sl_dist = max(atr * ATR_SL_MULT, entry * MIN_SL_PCT / 100)
    else:
        tp_dist = entry * MIN_TP_PCT / 100
        sl_dist = entry * MIN_SL_PCT / 100

    if direction == "long":
        return round(entry + tp_dist, precision), round(entry - sl_dist, precision)
    else:
        return round(entry - tp_dist, precision), round(entry + sl_dist, precision)


# =====================================================
# STAGE 1 — UNIVERSE FILTER
# =====================================================

def is_excluded(symbol):
    base = symbol.replace("USDT", "")
    return base in STABLECOINS or base in WRAPPED


def build_eligible_universe(all_symbols_rows):
    """
    Pre-filter: remove stablecoins and wrapped tokens only.
    Volume is checked individually inside check_and_trade from candle data
    so every discard is logged with the actual computed volume.
    """
    eligible    = []
    skip_stable = 0

    for symbol, row in all_symbols_rows:
        if is_excluded(symbol):
            skip_stable += 1
            print(f"  [{symbol}] DISCARDED — stablecoin or wrapped token")
            continue
        eligible.append((symbol, row))

    print(f"\n[UNIVERSE] {len(all_symbols_rows)} in sheet | "
          f"-{skip_stable} stables/wrapped | "
          f"-> {len(eligible)} proceeding to per-coin scan")
    return eligible


def get_btc_regime():
    """Returns (is_bull, btc_close, ema200)."""
    try:
        candles = fetch_candles("BTCUSDT", 220, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
        if len(candles) < EMA200_DAILY_LEN:
            print(f"[BTC REGIME] Only {len(candles)} daily candles — defaulting BULL")
            return True, 0.0, 0.0
        closes  = [float(c["close"]) for c in candles]
        ema200  = compute_ema(closes, EMA200_DAILY_LEN)
        is_bull = closes[-1] > ema200 if ema200 else True
        label   = "BULL -> LONG mode" if is_bull else "BEAR -> SHORT mode"
        print(f"[BTC REGIME] close={closes[-1]:,.2f}  EMA200={ema200:,.2f}  {label}")
        return is_bull, closes[-1], ema200 or 0.0
    except Exception as e:
        print(f"[BTC REGIME] Error: {e} — defaulting BULL")
        return True, 0.0, 0.0


# =====================================================
# STAGE 2 — STRUCTURAL SCREEN
# =====================================================

def check_4h_uptrend(candles_4h):
    if len(candles_4h) < EMA50_4H_LEN:
        return False, 0.0, 0.0
    closes = [float(c["close"]) for c in candles_4h]
    ema50  = compute_ema(closes, EMA50_4H_LEN)
    if ema50 is None:
        return False, 0.0, 0.0
    last = closes[-1]
    return last > ema50, round(last, 8), round(ema50, 8)


def check_4h_downtrend(candles_4h):
    if len(candles_4h) < EMA50_4H_LEN:
        return False, 0.0, 0.0
    closes = [float(c["close"]) for c in candles_4h]
    ema50  = compute_ema(closes, EMA50_4H_LEN)
    if ema50 is None:
        return False, 0.0, 0.0
    last = closes[-1]
    return last < ema50, round(last, 8), round(ema50, 8)


def check_1h_compression(candles_1h):
    atr = compute_atr(candles_1h, ATR_LEN)
    if atr is None:
        return False, 0.0, 0.0
    last_close = float(candles_1h[-1]["close"])
    atr_pct    = (atr / last_close) * 100 if last_close > 0 else 999.0
    return atr_pct < ATR_COMPRESS_PCT, round(atr_pct, 4), round(atr, 8)


def check_range_not_extended(candles_15m):
    if len(candles_15m) < RANGE_LOOKBACK:
        return False, 0.0
    window  = candles_15m[-RANGE_LOOKBACK:]
    lo      = min(float(c["low"])  for c in window)
    hi      = max(float(c["high"]) for c in window)
    rng_pct = round(((hi - lo) / lo) * 100, 2) if lo > 0 else 0.0
    return rng_pct >= RANGE_SKIP_PCT, rng_pct


# =====================================================
# STAGE 3 — ENTRY SIGNAL (LONG)
# =====================================================

def check_15m_breakout(candles_15m):
    needed = BREAKOUT_BARS + 1
    if len(candles_15m) < needed:
        return False, 0.0, 0.0, 0.0
    curr       = candles_15m[-1]
    prev_bars  = candles_15m[-(BREAKOUT_BARS + 1):-1]
    curr_close = float(curr["close"])
    curr_vol   = float(curr["volume"])
    prev_high  = max(float(c["close"]) for c in prev_bars)
    avg_vol    = sum(float(c["volume"]) for c in prev_bars) / len(prev_bars) if prev_bars else 0
    vol_ratio  = curr_vol / avg_vol if avg_vol > 0 else 0.0
    return (
        (curr_close > prev_high and vol_ratio >= VOL_SPIKE_MULT),
        round(curr_close, 8), round(prev_high, 8), round(vol_ratio, 2),
    )


def check_1h_bullish_confirmation(candles_1h):
    needed = HTF_VOL_BARS * 2
    if len(candles_1h) < needed:
        return False, 0.0, 0.0, False
    recent_vols = [float(c["volume"]) for c in candles_1h[-HTF_VOL_BARS:]]
    prev_vols   = [float(c["volume"]) for c in candles_1h[-(HTF_VOL_BARS * 2):-HTF_VOL_BARS]]
    avg_recent  = sum(recent_vols) / len(recent_vols)
    avg_prev    = sum(prev_vols)   / len(prev_vols)
    last        = candles_1h[-1]
    is_bullish  = float(last["close"]) > float(last["open"])
    return (avg_recent > avg_prev and is_bullish), round(avg_recent, 2), round(avg_prev, 2), is_bullish


# =====================================================
# STAGE 3 — ENTRY SIGNAL (SHORT)
# =====================================================

def check_15m_breakdown(candles_15m):
    needed = BREAKOUT_BARS + 1
    if len(candles_15m) < needed:
        return False, 0.0, 0.0, 0.0
    curr       = candles_15m[-1]
    prev_bars  = candles_15m[-(BREAKOUT_BARS + 1):-1]
    curr_close = float(curr["close"])
    curr_vol   = float(curr["volume"])
    prev_low   = min(float(c["close"]) for c in prev_bars)
    avg_vol    = sum(float(c["volume"]) for c in prev_bars) / len(prev_bars) if prev_bars else 0
    vol_ratio  = curr_vol / avg_vol if avg_vol > 0 else 0.0
    return (
        (curr_close < prev_low and vol_ratio >= VOL_SPIKE_MULT),
        round(curr_close, 8), round(prev_low, 8), round(vol_ratio, 2),
    )


def check_1h_bearish_confirmation(candles_1h):
    needed = HTF_VOL_BARS * 2
    if len(candles_1h) < needed:
        return False, 0.0, 0.0, False
    recent_vols = [float(c["volume"]) for c in candles_1h[-HTF_VOL_BARS:]]
    prev_vols   = [float(c["volume"]) for c in candles_1h[-(HTF_VOL_BARS * 2):-HTF_VOL_BARS]]
    avg_recent  = sum(recent_vols) / len(recent_vols)
    avg_prev    = sum(prev_vols)   / len(prev_vols)
    last        = candles_1h[-1]
    is_bearish  = float(last["close"]) < float(last["open"])
    return (avg_recent > avg_prev and is_bearish), round(avg_recent, 2), round(avg_prev, 2), is_bearish


# =====================================================
# STAGE 4 — SCORING
# =====================================================

def score_candidate(vol_ratio, move_strength_pct, ema_proximity_pct, vol_24h_usd):
    s1 = min(vol_ratio / 5.0,              1.0) * 30
    s2 = min(move_strength_pct / 5.0,      1.0) * 20
    s3 = max(0, 1 - ema_proximity_pct / 10)    * 20
    s4 = min(vol_24h_usd / 50_000_000,     1.0) * 30
    return round(s1 + s2 + s3 + s4, 4)


# =====================================================
# CANDLE FETCHERS
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
# PLACE LONG ORDER
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision):
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
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
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None

    send_telegram(
        f"🟢 <b>NEW LONG (BREAKOUT) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True, entry, tp


# =====================================================
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision):
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
    tp_pct = round(((entry - tp) / entry) * 100, 2)
    sl_pct = round(((sl - entry) / entry) * 100, 2)
    print(f"  [SHORT] Entry={entry}  TP={tp}(-{tp_pct}%)  SL={sl}(+{sl_pct}%)  Qty={qty}")

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
        return False, None, None

    print(f"  [API] {symbol}: {result}")
    if "order" not in result and not isinstance(result, list):
        send_telegram(f"❌ <b>SHORT REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None

    send_telegram(
        f"🔴 <b>NEW SHORT (BREAKDOWN) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (-{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (+{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True, entry, tp


# =====================================================
# EXECUTE ENTRY
# =====================================================

def execute_entry(cand, all_state):
    symbol      = cand["symbol"]
    row         = cand["row"]
    entry_price = cand["entry_price"]
    tp_price    = cand["tp_price"]
    sl_price    = cand["sl_price"]
    precision   = cand["precision"]
    curr_ts     = cand["curr_ts"]
    direction   = cand["direction"]

    st = all_state.setdefault(symbol, init_symbol_state())

    if direction == "long":
        placed, confirmed_entry, confirmed_tp = place_long_order(
            symbol, entry_price, tp_price, sl_price, precision)
    else:
        placed, confirmed_entry, confirmed_tp = place_short_order(
            symbol, entry_price, tp_price, sl_price, precision)

    if placed:
        st["in_position"]   = True
        st["direction"]     = direction
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

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders,
                    signal_direction="long"):
    """
    Runs full state management (TP hit, reconciliation, day reset, dedup).
    Runs direction-appropriate strategy checks.
    Returns candidate dict if entry qualifies, else None.
    Volume is fetched individually from 15m candles — every discard is logged.
    """
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch 15m candles ──────────────────────────────────────────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]

    min_15m = RANGE_LOOKBACK + BREAKOUT_BARS + 5
    if len(candles_15m) < min_15m:
        print(f"  [{symbol}] DISCARDED — not enough history "
              f"({len(candles_15m)} candles, need {min_15m})")
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
        print(f"  [{symbol}] SKIP — TP already completed")
        if st.get("in_position"):
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
        return None

    # ── 5. Resolve TP target + check if already hit ───────────────────────────
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
        existing_dir = st.get("direction") or "long"
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit     = False
        hit_kind   = None
        hit_price  = None

        if existing_dir == "long":
            tp_threshold = tp_stored * 0.9999
            if last_close and last_close >= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rh = get_recent_high(symbol)
                if rh and rh >= tp_threshold:
                    tp_hit, hit_kind, hit_price = True, "wick", rh
        else:
            tp_threshold = tp_stored * 1.0001
            if last_close and last_close <= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rl = get_recent_low(symbol)
                if rl and rl <= tp_threshold:
                    tp_hit, hit_kind, hit_price = True, "wick", rl

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
        return None

    if st.get("in_position"):
        print(f"  [{symbol}] POSITION CLOSED — resetting state")
        prev_last = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last
        st = all_state[symbol]
        save_state(all_state)

    has_order = any(o.get("pair") == pair_name for o in global_orders)
    if has_order:
        print(f"  [{symbol}] SKIP — open order already on book")
        return None

    # ── 7. Candle dedup guard ─────────────────────────────────────────────────
    curr    = candles_15m[-1]
    curr_ts = int(curr["time"])
    curr_c  = float(curr["close"])

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — same 15m candle already processed this cycle")
        save_state(all_state)
        return None

    # ── 7b. STAGE 1 — Volume check (individual, computed from candles) ────────
    # Each coin's volume is computed here from its own 15m candle data.
    # No batch ticker call — no key mismatch, no silent $0 volume errors.
    vol_24h_usd = compute_24h_vol_usd(candles_15m)
    if vol_24h_usd < MIN_24H_VOL_USDT:
        print(f"  [{symbol}] DISCARDED — 24h vol ${vol_24h_usd:,.0f} "
              f"< threshold ${MIN_24H_VOL_USDT:,.0f}")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] Volume PASS — 24h vol ${vol_24h_usd:,.0f}")

    # ── 8. Fetch 4H and 1H candles ───────────────────────────────────────────
    candles_4h = fetch_candles(symbol, CANDLES_4H, RESOLUTION_4H, CANDLE_SECONDS_4H)
    candles_1h = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)

    if candles_4h and (now_ms - int(candles_4h[-1]["time"])) < CANDLE_SECONDS_4H * 1000:
        candles_4h = candles_4h[:-1]
    if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
        candles_1h = candles_1h[:-1]

    # ── 9. STAGE 2 — Structural screen ───────────────────────────────────────
    if signal_direction == "long":
        trend_ok, close_4h, ema50_4h = check_4h_uptrend(candles_4h)
    else:
        trend_ok, close_4h, ema50_4h = check_4h_downtrend(candles_4h)

    if not trend_ok:
        print(f"  [{symbol}] DISCARDED — 4H trend fail ({signal_direction.upper()}) "
              f"close={close_4h} ema50={ema50_4h}")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] 4H trend PASS ({signal_direction.upper()}) "
          f"close={close_4h} ema50={ema50_4h}")

    compress_ok, atr_pct, _ = check_1h_compression(candles_1h)
    if not compress_ok:
        print(f"  [{symbol}] DISCARDED — no compression "
              f"(1H ATR={atr_pct}% >= threshold {ATR_COMPRESS_PCT}%)")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] Compression PASS — 1H ATR={atr_pct}%")

    extended, rng_pct = check_range_not_extended(candles_15m)
    if extended:
        print(f"  [{symbol}] DISCARDED — already extended "
              f"(5-day range={rng_pct}% >= {RANGE_SKIP_PCT}%)")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] Range PASS — 5-day range={rng_pct}%")

    # ── 10. STAGE 3 — Entry signal ────────────────────────────────────────────
    if signal_direction == "long":
        sig_ok, sig_close, prev_ref, vol_ratio = check_15m_breakout(candles_15m)
        htf_ok, htf_r, htf_p, htf_conf        = check_1h_bullish_confirmation(candles_1h)
        signal_label = "BREAKOUT"
    else:
        sig_ok, sig_close, prev_ref, vol_ratio = check_15m_breakdown(candles_15m)
        htf_ok, htf_r, htf_p, htf_conf        = check_1h_bearish_confirmation(candles_1h)
        signal_label = "BREAKDOWN"

    if not sig_ok:
        print(f"  [{symbol}] DISCARDED — no 15m {signal_label} "
              f"(close={sig_close} vs ref={prev_ref}, "
              f"vol_ratio={vol_ratio}x needs >={VOL_SPIKE_MULT}x)")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] 15m {signal_label} PASS "
          f"close={sig_close} ref={prev_ref} vol_ratio={vol_ratio}x")

    if not htf_ok:
        print(f"  [{symbol}] DISCARDED — 1H confirm fail "
              f"(directional={htf_conf}, "
              f"vol_recent={htf_r} vs vol_prev={htf_p})")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] 1H confirm PASS "
          f"directional={htf_conf} vol_recent={htf_r} vol_prev={htf_p}")

    st["last_candle_ts"] = curr_ts

    # ── 11. ATR-based TP/SL ───────────────────────────────────────────────────
    entry_price        = round(curr_c, precision)
    tp_price, sl_price = compute_atr_tp_sl(entry_price, candles_15m, signal_direction, precision)
    tp_pct_disp = round(abs((tp_price - entry_price) / entry_price * 100), 2)
    sl_pct_disp = round(abs((sl_price - entry_price) / entry_price * 100), 2)

    # ── 12. STAGE 4 — Score ───────────────────────────────────────────────────
    if signal_direction == "long":
        move_pct     = ((sig_close - prev_ref) / prev_ref * 100) if prev_ref > 0 else 0.0
        ema_prox_pct = ((sig_close / ema50_4h - 1) * 100)        if ema50_4h > 0 else 0.0
    else:
        move_pct     = ((prev_ref - sig_close) / prev_ref * 100) if prev_ref > 0 else 0.0
        ema_prox_pct = ((1 - sig_close / ema50_4h) * 100)        if ema50_4h > 0 else 0.0

    candidate_score = score_candidate(vol_ratio, move_pct, ema_prox_pct, vol_24h_usd)

    print(f"  [{symbol}] ✅ CANDIDATE ({signal_direction.upper()})  "
          f"score={candidate_score}  entry={entry_price}  "
          f"tp={tp_price}({'+' if signal_direction == 'long' else '-'}{tp_pct_disp}%)  "
          f"sl={sl_price}({'-' if signal_direction == 'long' else '+'}{sl_pct_disp}%)  "
          f"vol_ratio={vol_ratio}x  move={round(move_pct, 2)}%")

    return {
        "symbol":       symbol,
        "row":          row,
        "direction":    signal_direction,
        "score":        candidate_score,
        "entry_price":  entry_price,
        "tp_price":     tp_price,
        "sl_price":     sl_price,
        "precision":    precision,
        "curr_ts":      curr_ts,
        "vol_ratio":    vol_ratio,
        "move_pct":     round(move_pct, 4),
        "ema_prox_pct": round(ema_prox_pct, 4),
        "vol_24h_usd":  round(vol_24h_usd, 0),
    }


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Momentum Bot Started — Long + Short</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy : <code>Multi-Stage Breakout/Breakdown (All Instruments)</code>\n"
    f"\n"
    f"🔍 Stage 1 — Volume (per-coin from candles):\n"
    f"  <code>• Exclude stables/wrapped upfront</code>\n"
    f"  <code>• 24h vol computed from 96x15m candles per coin</code>\n"
    f"  <code>• Discard if &lt; ${MIN_24H_VOL_USDT:,} USD</code>\n"
    f"\n"
    f"🔍 Stage 2 — Structure:\n"
    f"  <code>• LONG : 4H close &gt; 50 EMA</code>\n"
    f"  <code>• SHORT: 4H close &lt; 50 EMA</code>\n"
    f"  <code>• Both : 1H ATR(14) &lt; {ATR_COMPRESS_PCT}% + 5-day range &lt; {RANGE_SKIP_PCT}%</code>\n"
    f"\n"
    f"🔍 Stage 3 — Signal:\n"
    f"  <code>• LONG : 15m close &gt; {BREAKOUT_BARS}-bar high + {VOL_SPIKE_MULT}x vol + 1H bullish</code>\n"
    f"  <code>• SHORT: 15m close &lt; {BREAKOUT_BARS}-bar low  + {VOL_SPIKE_MULT}x vol + 1H bearish</code>\n"
    f"\n"
    f"📊 Stage 4 : <code>Top {MAX_OPEN_TRADES} by score</code>\n"
    f"🌍 Regime  : <code>BTC &gt; EMA200 daily -> LONG | BTC &lt; EMA200 -> SHORT</code>\n"
    f"💹 TP/SL   : <code>ATR x {ATR_TP_MULT} / ATR x {ATR_SL_MULT} "
    f"(floor {MIN_TP_PCT}% / {MIN_SL_PCT}%)</code>\n"
    f"🔁 Scan    : <code>Every {SCAN_INTERVAL}s</code>  |  "
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

        # ── Build full symbol list ────────────────────────────────────────────
        all_symbols_rows = []
        row_index        = {}
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if symbol:
                all_symbols_rows.append((symbol, row))
                row_index[symbol] = row

        # ── BTC regime -> direction ───────────────────────────────────────────
        btc_bull, btc_close, btc_ema200 = get_btc_regime()
        signal_direction = "long" if btc_bull else "short"

        # ── STAGE 1 pre-filter: remove stables/wrapped ───────────────────────
        # Volume is checked per-coin inside check_and_trade from candle data.
        eligible = build_eligible_universe(all_symbols_rows)

        # Force-include symbols with active positions for TP monitoring.
        # They hit early-return branches before the volume check, so they
        # are never blocked by the volume gate even if volume dropped.
        eligible_set = {s for s, _ in eligible}
        for sym, sym_st in state.items():
            if (sym_st.get("in_position") or sym_st.get("tp_level")) and sym not in eligible_set:
                r = row_index.get(sym)
                if r is not None:
                    eligible.append((sym, r))
                    eligible_set.add(sym)
                    print(f"[FORCE-INCLUDE] {sym} — active position/TP, monitoring only")

        # ── Slot calculation ─────────────────────────────────────────────────
        active_count    = len(global_positions)
        slots_available = max(0, MAX_OPEN_TRADES - active_count)
        print(f"[SLOTS] {active_count} open / {MAX_OPEN_TRADES} max -> {slots_available} slot(s)")
        print(f"[MODE] {signal_direction.upper()} — scanning {len(eligible)} symbols\n")

        # ── Per-coin scan: stages 2-3 + scoring ──────────────────────────────
        candidates = []

        for symbol, row in eligible:
            print(f"--- {symbol} ---")
            try:
                cand = check_and_trade(
                    symbol, row, df, state,
                    global_positions, global_orders,
                    signal_direction,
                )
                if cand:
                    candidates.append(cand)
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")
                continue

        # ── STAGE 4: Rank and execute top N ──────────────────────────────────
        candidates.sort(key=lambda x: x["score"], reverse=True)

        print(f"\n[RANKING] {len(candidates)} candidate(s) | "
              f"{slots_available} slot(s) | {signal_direction.upper()} mode")
        for i, c in enumerate(candidates):
            tag = f"EXECUTE #{i + 1}" if i < slots_available else "SKIP (no slot)"
            print(f"  [{tag}] {c['symbol']} ({c['direction'].upper()})  "
                  f"score={c['score']}  vol_ratio={c['vol_ratio']}x  "
                  f"move={c['move_pct']}%  24h_vol=${c['vol_24h_usd']:,.0f}")

        for cand in candidates[:slots_available]:
            try:
                execute_entry(cand, state)
            except Exception as e:
                print(f"  [{cand['symbol']}] ENTRY ERROR: {e}")

        if candidates:
            executed = candidates[:slots_available]
            skipped  = candidates[slots_available:]
            emoji    = "🟢" if signal_direction == "long" else "🔴"
            msg = (
                f"📊 <b>Cycle {cycle} — {signal_direction.upper()} Mode</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔍 Scanned  : <code>{len(eligible)}</code>\n"
                f"✅ Qualified: <code>{len(candidates)}</code>\n"
                f"{emoji} Executed : <code>{len(executed)}</code>\n"
            )
            for c in executed:
                msg += f"  • {c['symbol']}  score={c['score']}  entry={c['entry_price']}\n"
            if skipped:
                msg += f"⏭ Skipped : <code>{', '.join(c['symbol'] for c in skipped)}</code>"
            send_telegram(msg)

        print(f"\n===== CYCLE {cycle} DONE =====")
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