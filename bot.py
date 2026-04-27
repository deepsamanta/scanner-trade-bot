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
# STRATEGY PARAMETERS  (SHORT bot — mirror of the 200 EMA Dual-Path Long bot)
#
# TIMEFRAME ARCHITECTURE:
#   - Primary analysis candles : 4h (EMA, slope, pump, crossdown, crossover)
#   - Entry confirmation       : 30m (first 30m close that satisfies trigger)
#   - Path C pivot confluence  : 4h + 12h (synthetic from 4h) + 1D
#   - Scan interval            : 30 minutes
# =============================================================================

# ─── CORE ─────────────────────────────────────────────────────────────────────
EMA_PERIOD           = 200
LOOKBACK             = 200      # candles to count ABOVE EMA (mirror: was "below" for long)
ABOVE_PCT_MIN        = 70.0     # min % of last LOOKBACK candles ABOVE EMA
TP_PCT               = 5        # Take Profit % (fixed BELOW entry for shorts)
SL_ABOVE_EMA_PCT     = 5.0      # Paths A/B SL: EMA × (1 + this/100)

# ─── PATH A: REJECTION RETEST (mirror of long reversal retest) ───────────────
MAX_RETEST_BARS      = 20       # max 4h bars to wait for retest after arming
PROXIMITY_PCT        = 0.3      # retest zone = EMA × (1 - this/100)  (from BELOW for shorts)

# ─── SLOPE FILTER ────────────────────────────────────────────────────────────
# MIRROR: long required slope ≥ -0.2% (rising or flat-to-slightly-falling).
# Short requires slope ≤ +0.2% (falling or flat-to-slightly-rising).
USE_SLOPE_FILTER     = True
SLOPE_BARS           = 10
MAX_EMA_SLOPE_PCT    = 0.2      # max EMA slope % to qualify for short (mirror of -0.2% min for long)

# ─── VOLUME FILTER ───────────────────────────────────────────────────────────
USE_VOLUME_FILTER    = True
VOL_LOOKBACK         = 20
VOL_MULTIPLIER       = 1.0      # rejection path (Path A)
BREAKDOWN_VOL_MULT   = 1.3      # breakdown path (Path B) — mirror of BREAKOUT_VOL_MULT

# ─── PATH B: MOMENTUM BREAKDOWN ──────────────────────────────────────────────
USE_BREAKDOWN_PATH   = True
MOMENTUM_LOOKBACK    = 5        # close < close[N] bars ago (mirror: long was >)

# ─── CROSSDOWN LOOKBACK (rescue filter) ──────────────────────────────────────
CROSS_LOOKBACK        = 5        # accept crossdown from last N 4h bars (incl. current)
MAX_EMA_DISTANCE_PCT  = 2.0      # don't arm if price is already >X% BELOW EMA (anti-chase)

# ─── CROSSUP PUMP FILTER (Paths A/B) ─────────────────────────────────────────
# Mirror of long's "crossdown drop filter". For shorts we measure peak pump
# magnitude using the MIN-EMA across all crossups in the PUMP_LOOKBACK window.
# ≥10% pump required.
USE_PUMP_FILTER      = True
PUMP_LOOKBACK        = 200       # matches LOOKBACK (same 4h window used throughout)
MIN_PUMP_PCT         = 10.0      # min pump % from lower hinge to highest high

# ─── PATH C: RESISTANCE REJECTION (multi-timeframe pivot confluence) ────────
# Mirror of long's Path C (support bounce), but looking at pivot highs ABOVE
# current price instead of pivot lows below.
#
# NOTE on 12h: CoinDCX's futures candlestick endpoint doesn't document a
# native 12h / 720-minute resolution. To avoid silent API failures, we
# construct 12h candles synthetically from 4h data (every 3 × 4h = 12h).
USE_PATH_C                = True
PATH_C_ENABLED_TIMEFRAMES = ["240", "12H_synth", "1D"]   # 4h + synthetic 12h + 1D
PATH_C_CANDLES            = 600           # target bars per TF
PATH_C_MIN_PUMP_PCT       = 5.0           # min pump from MOST RECENT crossup (4h-based)
PIVOT_STRENGTH            = 3             # N bars on each side for pivot detection
PIVOT_ZONE_PCT            = 1.0           # ±% band for clustering pivots
MIN_TF_CONFLUENCE         = 2             # min TFs defending a zone (2 of 3)
PATH_C_MAX_WAIT_BARS      = 30            # max 30m bars to wait for rejection
PATH_C_TOUCH_TOLERANCE_PCT = 0.5          # price must come within this % of zone to count as "tested"
PATH_C_SL_ABOVE_ZONE_PCT  = 1.5           # Path C SL: zone_high × (1 + this/100)

# ─── PATH C: ADDITIONAL EMA-POSITION ARM TRIGGERS ───────────────────────
# Path C also arms when price sits in one of these two zones vs. 200 EMA (4h):
PATH_C_BELOW_EMA_PROXIMITY_PCT = 4.0    # arm if  -4%  <= (close-EMA)/EMA < 0
PATH_C_ABOVE_EMA_EXTENDED_PCT  = 10.0   # arm if  (close-EMA)/EMA >= +10%

# ─── SAFETY (reward/risk floor) ──────────────────────────────────────────────
MIN_RR               = 1.5          # Skip trade if TP/SL reward:risk falls below this

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY   = "240"    # CoinDCX 4-hour candles
RESOLUTION_ENTRY     = "30"     # CoinDCX 30-minute candles for entry confirmation
CANDLE_SECONDS       = 4 * 3600 # primary candle length (4h)
ENTRY_CANDLE_SECONDS = 30 * 60  # entry candle length (30m)
SCAN_INTERVAL        = 1800     # 30 minutes

# ─── REQUEST TIMEOUTS (seconds) ──────────────────────────────────────────────
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10

# ─── GSPREAD RE-AUTH INTERVAL ────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60

# ─── LOCAL STATE FILE (wait-for-retest persistence) ──────────────────────────
STATE_FILE           = "short_bot_state.json"
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
# LOCAL STATE PERSISTENCE (waiting_retest across scans)
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
        # ── Path A state ─────────────────────────────────────────────────
        "waiting_retest":        False,
        "wait_start_candle_ts":  None,    # ms timestamp of the candle when armed
        # ── Path C state (resistance rejection) ──────────────────────────
        "path_c_armed":          False,
        "path_c_start_ts":       None,    # ms timestamp when zone was armed
        "path_c_zone_low":       None,    # lower edge of resistance zone
        "path_c_zone_high":      None,    # upper edge of resistance zone
        "path_c_zone_center":    None,    # midpoint (for entry comparisons)
        "path_c_zone_touched":   False,   # has price popped into the zone yet?
        "path_c_tf_count":       None,    # how many TFs defended this zone (log/telegram only)
        # ── Common position state ────────────────────────────────────────
        "in_position":           False,
        "entry_path":            None,    # "retest", "breakdown", or "resistance_rejection"
        "entry_price":           None,
        "tp_level":              None,
        "sl_price":              None,
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
    """
    Returns EMA series aligned so ema_values[-1] pairs with closes[-1].
    Left-padded with None for indices where EMA is not yet defined.
    """
    if len(closes) < period:
        return [None] * len(closes)
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    pad = [None] * (len(closes) - len(values))
    return pad + values


def had_recent_crossdown(closes, emas, lookback):
    """
    Returns (found, bars_ago) where found=True if a close crossed BELOW EMA
    at any point within the last `lookback` bars (inclusive of current bar).
    bars_ago = 0 means cross on current bar, 1 means previous bar, etc.

    A cross is defined as: closes[i-1] >= emas[i-1] AND closes[i] < emas[i]

    Used by the rescue filter so a crossdown that happened a few bars ago
    isn't forgotten just because slope/volume failed on that exact bar.
    """
    n = len(closes)
    if n < 2 or lookback < 1:
        return False, None

    for k in range(lookback):
        i_now  = n - 1 - k
        i_prev = i_now - 1
        if i_prev < 0:
            break
        c_now,  c_prev = closes[i_now], closes[i_prev]
        e_now,  e_prev = emas[i_now],   emas[i_prev]
        if e_now is None or e_prev is None:
            continue
        if c_prev >= e_prev and c_now < e_now:
            return True, k
    return False, None


# =====================================================
# CANDLE FETCH (primary: 4h; entry confirmation: 30m)
# =====================================================

def fetch_candles(symbol, num_candles_needed, resolution_str=None, candle_seconds=None):
    """
    Primary candle fetch. Defaults to RESOLUTION_PRIMARY (4h) but accepts
    overrides for the 30m entry-confirmation fetch.
    """
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
        candles  = sorted(data, key=lambda x: x["time"])
        return candles
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} fetch error: {e}")
        return []


def fetch_candles_tf(symbol, resolution_str, num_candles_needed):
    """
    Multi-timeframe candle fetch supporting both native CoinDCX resolutions
    and synthetic aggregated resolutions.

    Supported native: "1", "5", "15", "30", "60", "240", "1D"
    Supported synthetic: "12H_synth" (built from 3 × 4h candles)
    """
    res_to_seconds = {
        "1":    60,
        "5":    5 * 60,
        "15":   15 * 60,
        "30":   30 * 60,
        "60":   60 * 60,
        "240":  4  * 60 * 60,
        "1D":   24 * 60 * 60,
    }

    # ── Synthetic 12h: build from 3 × 4h candles ──────────────────────────
    if resolution_str == "12H_synth":
        needed_4h = num_candles_needed * 3 + 10
        candles_4h = fetch_candles_tf(symbol, "240", needed_4h)
        if len(candles_4h) < 3:
            return []

        synthetic = []
        for i in range(0, len(candles_4h) - 2, 3):
            group = candles_4h[i:i + 3]
            if len(group) != 3:
                continue
            synthetic.append({
                "time":   int(group[0]["time"]),
                "open":   float(group[0]["open"]),
                "high":   max(float(c["high"])   for c in group),
                "low":    min(float(c["low"])    for c in group),
                "close":  float(group[-1]["close"]),
                "volume": sum(float(c.get("volume", 0)) for c in group),
            })
        if len(synthetic) > num_candles_needed:
            synthetic = synthetic[-num_candles_needed:]
        return synthetic

    # ── Native resolutions ────────────────────────────────────────────────
    seconds_per_candle = res_to_seconds.get(resolution_str, CANDLE_SECONDS)

    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    fetch_seconds = (num_candles_needed + 50) * seconds_per_candle

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
        candles  = sorted(data, key=lambda x: x["time"])
        return candles
    except Exception as e:
        print(f"[CANDLES-TF {resolution_str}] {symbol} fetch error: {e}")
        return []


# =====================================================
# PIVOT DETECTION (Path C)
# =====================================================

def find_pivots(highs, lows, strength):
    """
    Returns a list of pivot prices found in the given candle series.
    Both pivot highs and pivot lows are "reactive levels" where price
    changed direction. Returns a flat list of price values.
    """
    pivot_prices = []
    n = len(highs)
    if n < 2 * strength + 1:
        return pivot_prices

    for i in range(strength, n - strength):
        h_center = highs[i]
        l_center = lows[i]

        # Pivot high check
        is_pivot_high = True
        for k in range(1, strength + 1):
            if highs[i - k] >= h_center or highs[i + k] >= h_center:
                is_pivot_high = False
                break
        if is_pivot_high:
            pivot_prices.append(h_center)
            continue

        # Pivot low check
        is_pivot_low = True
        for k in range(1, strength + 1):
            if lows[i - k] <= l_center or lows[i + k] <= l_center:
                is_pivot_low = False
                break
        if is_pivot_low:
            pivot_prices.append(l_center)

    return pivot_prices


def cluster_pivots_to_zones(pivots_by_tf, proximity_pct):
    """
    Clusters pivots across all timeframes into zones.
    """
    flat = []
    for tf, prices in pivots_by_tf.items():
        for p in prices:
            if p > 0:
                flat.append((p, tf))
    if not flat:
        return []

    flat.sort(key=lambda t: t[0])

    zones = []
    current_pivots = [flat[0]]
    current_center = flat[0][0]

    for price, tf in flat[1:]:
        gap_pct = abs(price - current_center) / current_center * 100.0
        if gap_pct <= proximity_pct:
            current_pivots.append((price, tf))
            current_center = sum(p for p, _ in current_pivots) / len(current_pivots)
        else:
            zones.append(_finalize_zone(current_pivots))
            current_pivots = [(price, tf)]
            current_center = price

    zones.append(_finalize_zone(current_pivots))
    return zones


def _finalize_zone(pivots):
    """Helper: build a zone dict from a list of (price, tf) tuples."""
    prices = [p for p, _ in pivots]
    tfs    = {tf for _, tf in pivots}
    return {
        "center": sum(prices) / len(prices),
        "low":    min(prices),
        "high":   max(prices),
        "tfs":    tfs,
        "pivots": pivots,
    }


def find_nearest_resistance_zone_above(symbol, current_price):
    """
    Path C resistance-zone detection pipeline (mirror of long's support zone).

    1. Fetch PATH_C_CANDLES bars on each timeframe in PATH_C_ENABLED_TIMEFRAMES
    2. Compute pivots on each TF (both pivot highs and pivot lows)
    3. Cluster all pivots across TFs into confluence zones (±PIVOT_ZONE_PCT)
    4. Keep only zones defended by ≥MIN_TF_CONFLUENCE timeframes
    5. Filter to zones strictly ABOVE current_price
    6. Return the NEAREST one (lowest zone among those above current price)
    """
    pivots_by_tf = {}
    for tf in PATH_C_ENABLED_TIMEFRAMES:
        candles = fetch_candles_tf(symbol, tf, PATH_C_CANDLES)
        if len(candles) < 2 * PIVOT_STRENGTH + 1:
            print(f"[PATH-C] {symbol} TF {tf} — insufficient candles ({len(candles)}), skipping this TF")
            continue
        tf_highs = [float(c["high"]) for c in candles]
        tf_lows  = [float(c["low"])  for c in candles]
        pivots   = find_pivots(tf_highs, tf_lows, PIVOT_STRENGTH)
        pivots_by_tf[tf] = pivots

    if not pivots_by_tf:
        return None

    zones = cluster_pivots_to_zones(pivots_by_tf, PIVOT_ZONE_PCT)

    # Keep only confluence zones with enough TF support
    strong_zones = [z for z in zones if len(z["tfs"]) >= MIN_TF_CONFLUENCE]
    if not strong_zones:
        return None

    # Filter to zones strictly above current price — zone's lower edge must
    # be above current price so price has room to rise INTO the zone.
    above_zones = [z for z in strong_zones if z["low"] > current_price]
    if not above_zones:
        return None

    # Pick the one with the lowest center (nearest to current price from above)
    nearest = min(above_zones, key=lambda z: z["center"])
    return nearest


# =====================================================
# 30-MINUTE ENTRY CONFIRMATION HELPERS
# =====================================================
# Architecture: Paths A/B/C all QUALIFY on 4h candles but TRIGGER on 30m.

def _fetch_closed_30m_candles(symbol, num_candles=6):
    """
    Fetch the last N closed 30m candles. Drops the in-progress bar if any.
    """
    candles = fetch_candles(symbol, num_candles + 2, RESOLUTION_ENTRY, ENTRY_CANDLE_SECONDS)
    if not candles:
        return []
    now_ms = int(time.time() * 1000)
    if len(candles) >= 1:
        last_ts_ms = int(candles[-1]["time"])
        elapsed_ms = now_ms - last_ts_ms
        if elapsed_ms < ENTRY_CANDLE_SECONDS * 1000:
            candles = candles[:-1]
    return candles


def confirm_30m_close_below(symbol, level):
    """
    Returns the first CLOSED 30m candle (most-recent) whose close is < level,
    or None if none in the lookback window. Use this for:
      - Path A retest trigger: close below EMA after a pop
      - Path B breakdown trigger: close below EMA
    """
    candles = _fetch_closed_30m_candles(symbol, 6)
    if not candles:
        return None
    last = candles[-1]
    if float(last["close"]) < level:
        return last
    return None


def confirm_30m_touch_and_close_below(symbol, zone_low, zone_high,
                                       touch_tolerance_pct=None):
    """
    Path C variant: requires the last 6 × 30m candles (3h) to have had at
    least one bar wick INTO the zone (high touched the zone band), followed
    by the current 30m closing BELOW zone_low. Returns confirming bar or None.
    """
    if touch_tolerance_pct is None:
        touch_tolerance_pct = PATH_C_TOUCH_TOLERANCE_PCT

    candles = _fetch_closed_30m_candles(symbol, 6)
    if not candles:
        return None

    last = candles[-1]
    last_close = float(last["close"])

    # Current 30m close must be strictly below zone_low
    if last_close >= zone_low:
        return None

    # At least one of the last few 30m bars must have wicked INTO the zone
    touch_ceiling = zone_high * (1 + touch_tolerance_pct / 100.0)
    touch_floor   = zone_low  * (1 - touch_tolerance_pct / 100.0)
    for c in candles:
        c_high = float(c["high"])
        if touch_floor <= c_high <= touch_ceiling:
            return last
    return None


# =====================================================
# RECENT LOW — wick-based TP detection (for shorts)
# =====================================================

def get_recent_low(symbol):
    """
    Fetches 1-min candles over the last SCAN_INTERVAL seconds to check if
    price wicked down to touch a stored TP between scans.
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
        candles  = response.json().get("data", [])
        if not candles:
            return None
        lows = [float(c["low"]) for c in candles]
        return min(lows)
    except Exception as e:
        print(f"[RECENT LOW] {symbol} error: {e}")
        return None


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
    """
    Returns True if there is an unfilled SELL entry order on the book for this
    pair (status: open or partially_filled).

    Per CoinDCX List Orders API:
      - timestamp: int (epoch ms)
      - status:    string, comma-separated  (NOT array)
      - side:      string, mandatory
      - page:      string  (NOT int)
      - size:      string  (NOT int)
      - margin_currency_short_name: array  (NOT string)
    """
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
            print(f"[has_open_order] {symbol} unexpected response (request rejected?): {orders}")
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
# PLACE SHORT ORDER — fixed TP + fixed SL attached to entry
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, entry_path):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)

    # Sanity: for a short, SL must be ABOVE entry, TP must be BELOW entry
    if sl <= entry:
        print(f"[SKIP] {symbol} [{entry_path}] SL {sl} not above entry {entry} — aborting")
        send_telegram(
            f"⚠️ <b>SHORT SKIPPED — {symbol} [{entry_path}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>SL {sl} not above entry {entry}</code>"
        )
        return False
    if tp >= entry:
        print(f"[SKIP] {symbol} [{entry_path}] TP {tp} not below entry {entry} — aborting")
        return False

    # Reward/Risk gate
    reward = entry - tp
    risk   = sl - entry
    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} [{entry_path}] RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>SHORT SIGNAL SKIPPED — {symbol} [{entry_path}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>\n"
            f"🛑 SL     : <code>{sl}</code>"
        )
        return False

    qty = compute_qty(entry_price, symbol)

    tp_pct_display = round(((entry - tp) / entry) * 100, 2) if entry else 0
    sl_pct_display = round(((sl - entry) / entry) * 100, 2) if entry else 0
    rr_display     = round(reward / risk, 2)

    print(
        f"[SHORT TRADE] {symbol} SELL ({entry_path}) | Entry {entry} | "
        f"TP {tp} (-{tp_pct_display}%) | SL {sl} (+{sl_pct_display}%) | RR {rr_display} | Qty {qty}"
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
            data=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
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
            f"🛤 Path    : <code>{entry_path}</code>\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>\n"
            f"🛑 SL      : <code>{sl}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return False

    send_telegram(
        f"🔴 <b>NEW SHORT ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (-{tp_pct_display}%)\n"
        f"🛑 SL      : <code>{sl}</code>  (+{sl_pct_display}%)\n"
        f"📊 RR      : <code>{rr_display}</code>\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    pair = fut_pair(symbol)

    # ─── Fetch enough 4h candles (primary TF) ────────────────────────────────
    candles_needed = EMA_PERIOD + LOOKBACK + 30
    candles = fetch_candles(symbol, candles_needed)

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    # ─── Use CLOSED candles only ─────────────────────────────────────────────
    if len(candles) >= 2:
        now_ms           = int(time.time() * 1000)
        last_candle_time = int(candles[-1]["time"])
        bar_elapsed_ms   = now_ms - last_candle_time
        if bar_elapsed_ms < CANDLE_SECONDS * 1000:
            candles = candles[:-1]
            print(f"[{symbol}] Dropping in-progress bar ({bar_elapsed_ms/1000:.0f}s elapsed of {CANDLE_SECONDS}s)")

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient closed candles ({len(candles)})")
        return

    precision = get_precision(candles[-1]["close"])
    closes    = [float(c["close"])  for c in candles]
    highs     = [float(c["high"])   for c in candles]
    lows      = [float(c["low"])    for c in candles]
    volumes   = [float(c.get("volume", 0)) for c in candles]

    last_close = closes[-1]
    last_high  = highs[-1]
    last_ts    = int(candles[-1]["time"])

    # ─── Indicators ──────────────────────────────────────────────────────────
    ema_values = compute_ema(closes, EMA_PERIOD)
    if (ema_values[-1] is None
            or ema_values[-1 - SLOPE_BARS] is None
            or ema_values[-1 - LOOKBACK] is None):
        print(f"[SKIP] {symbol} — EMA not ready deep enough")
        return

    ema_now    = ema_values[-1]
    ema_prev   = ema_values[-2]
    close_prev = closes[-2]

    # % ABOVE EMA over last LOOKBACK bars (mirror: long used "below")
    above_count = 0
    for i in range(LOOKBACK):
        c = closes[-1 - i]
        e = ema_values[-1 - i]
        if e is None:
            continue
        if c > e:
            above_count += 1
    above_pct_actual = (above_count / LOOKBACK) * 100.0
    trend_qualifies  = above_pct_actual >= ABOVE_PCT_MIN

    # EMA slope %
    ema_slope_ref = ema_values[-1 - SLOPE_BARS]
    ema_slope_pct = ((ema_now - ema_slope_ref) / ema_slope_ref * 100.0) if ema_slope_ref else 0
    # For shorts: slope must be ≤ MAX (EMA not rising too fast)
    slope_ok      = (not USE_SLOPE_FILTER) or (ema_slope_pct <= MAX_EMA_SLOPE_PCT)

    # Volume SMA + checks
    vol_window = volumes[-VOL_LOOKBACK:]
    vol_avg    = (sum(vol_window) / VOL_LOOKBACK) if len(vol_window) == VOL_LOOKBACK else 0
    last_vol   = volumes[-1]
    vol_ok           = (not USE_VOLUME_FILTER) or (vol_avg > 0 and last_vol > vol_avg * VOL_MULTIPLIER)
    breakdown_vol_ok = (vol_avg > 0) and (last_vol > vol_avg * BREAKDOWN_VOL_MULT)

    # ─── Crossup pump filter (mirror of long's crossdown drop filter) ────────
    # Measure how much the coin pumped — only short rejections with real damage.
    #
    # Step 1: Scan the last PUMP_LOOKBACK candles for ALL crossups
    #         (close went from ≤EMA → >EMA). There may be multiple.
    #
    # Step 2a — One or more crossups FOUND:
    #   Of all those crossup bars, pick the one where EMA value is LOWEST.
    #   This picks the "bottom of the rally" — in a real uptrend, the EMA at
    #   the first crossup is usually lowest.
    #       lower_hinge = min(EMA_at_each_crossup)
    #       upper       = HIGHEST HIGH across the entire PUMP_LOOKBACK window
    #
    # Step 2b — No crossups (price was already above EMA throughout):
    #       lower_hinge = LOWEST LOW across the window
    #       upper       = HIGHEST HIGH across the window
    #
    # Step 3: pump_pct = (upper - lower_hinge) / lower_hinge × 100
    #         pump_ok  = pump_pct ≥ MIN_PUMP_PCT
    n_candles      = len(closes)
    pump_start_idx = max(0, n_candles - PUMP_LOOKBACK)
    window_highs   = highs[pump_start_idx:]
    window_lows    = lows[pump_start_idx:]
    highest_in_window = max(window_highs) if window_highs else 0
    lowest_in_window  = min(window_lows)  if window_lows  else 0

    # Collect ALL crossup events and the EMA values at each
    crossup_emas = []   # list of (bar_index, ema_at_that_bar)
    for i in range(pump_start_idx + 1, n_candles):
        e_prev_i = ema_values[i - 1]
        e_now_i  = ema_values[i]
        if e_prev_i is None or e_now_i is None:
            continue
        if closes[i - 1] <= e_prev_i and closes[i] > e_now_i:
            crossup_emas.append((i, e_now_i))

    if crossup_emas:
        # Pick the crossup with the LOWEST EMA value as lower hinge
        best_xu_idx, lower_hinge = min(crossup_emas, key=lambda t: t[1])
        pump_anchor_type   = "crossup"
        crossup_count      = len(crossup_emas)
        chosen_crossup_i   = best_xu_idx
    else:
        # No crossup → price was already above EMA throughout the window
        lower_hinge        = lowest_in_window
        pump_anchor_type   = "lowest_low"
        crossup_count      = 0
        chosen_crossup_i   = None

    if lower_hinge is None or lower_hinge <= 0 or highest_in_window <= 0:
        pump_pct = 0.0
        pump_ok  = (not USE_PUMP_FILTER)
    else:
        pump_pct = (highest_in_window - lower_hinge) / lower_hinge * 100.0
        pump_ok  = (not USE_PUMP_FILTER) or (pump_pct >= MIN_PUMP_PCT)

    # ─── Path C pump variant (mirror of long's dropC) ───────────────────────
    # Paths A/B use `pump_pct` above (min-EMA across all crossups).
    # Path C uses the CURRENT leg up, from the most recent crossup to now.
    if crossup_emas:
        recent_xu_idx, recent_xu_ema = crossup_emas[-1]
        highs_since_recent_xu = highs[recent_xu_idx:]
        highest_since_recent_xu = max(highs_since_recent_xu) if highs_since_recent_xu else 0

        if recent_xu_ema and recent_xu_ema > 0 and highest_since_recent_xu > 0:
            pump_pct_path_c = (highest_since_recent_xu - recent_xu_ema) / recent_xu_ema * 100.0
            path_c_pump_anchor = f"recent_crossup@{recent_xu_idx}"
        else:
            pump_pct_path_c = 0.0
            path_c_pump_anchor = "invalid"
    else:
        pump_pct_path_c = pump_pct
        path_c_pump_anchor = f"fallback_{pump_anchor_type}"

    # Momentum (close < close N bars ago) — Path B only
    price_falling = closes[-1] < closes[-1 - MOMENTUM_LOOKBACK]

    # ─── Crossdown detection (mirror of long's crossover) ────────────────────
    # STRICT: cross on the current bar only.
    cross_down_strict = (close_prev >= ema_prev) and (last_close < ema_now)

    # RESCUE: a cross within the last CROSS_LOOKBACK bars.
    cross_down_recent, cross_bars_ago = had_recent_crossdown(closes, ema_values, CROSS_LOOKBACK)

    # Anti-chase guard: don't arm if price has already dumped far below EMA.
    ema_distance_pct  = ((ema_now - last_close) / ema_now * 100.0) if ema_now else 0
    not_overextended  = ema_distance_pct <= MAX_EMA_DISTANCE_PCT
    price_below_ema   = last_close < ema_now

    # Final cross gate used by both paths A/B
    cross_valid = cross_down_strict or (
        cross_down_recent and price_below_ema and not_overextended
    )

    # Proximity zone for retest (price pops UP toward EMA from below)
    proximity_level = ema_now * (1 - PROXIMITY_PCT / 100)

    # ─── Get/init per-symbol state ───────────────────────────────────────────
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # =========================================================================
    # TP COMPLETED MONITORING
    # =========================================================================
    # Col B semantics:
    #   - "" or empty     : no live trade on this coin; scanning allowed
    #   - numeric (float) : live trade — stored TP price for this coin
    #   - "TP COMPLETED"  : most recent trade closed at TP; do NOT re-enter
    # =========================================================================
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    # Rule 1: explicit "TP COMPLETED" marker — skip this symbol entirely
    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED marker in sheet, not re-entering")
        save_state(all_state)
        return

    # Rules 2 & 3: if col B has a numeric TP, watch for it being hit.
    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        tp_hit = False
        hit_kind = None
        hit_price = None

        # Rule 2 — TP hit by current 4h CLOSE (for shorts: close ≤ TP)
        if last_close <= tp_stored:
            tp_hit    = True
            hit_kind  = "close"
            hit_price = last_close

        # Rule 3 — TP hit by a WICK between scans (1-min low over last 30 min)
        if not tp_hit:
            recent_low = get_recent_low(symbol)
            if recent_low is not None and recent_low <= tp_stored:
                tp_hit    = True
                hit_kind  = "wick"
                hit_price = recent_low

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} — {hit_kind} {hit_price} ≤ stored TP {tp_stored}")
            send_telegram(
                f"🎯 <b>TP HIT ({hit_kind}) — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 {hit_kind.capitalize():8}: <code>{hit_price}</code>\n"
                f"🎯 TP       : <code>{tp_stored}</code>\n"
                f"✅ Marked <b>TP COMPLETED</b> in sheet — no further entries on this coin"
            )
            if st.get("in_position"):
                all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    # --- Case A: We have an active position on the exchange -----------------
    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close)
            st["in_position"]          = True
            st["entry_path"]           = st.get("entry_path") or "unknown"
            st["entry_price"]          = entry_px
            st["tp_level"]             = round(entry_px * (1 - TP_PCT / 100), precision)
            st["sl_price"]             = round(ema_now  * (1 + SL_ABOVE_EMA_PCT / 100), precision)
            st["waiting_retest"]       = False
            st["wait_start_candle_ts"] = None
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")

        save_state(all_state)
        return

    # --- Case B: Position just closed ---
    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")
        send_telegram(
            f"✅ <b>POSITION CLOSED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path     : <code>{st.get('entry_path')}</code>\n"
            f"📍 Entry    : <code>{st.get('entry_price')}</code>\n"
            f"🎯 TP was   : <code>{st.get('tp_level')}</code>\n"
            f"🛑 SL was   : <code>{st.get('sl_price')}</code>"
        )
        all_state[symbol] = init_symbol_state()
        st = all_state[symbol]
        save_state(all_state)

    # --- Skip if an entry limit order is still on the book ------------------
    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # STRATEGY EVALUATION
    # =========================================================================
    vol_ratio = round(last_vol / vol_avg, 2) if vol_avg else 0

    if cross_down_strict:
        cross_log = "strict(now)"
    elif cross_down_recent:
        cross_log = f"recent({cross_bars_ago}b ago)"
    else:
        cross_log = "none"

    print(
        f"[SCAN] {symbol} | close={last_close} ema200={round(ema_now, precision)} | "
        f"above%={round(above_pct_actual, 1)} (need >={ABOVE_PCT_MIN}) | "
        f"slope%={round(ema_slope_pct, 3)} (need <={MAX_EMA_SLOPE_PCT}) | "
        f"vol={round(last_vol, 2)} avg={round(vol_avg, 2)} ratio={vol_ratio}x | "
        f"pump%={round(pump_pct, 2)} from {pump_anchor_type} (xu_count={crossup_count}, need >={MIN_PUMP_PCT}) | "
        f"pumpC%={round(pump_pct_path_c, 2)} from {path_c_pump_anchor} (need >={PATH_C_MIN_PUMP_PCT}) | "
        f"cross={cross_log} crossValid={cross_valid} | "
        f"distEMA={round(ema_distance_pct, 2)}% (max {MAX_EMA_DISTANCE_PCT}%) notOver={not_overextended} | "
        f"trendQ={trend_qualifies} slopeOK={slope_ok} volOK={vol_ok} pumpOK={pump_ok} "
        f"priceFalling={price_falling} waitingRetest={st['waiting_retest']} "
        f"pathC_armed={st.get('path_c_armed', False)}"
    )

    # =========================================================================
    # PATH A — WAITING RETEST STATE  (already armed in a prior scan)
    # =========================================================================
    if st["waiting_retest"]:
        wait_start = st.get("wait_start_candle_ts")
        if wait_start is None:
            st["waiting_retest"] = False
            save_state(all_state)
        else:
            bars_waiting = max(0, int((last_ts - wait_start) // (CANDLE_SECONDS * 1000)))

            # Invalidated: close back above EMA after >= 1 bar
            if bars_waiting >= 1 and last_close > ema_now:
                print(f"[INVALIDATED] {symbol} — close {last_close} > EMA {round(ema_now, precision)}")
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                save_state(all_state)
                return

            # Timed out
            if bars_waiting > MAX_RETEST_BARS:
                print(f"[TIMEOUT] {symbol} — {bars_waiting} bars > max {MAX_RETEST_BARS}, clearing wait")
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                save_state(all_state)
                return

            # Retest qualification: a 4h bar's high has popped into proximity AND
            # its close stayed below EMA.
            retest_qualified = (bars_waiting >= 1
                                and last_high >= proximity_level
                                and last_close < ema_now)

            if retest_qualified:
                # 30m ENTRY CONFIRMATION: require a closed 30m candle that
                # closes BELOW the EMA level.
                confirm_bar = confirm_30m_close_below(symbol, ema_now)

                if confirm_bar is None:
                    print(f"[PATH A] {symbol} — 4h retest qualified, awaiting 30m close below EMA")
                    save_state(all_state)
                    return

                print(f"[RETEST CONFIRMED] {symbol} — 4h qualified + 30m close {confirm_bar['close']} < EMA (Path A)")

                # Final guard
                if get_position_by_pair(symbol) is not None:
                    print(f"[ABORT] {symbol} — position appeared just before placement")
                    return
                if has_open_order(symbol):
                    print(f"[ABORT] {symbol} — order appeared just before placement")
                    return

                entry_price = float(confirm_bar["close"])
                tp_price    = entry_price * (1 - TP_PCT / 100)
                sl_price    = ema_now     * (1 + SL_ABOVE_EMA_PCT / 100)

                placed = place_short_order(symbol, entry_price, tp_price, sl_price, precision, "retest")
                if placed:
                    st["waiting_retest"]       = False
                    st["wait_start_candle_ts"] = None
                    st["in_position"]          = True
                    st["entry_path"]           = "retest"
                    st["entry_price"]          = round(entry_price, precision)
                    st["tp_level"]             = round(tp_price,    precision)
                    st["sl_price"]             = round(sl_price,    precision)
                    update_sheet_tp(row, st["tp_level"])
                    update_sheet_sl(row, st["sl_price"])

                save_state(all_state)
                return

            print(f"[WAIT] {symbol} — bars_waiting={bars_waiting}/{MAX_RETEST_BARS}")

    # =========================================================================
    # PATH A — ARM NEW REJECTION SETUP
    # =========================================================================
    new_setup = (trend_qualifies
                 and cross_valid
                 and slope_ok
                 and vol_ok
                 and pump_ok
                 and not st["waiting_retest"])

    if new_setup:
        cross_detail = "strict" if cross_down_strict else f"rescued ({cross_bars_ago}b ago)"
        print(
            f"[SETUP ARMED] {symbol} — trendQ ✓ cross:{cross_detail} ✓ "
            f"slope ✓ vol ✓ pump:{round(pump_pct, 2)}% ✓ → waiting retest"
        )
        st["waiting_retest"]       = True
        st["wait_start_candle_ts"] = last_ts
        send_telegram(
            f"🟡 <b>REJECTION SETUP ARMED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Close     : <code>{last_close}</code>\n"
            f"📊 EMA200    : <code>{round(ema_now, precision)}</code>\n"
            f"📈 Above %   : <code>{round(above_pct_actual, 1)}%</code>\n"
            f"📉 Slope %   : <code>{round(ema_slope_pct, 3)}%</code>\n"
            f"📦 Vol ratio : <code>{vol_ratio}x</code>\n"
            f"📈 Pump %    : <code>{round(pump_pct, 2)}% (from {pump_anchor_type}, {crossup_count} crossups)</code>\n"
            f"🔀 Cross     : <code>{cross_detail}</code>\n"
            f"📏 Dist EMA  : <code>{round(ema_distance_pct, 2)}%</code>\n"
            f"🎯 Proximity : <code>{round(proximity_level, precision)}</code>\n"
            f"⌛ Waiting up to {MAX_RETEST_BARS} × 4h candles for retest"
        )
        save_state(all_state)
        return

    # =========================================================================
    # PATH B — MOMENTUM BREAKDOWN  (fires when trend does NOT qualify)
    # =========================================================================
    if USE_BREAKDOWN_PATH:
        breakdown_qualified = ((not trend_qualifies)
                               and cross_valid
                               and price_falling
                               and breakdown_vol_ok
                               and pump_ok)
        if breakdown_qualified:
            cross_detail = "strict" if cross_down_strict else f"rescued ({cross_bars_ago}b ago)"

            # 30m ENTRY CONFIRMATION — require a 30m close below EMA
            confirm_bar = confirm_30m_close_below(symbol, ema_now)
            if confirm_bar is None:
                print(f"[BREAKDOWN] {symbol} — 4h qualified (cross:{cross_detail}, pump:{round(pump_pct, 2)}%), awaiting 30m close below EMA")
                save_state(all_state)
                return

            print(f"[BREAKDOWN] {symbol} — 4h qualified + 30m close {confirm_bar['close']} < EMA (Path B, cross:{cross_detail}, pump:{round(pump_pct, 2)}%)")

            # Final guard
            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            entry_price = float(confirm_bar["close"])
            tp_price    = entry_price * (1 - TP_PCT / 100)
            sl_price    = ema_now     * (1 + SL_ABOVE_EMA_PCT / 100)

            placed = place_short_order(symbol, entry_price, tp_price, sl_price, precision, "breakdown")
            if placed:
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                st["in_position"]          = True
                st["entry_path"]           = "breakdown"
                st["entry_price"]          = round(entry_price, precision)
                st["tp_level"]             = round(tp_price,    precision)
                st["sl_price"]             = round(sl_price,    precision)
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["sl_price"])

            save_state(all_state)
            return

    # =========================================================================
    # PATH C — RESISTANCE REJECTION  (multi-TF pivot confluence)
    # =========================================================================
    # Two-stage logic:
    #   Stage 1 (WAITING): If a zone is already armed, watch for the rejection.
    #     - Track whether price has popped INTO the zone (touched)
    #     - Once touched, wait for a 30m close strictly below the zone lower edge
    #     - On such a close → enter short
    #     - Cancel the armed zone if:
    #         • wait exceeds PATH_C_MAX_WAIT_BARS, OR
    #         • price has risen more than 2% above zone_high (zone broken)
    #
    #   Stage 2 (ARM): Otherwise, if conditions hold, arm a new zone:
    #     - Pump from lowest-EMA crossup ≥ PATH_C_MIN_PUMP_PCT, OR
    #     - Price below EMA within PATH_C_BELOW_EMA_PROXIMITY_PCT, OR
    #     - Price above EMA by ≥ PATH_C_ABOVE_EMA_EXTENDED_PCT
    #     Then find nearest confluence resistance zone ABOVE current price.
    # =========================================================================
    if USE_PATH_C:
        # ── STAGE 1: already armed — watch for rejection ───────────────────
        if st.get("path_c_armed"):
            zone_low    = st.get("path_c_zone_low")
            zone_high   = st.get("path_c_zone_high")
            zone_center = st.get("path_c_zone_center")
            armed_ts    = st.get("path_c_start_ts")

            if None in (zone_low, zone_high, zone_center, armed_ts):
                print(f"[PATH-C] {symbol} — incomplete armed state, clearing")
                st["path_c_armed"] = False
                save_state(all_state)
            else:
                bars_waiting = max(0, int((last_ts - armed_ts) // (CANDLE_SECONDS * 1000)))

                # Zone-broken check: price traded well above zone (abandoned)
                broken_threshold = zone_high * (1 + 2.0 / 100.0)  # 2% above zone_high = broken

                # Mark "touched" if high popped into or near the zone
                if not st.get("path_c_zone_touched"):
                    if last_high >= zone_low * (1 - PATH_C_TOUCH_TOLERANCE_PCT / 100.0) \
                       and last_high <= zone_high * (1 + PATH_C_TOUCH_TOLERANCE_PCT / 100.0):
                        st["path_c_zone_touched"] = True
                        print(f"[PATH-C] {symbol} — zone TOUCHED at high {last_high} (zone {zone_low}–{zone_high})")

                # Zone broken?
                if last_close > broken_threshold:
                    print(f"[PATH-C] {symbol} — zone BROKEN (close {last_close} > {broken_threshold:.6f}), cancelling")
                    send_telegram(
                        f"❌ <b>PATH C CANCELLED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Close    : <code>{last_close}</code>\n"
                        f"🧱 Zone     : <code>{round(zone_low, precision)} – {round(zone_high, precision)}</code>\n"
                        f"⚠️ Reason   : zone broken (price rose &gt;2% above zone)"
                    )
                    st["path_c_armed"]        = False
                    st["path_c_zone_low"]     = None
                    st["path_c_zone_high"]    = None
                    st["path_c_zone_center"]  = None
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = None
                    st["path_c_tf_count"]     = None
                    save_state(all_state)
                    return

                # Timeout?
                if bars_waiting > PATH_C_MAX_WAIT_BARS:
                    print(f"[PATH-C] {symbol} — wait timed out ({bars_waiting} > {PATH_C_MAX_WAIT_BARS})")
                    st["path_c_armed"]        = False
                    st["path_c_zone_low"]     = None
                    st["path_c_zone_high"]    = None
                    st["path_c_zone_center"]  = None
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = None
                    st["path_c_tf_count"]     = None
                    save_state(all_state)
                    return

                # Rejection confirmed on 30m?
                confirm_bar = confirm_30m_touch_and_close_below(symbol, zone_low, zone_high)

                if confirm_bar is not None:
                    entry_price_30m = float(confirm_bar["close"])
                    print(f"[PATH-C] {symbol} — REJECTION CONFIRMED on 30m close {entry_price_30m} < zone_low {zone_low}")

                    # Final placement guards
                    if get_position_by_pair(symbol) is not None:
                        print(f"[ABORT] {symbol} — position appeared just before placement")
                        return
                    if has_open_order(symbol):
                        print(f"[ABORT] {symbol} — order appeared just before placement")
                        return

                    entry_price = entry_price_30m
                    tp_price    = entry_price * (1 - TP_PCT / 100)
                    # SL for Path C: fixed % above zone_high (structural level).
                    sl_price    = zone_high * (1 + PATH_C_SL_ABOVE_ZONE_PCT / 100)

                    placed = place_short_order(symbol, entry_price, tp_price, sl_price, precision, "resistance_rejection")
                    if placed:
                        st["path_c_armed"]        = False
                        st["path_c_zone_low"]     = None
                        st["path_c_zone_high"]    = None
                        st["path_c_zone_center"]  = None
                        st["path_c_zone_touched"] = False
                        st["path_c_start_ts"]     = None
                        st["path_c_tf_count"]     = None
                        st["in_position"]         = True
                        st["entry_path"]          = "resistance_rejection"
                        st["entry_price"]         = round(entry_price, precision)
                        st["tp_level"]            = round(tp_price,    precision)
                        st["sl_price"]            = round(sl_price,    precision)
                        update_sheet_tp(row, st["tp_level"])
                        update_sheet_sl(row, st["sl_price"])

                    save_state(all_state)
                    return

                # Still waiting
                touched_str = "touched" if st.get("path_c_zone_touched") else "awaiting touch"
                print(f"[PATH-C] {symbol} — waiting ({bars_waiting}/{PATH_C_MAX_WAIT_BARS}b, {touched_str}), "
                      f"zone {round(zone_low, precision)}–{round(zone_high, precision)}, close {last_close}")
                save_state(all_state)
                return

        # ── STAGE 2: not armed — evaluate whether to arm a new zone ────────
        path_c_pump_ok = pump_pct_path_c >= PATH_C_MIN_PUMP_PCT

        # NEW: EMA-position-based arm triggers
        ema_diff_pct        = ((last_close - ema_now) / ema_now * 100.0) if ema_now else 0
        below_ema_proximity = (-PATH_C_BELOW_EMA_PROXIMITY_PCT) <= ema_diff_pct < 0
        above_ema_extended  = ema_diff_pct >= PATH_C_ABOVE_EMA_EXTENDED_PCT

        path_c_arm_ok = path_c_pump_ok or below_ema_proximity or above_ema_extended

        if path_c_arm_ok:
            # Build trigger label for logs / telegram
            triggers = []
            if path_c_pump_ok:      triggers.append(f"pump≥{PATH_C_MIN_PUMP_PCT}%")
            if below_ema_proximity: triggers.append(f"below EMA within {PATH_C_BELOW_EMA_PROXIMITY_PCT}%")
            if above_ema_extended:  triggers.append(f"above EMA ≥{PATH_C_ABOVE_EMA_EXTENDED_PCT}%")
            trigger_str = " + ".join(triggers)

            zone = find_nearest_resistance_zone_above(symbol, last_close)

            if zone is not None:
                if zone["low"] > last_close:
                    tf_count   = len(zone["tfs"])
                    tfs_str    = ",".join(sorted(zone["tfs"]))
                    zone_low   = zone["low"]
                    zone_high  = zone["high"]
                    zone_cent  = zone["center"]
                    dist_pct   = (zone_cent - last_close) / last_close * 100.0

                    print(f"[PATH-C] {symbol} — ARMING zone {round(zone_low, precision)}–{round(zone_high, precision)} "
                          f"(center {round(zone_cent, precision)}, {tf_count} TFs: {tfs_str}, "
                          f"{round(dist_pct, 2)}% above close, pump_c {round(pump_pct_path_c, 2)}% from {path_c_pump_anchor}, "
                          f"trigger: {trigger_str})")

                    st["path_c_armed"]        = True
                    st["path_c_zone_low"]     = zone_low
                    st["path_c_zone_high"]    = zone_high
                    st["path_c_zone_center"]  = zone_cent
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = last_ts
                    st["path_c_tf_count"]     = tf_count

                    send_telegram(
                        f"🟠 <b>PATH C ARMED (resistance rejection) — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Close      : <code>{last_close}</code>\n"
                        f"🧱 Zone low   : <code>{round(zone_low, precision)}</code>\n"
                        f"🧱 Zone high  : <code>{round(zone_high, precision)}</code>\n"
                        f"📊 Zone cent  : <code>{round(zone_cent, precision)}</code>\n"
                        f"⏫ Dist above : <code>{round(dist_pct, 2)}%</code>\n"
                        f"📈 Pump (C)   : <code>{round(pump_pct_path_c, 2)}% ({path_c_pump_anchor})</code>\n"
                        f"⚙️ Trigger    : <code>{trigger_str}</code>\n"
                        f"📏 EMA diff   : <code>{round(ema_diff_pct, 2)}%</code>\n"
                        f"🪢 Confluence : <code>{tf_count} TFs ({tfs_str})</code>\n"
                        f"⌛ Waiting up to {PATH_C_MAX_WAIT_BARS} × 30m bars for rejection"
                    )
                    save_state(all_state)
                    return

    # No action this cycle
    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>SHORT Bot Started — Triple-Path</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy   : <code>200 EMA Triple-Path (Rejection + Breakdown + Resistance Rejection)</code>\n"
    f"⏱ Analysis   : <code>4h primary (closed bars only)</code>\n"
    f"⚡ Entry      : <code>30m close confirmation (all paths)</code>\n"
    f"🔁 Scan       : <code>Every 30 minutes</code>\n"
    f"🅰️ Path A    : <code>{ABOVE_PCT_MIN}% above EMA + crossdown + slope≤{MAX_EMA_SLOPE_PCT}% + vol×{VOL_MULTIPLIER} + pump≥{MIN_PUMP_PCT}% → wait retest (≤{MAX_RETEST_BARS} × 4h bars)</code>\n"
    f"🅱️ Path B    : <code>crossdown + close&lt;close[{MOMENTUM_LOOKBACK}] + vol×{BREAKDOWN_VOL_MULT} + pump≥{MIN_PUMP_PCT}% when trend NOT qualifying</code>\n"
    f"🆎 Path C    : <code>(pump≥{PATH_C_MIN_PUMP_PCT}% OR below EMA within {PATH_C_BELOW_EMA_PROXIMITY_PCT}% OR above EMA ≥{PATH_C_ABOVE_EMA_EXTENDED_PCT}%) + nearest multi-TF pivot zone above price ({MIN_TF_CONFLUENCE}/{len(PATH_C_ENABLED_TIMEFRAMES)} TFs) → 30m break-below (≤{PATH_C_MAX_WAIT_BARS} bars)</code>\n"
    f"🔀 Cross      : <code>strict OR within last {CROSS_LOOKBACK} bars (if price still below EMA and ≤{MAX_EMA_DISTANCE_PCT}% away)</code>\n"
    f"📈 Pump A/B   : <code>min-EMA across all crossups (or lowest-low if never crossed) → highest-high across last {PUMP_LOOKBACK} × 4h bars must be ≥{MIN_PUMP_PCT}%</code>\n"
    f"🧱 Pivots     : <code>N={PIVOT_STRENGTH} each side, ±{PIVOT_ZONE_PCT}% zone band, TFs: 4h + 12h synth + 1D</code>\n"
    f"🎯 TP         : <code>{TP_PCT}% fixed below entry</code>\n"
    f"🛑 SL         : <code>EMA × (1 + {SL_ABOVE_EMA_PCT}%) for Paths A/B  •  zone_high × (1 + {PATH_C_SL_ABOVE_ZONE_PCT}%) for Path C</code>\n"
    f"📊 Min RR     : <code>{MIN_RR}</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 30 min")
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