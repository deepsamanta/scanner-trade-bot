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
# STRATEGY: VWAP (hlc3, UTC session) Mean-Reversion — All Instruments — 15m TF
#
# Sheet contains ALL CoinDCX USDT futures instruments.
# Each cycle the bot self-selects the best MAX_OPEN_TRADES setups via 4 stages:
#
# STAGE 1 — UNIVERSE FILTER:
#   • Exclude stablecoins and wrapped tokens (sheet-level)
#   • 24h USD volume >= MIN_24H_VOL_USDT — fetched PER COIN from the latest
#     DAILY futures candle (volume x close), not the bulk ticker endpoint
#
# STAGE 2 — EXTENSION SCREEN  (per-symbol, 15m candle-based):
#   VWAP = SESSION VWAP (hlc3), cum(hlc3 x vol)/cum(vol), RESET EVERY UTC DAY.
#   So each day has its OWN independent VWAP line — day 1 has its VWAP,
#   day 2 has its VWAP, day 3 has its VWAP.
#   Every bar is judged against ITS OWN DAY'S session VWAP.
#   • Look back over the last DAYS_LOOKBACK (3) UTC days:
#       if >= VWAP_EXT_MIN_PCT% of those bars closed BELOW their own day's
#       session VWAP -> the coin has been under VWAP for days  -> LONG setup
#       if >= VWAP_EXT_MIN_PCT% closed ABOVE                    -> SHORT setup
#     (per-day breakdown is printed so the behaviour is visible)
#   • TP sanity: 200 EMA (15m) must sit MIN_TP_PCT..MAX_TP_PCT away
#     on the correct side (above entry for long, below for short)
#
# STAGE 3 — REVERSAL + CONFIRMATION  (all vs TODAY'S session VWAP):
#   LONG  (was stuck BELOW VWAP for the past days):
#     • bar[-2] closed back ABOVE today's session VWAP   (the REVERSAL)
#     • bar[-1] low tagged VWAP (within RESPECT_TOL_PCT)
#       AND closed above it                              (VWAP RESPECTED)
#     • VWAP ANGLE rising: vwap[-1] vs vwap[-1-VWAP_SLOPE_BARS] must be
#       up by at least MIN_VWAP_SLOPE_PCT% — a flat VWAP is not a turn
#   SHORT (was stuck ABOVE VWAP): exact mirror
#   Today's session needs only MIN_TRIGGER_SESSION_BARS bars (45 min) —
#   just enough for the VWAP line to exist. No 3-hour wait.
#
# STAGE 4 — RANKING  (all Stage-3 survivors scored 0-100):
#     (1) Extension ratio (time on one side)  25 pts
#     (2) Distance to 200 EMA target (reward) 25 pts
#     (3) VWAP slope strength                 20 pts
#     (4) 24h USD volume rank                 30 pts
#   -> Top N fill available slots (MAX_OPEN_TRADES - open positions).
#
# ENTRY : Limit order at close of the respect candle
# TP    : current 200 EMA (15m) value
# SL    : 1 candle beyond VWAP — lowest low (long) / highest high (short)
#         of the last 2 candles
# =============================================================================

# ── Trade params ──────────────────────────────────────────────────────────────
MAX_OPEN_TRADES    = 5     # max concurrent positions
MIN_TP_PCT         = 1.0   # skip if 200 EMA closer than this (reward too small)
MAX_TP_PCT         = 15.0  # skip if 200 EMA farther than this (target unrealistic)

# ── Universe filter ───────────────────────────────────────────────────────────
MIN_24H_VOL_USDT   = 1_000_000   # $1M 24h USD volume floor (per-coin daily candle)

STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "UST", "LUSD",
    "FDUSD", "PYUSD", "USDD", "USDN", "GUSD", "SUSD", "CUSD", "USDX", "OUSD",
}
WRAPPED = {"WBTC", "WETH", "WBNB", "WMATIC", "WAVAX", "WSOL", "WFTM"}

# ── Strategy params ───────────────────────────────────────────────────────────
EMA200_15M_LEN     = 200   # 15m 200 EMA — the TP target
DAYS_LOOKBACK      = 3     # how many UTC days to check one-sidedness over
VWAP_EXT_MIN_PCT   = 75    # >= this % of those bars on one side = "long time"
VWAP_EXT_MIN_BARS  = 100   # need at least this many bars in the window to judge
VWAP_SLOPE_BARS    = 5     # vwap[-1] vs vwap[-1-N] for the angle measurement
MIN_VWAP_SLOPE_PCT = 0.05  # minimum VWAP angle % — flat VWAP is not a turn
MIN_TRIGGER_SESSION_BARS = 3   # today's VWAP needs >= this many bars to be real
RESPECT_TOL_PCT    = 0.2   # wick may miss VWAP by this % and still count as a touch

# ── Candle counts ─────────────────────────────────────────────────────────────
CANDLES_15M        = 340   # 3 UTC days (288 bars) for the lookback + EMA200 + buffer
CANDLES_1M         = 5

# ── Resolutions ───────────────────────────────────────────────────────────────
RESOLUTION_15M     = "15"
RESOLUTION_1M      = "1"
RESOLUTION_DAILY   = "1D"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1M  = 60

# ── Timing ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "vwap_bot_state.json"


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


def compute_session_vwap_series(candles):
    """
    SESSION VWAP (hlc3) — cumulated within each UTC day and RESET at every
    day boundary. Each day therefore gets its own independent VWAP line.
    Returns:
      vwaps        — list aligned 1:1 with candles (each bar's own-day VWAP)
      day_keys     — list of 'YYYY-MM-DD' aligned 1:1 with candles
      session_bars — number of bars in the CURRENT (latest) session
    """
    vwaps        = []
    day_keys     = []
    cum_pv       = 0.0
    cum_v        = 0.0
    cur_day      = None
    session_bars = 0

    for c in candles:
        ts_sec = int(c["time"]) // 1000
        day    = datetime.fromtimestamp(ts_sec, timezone.utc).strftime("%Y-%m-%d")
        if day != cur_day:            # new UTC day -> reset the VWAP
            cur_day      = day
            cum_pv       = 0.0
            cum_v        = 0.0
            session_bars = 0

        h  = float(c["high"])
        l  = float(c["low"])
        cl = float(c["close"])
        v  = float(c["volume"])

        cum_pv += ((h + l + cl) / 3.0) * v
        cum_v  += v
        vwaps.append(cum_pv / cum_v if cum_v > 0 else cl)
        day_keys.append(day)
        session_bars += 1

    return vwaps, day_keys, session_bars


# =====================================================
# STAGE 1 — UNIVERSE FILTER (per-coin volume from daily candle)
# =====================================================

def fetch_24h_volume(symbol):
    """
    Per-coin 24h USD volume from the latest DAILY futures candle
    (volume x close). Returns (vol_usd, raw_daily_candle | None).
    """
    try:
        now        = int(time.time())
        from_time  = now - (3 * 24 * 60 * 60)
        url        = "https://public.coindcx.com/market_data/candlesticks"
        params     = {
            "pair":       fut_pair(symbol),
            "from":       from_time,
            "to":         now,
            "resolution": RESOLUTION_DAILY,
            "pcode":      "f",
        }
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"  [VOL] {symbol}  HTTP {response.status_code}")
            return 0.0, None
        data        = response.json()
        candle_list = data.get("data", data) if isinstance(data, dict) else data
        if not candle_list:
            print(f"  [VOL] {symbol}  no daily candles returned")
            return 0.0, None
        candle_list = sorted(candle_list, key=lambda x: x["time"])
        daily       = candle_list[-1]
        volume_qty  = float(daily.get("volume", 0) or 0)
        close_px    = float(daily.get("close",  0) or 0)
        vol_usd     = volume_qty * close_px
        print(f"  [VOL] {symbol}  volume_qty={volume_qty:,.2f} x close={close_px} "
              f"=> 24h_usd=${vol_usd:,.0f}")
        return vol_usd, daily
    except Exception as e:
        print(f"  [VOL] {symbol}  error: {e}")
        return 0.0, None


def is_excluded(symbol):
    """True if stablecoin or wrapped token."""
    base = symbol.replace("USDT", "")
    return base in STABLECOINS or base in WRAPPED


def build_eligible_universe(all_symbols_rows):
    """
    Stage 1 (sheet-level): drop stables/wrapped only.
    Volume is checked PER COIN inside check_and_trade via fetch_24h_volume.
    Returns list of (symbol, row).
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


# =====================================================
# STAGES 2-3 — EXTENSION SCREEN + ENTRY SIGNAL
# =====================================================

def check_extension(candles, vwaps, day_keys):
    """
    Stage 2 — Over the last DAYS_LOOKBACK UTC days, was price sitting on ONE
    side of VWAP? Every bar is compared to ITS OWN DAY'S session VWAP
    (day 1 vs day 1's VWAP, day 2 vs day 2's VWAP, ...).
    The last 2 bars are excluded — they are the reversal trigger bars.
    Returns (side, below_pct, above_pct, counted, per_day):
        side = "long"  -> was stuck below VWAP  (long reversal setup)
        side = "short" -> was stuck above VWAP  (short reversal setup)
        side = None    -> no clear one-sided behaviour
        per_day = {day: (below, total)} for logging
    """
    # the DAYS_LOOKBACK most recent UTC days present in the data
    recent_days = sorted(set(day_keys))[-DAYS_LOOKBACK:]

    below   = 0
    above   = 0
    counted = 0
    per_day = {d: [0, 0] for d in recent_days}   # day -> [below, total]

    for i in range(len(candles) - 2):            # exclude the 2 trigger bars
        d = day_keys[i]
        if d not in per_day:
            continue
        counted += 1
        per_day[d][1] += 1
        if float(candles[i]["close"]) < vwaps[i]:
            below += 1
            per_day[d][0] += 1
        else:
            above += 1

    if counted < VWAP_EXT_MIN_BARS:
        return None, 0.0, 0.0, counted, per_day

    below_pct = below / counted * 100
    above_pct = above / counted * 100

    if below_pct >= VWAP_EXT_MIN_PCT:
        return "long", round(below_pct, 2), round(above_pct, 2), counted, per_day
    if above_pct >= VWAP_EXT_MIN_PCT:
        return "short", round(below_pct, 2), round(above_pct, 2), counted, per_day
    return None, round(below_pct, 2), round(above_pct, 2), counted, per_day


def check_vwap_slope(vwaps, direction):
    """
    Stage 3a — VWAP ANGLE confirmation.
    Measures % change of today's session VWAP over VWAP_SLOPE_BARS bars.
    LONG needs it rising by >= MIN_VWAP_SLOPE_PCT, SHORT falling by that much.
    A near-flat VWAP means the market hasn't actually turned — rejected.
    Returns (ok, slope_pct).
    """
    if len(vwaps) < VWAP_SLOPE_BARS + 1:
        return False, 0.0
    v_now  = vwaps[-1]
    v_then = vwaps[-1 - VWAP_SLOPE_BARS]
    if v_now is None or v_then is None or v_then <= 0:
        return False, 0.0
    slope_pct = (v_now / v_then - 1) * 100
    if direction == "long":
        return slope_pct >= MIN_VWAP_SLOPE_PCT, round(slope_pct, 4)
    else:
        return slope_pct <= -MIN_VWAP_SLOPE_PCT, round(slope_pct, 4)


def check_reclaim_and_respect(candles, vwaps, direction):
    """
    Stage 3b — Reclaim + retest RESPECTED (the actual trigger).
    LONG  (was extended below):
      • bar[-2] close > vwap[-2]                       (reclaimed VWAP)
      • bar[-1] low  <= vwap[-1] * (1 + tol)           (retested VWAP)
      • bar[-1] close > vwap[-1]                       (respected — held above)
    SHORT is the exact mirror.
    Returns (ok, entry_close, vwap_now).
    """
    tol   = RESPECT_TOL_PCT / 100.0
    c2, c1 = candles[-2], candles[-1]
    v2, v1 = vwaps[-2],  vwaps[-1]
    if v1 is None or v2 is None:
        return False, 0.0, 0.0

    close2 = float(c2["close"])
    close1 = float(c1["close"])
    low1   = float(c1["low"])
    high1  = float(c1["high"])

    if direction == "long":
        reclaimed = close2 > v2
        retested  = low1 <= v1 * (1 + tol)
        respected = close1 > v1
    else:
        reclaimed = close2 < v2
        retested  = high1 >= v1 * (1 - tol)
        respected = close1 < v1

    return (reclaimed and retested and respected), round(close1, 8), round(v1, 8)


def check_tp_target(entry_price, ema200, direction):
    """
    Stage 2b — TP sanity. 200 EMA must be on the correct side of entry
    and MIN_TP_PCT..MAX_TP_PCT away.
    Returns (ok, tp_dist_pct).
    """
    if ema200 is None or entry_price <= 0:
        return False, 0.0
    if direction == "long":
        dist_pct = (ema200 / entry_price - 1) * 100
    else:
        dist_pct = (1 - ema200 / entry_price) * 100
    return (MIN_TP_PCT <= dist_pct <= MAX_TP_PCT), round(dist_pct, 4)


# =====================================================
# STAGE 4 — SCORING
# =====================================================

def score_candidate(ext_ratio, tp_dist_pct, slope_pct, vol_24h_usd):
    """
    0-100 scale. Weights:
      (1) Extension ratio (time on one side)  25 pts  (100% of lookback = max)
      (2) Distance to 200 EMA target          25 pts  (8%+ reward = max)
      (3) VWAP slope strength                 20 pts  (0.5% over slope bars = max)
      (4) 24h USD liquidity                   30 pts  ($50M+ = max)
    """
    s1 = min(ext_ratio,                 1.0) * 25
    s2 = min(tp_dist_pct / 8.0,         1.0) * 25
    s3 = min(abs(slope_pct) / 0.5,      1.0) * 20
    s4 = min(vol_24h_usd / 50_000_000,  1.0) * 30
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
# PLACE ORDER (LONG + SHORT)
# =====================================================

def place_reversal_order(symbol, direction, entry_price, tp_price, sl_price, precision):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    if direction == "long":
        side   = "buy"
        tp_pct = round(((tp - entry) / entry) * 100, 2)
        sl_pct = round(((entry - sl) / entry) * 100, 2)
        emoji  = "🟢"
        label  = "LONG"
    else:
        side   = "sell"
        tp_pct = round(((entry - tp) / entry) * 100, 2)
        sl_pct = round(((sl - entry) / entry) * 100, 2)
        emoji  = "🔴"
        label  = "SHORT"

    print(f"  [{label}] Entry={entry}  TP={tp}(+{tp_pct}%)  SL={sl}(-{sl_pct}%)  Qty={qty}")

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side": side, "pair": fut_pair(symbol),
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
        print(f"  [ERROR] {label.lower()} rejected: {result}")
        send_telegram(f"❌ <b>{label} REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None

    send_telegram(
        f"{emoji} <b>NEW {label} (VWAP REVERSAL) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%  = 200 EMA)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%  beyond VWAP)\n"
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
    cand keys: symbol, row, direction, entry_price, tp_price, sl_price,
               precision, curr_ts
    """
    symbol      = cand["symbol"]
    row         = cand["row"]
    direction   = cand["direction"]
    entry_price = cand["entry_price"]
    tp_price    = cand["tp_price"]
    sl_price    = cand["sl_price"]
    precision   = cand["precision"]
    curr_ts     = cand["curr_ts"]

    st = all_state.setdefault(symbol, init_symbol_state())

    placed, confirmed_entry, confirmed_tp = place_reversal_order(
        symbol, direction, entry_price, tp_price, sl_price, precision
    )

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

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
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

    min_15m = max(EMA200_15M_LEN, VWAP_EXT_MIN_BARS + 2) + 5
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
        direction  = st.get("direction") or "long"
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit     = False
        hit_kind   = None
        hit_price  = None

        if direction == "short":
            tp_threshold = tp_stored * 1.0001
            if last_close and last_close <= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rl = get_recent_low(symbol)
                if rl and rl <= tp_threshold:
                    tp_hit, hit_kind, hit_price = True, "wick", rl
        else:
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

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — candle already processed")
        save_state(all_state)
        return None

    # ── 7b. STAGE 1 (per-coin) — volume from DAILY futures candle ────────────
    vol_24h_usd, raw_daily = fetch_24h_volume(symbol)
    if raw_daily is None:
        print(f"  [{symbol}] DISCARDED — could not fetch daily candle for volume")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    if vol_24h_usd < MIN_24H_VOL_USDT:
        print(f"  [{symbol}] DISCARDED — 24h vol ${vol_24h_usd:,.0f} "
              f"< threshold ${MIN_24H_VOL_USDT:,.0f}")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] Volume PASS — 24h vol ${vol_24h_usd:,.0f}")

    # ── 8. Compute session VWAP (hlc3, reset each UTC day) + 200 EMA ─────────
    vwaps, day_keys, session_bars = compute_session_vwap_series(candles_15m)
    closes = [float(c["close"]) for c in candles_15m]
    ema200 = compute_ema(closes, EMA200_15M_LEN)

    st["last_candle_ts"] = curr_ts

    # Today's VWAP line must exist as more than 1-2 bars, otherwise vwap[-1]
    # is essentially the bar's own hlc3 and the reclaim/respect test is noise.
    if session_bars < MIN_TRIGGER_SESSION_BARS:
        print(f"  [{symbol}] SKIP — today's VWAP only {session_bars} bar(s) old "
              f"(need {MIN_TRIGGER_SESSION_BARS})")
        save_state(all_state)
        return None

    # ── 9. STAGE 2 — Extension over the last DAYS_LOOKBACK UTC days ──────────
    direction, below_pct, above_pct, counted, per_day = check_extension(
        candles_15m, vwaps, day_keys)

    day_report = "  ".join(
        f"{d[-5:]}:{(b / t * 100):.0f}%below({t}b)" if t else f"{d[-5:]}:--"
        for d, (b, t) in sorted(per_day.items())
    )
    print(f"  [{symbol}] {DAYS_LOOKBACK}d vs own-day VWAP -> {day_report}")
    print(f"  [{symbol}] extension={direction}  below={below_pct}%  above={above_pct}%  "
          f"bars={counted}  (need {VWAP_EXT_MIN_PCT}% of >= {VWAP_EXT_MIN_BARS})")

    if direction is None:
        save_state(all_state)
        return None

    # ── 10. STAGE 3 — Reversal + respect + VWAP angle ────────────────────────
    trigger_ok, entry_close, vwap_now = check_reclaim_and_respect(candles_15m, vwaps, direction)
    print(f"  [{symbol}] reversal+respect={trigger_ok}  close={entry_close}  "
          f"today_vwap={vwap_now}")

    slope_ok, slope_pct = check_vwap_slope(vwaps, direction)
    print(f"  [{symbol}] vwap_angle_ok={slope_ok}  angle={slope_pct}% over "
          f"{VWAP_SLOPE_BARS} bars (need {MIN_VWAP_SLOPE_PCT}%)")

    if not trigger_ok or not slope_ok:
        save_state(all_state)
        return None

    # ── 11. TP sanity — 200 EMA must be a valid target ───────────────────────
    tp_ok, tp_dist_pct = check_tp_target(entry_close, ema200, direction)
    print(f"  [{symbol}] tp_target_ok={tp_ok}  ema200={round(ema200, 8) if ema200 else None}  "
          f"dist={tp_dist_pct}%")

    if not tp_ok:
        save_state(all_state)
        return None

    # ── 12. Build entry / TP / SL ────────────────────────────────────────────
    entry_price = round(entry_close, precision)
    tp_price    = round(ema200, precision)

    low1  = float(candles_15m[-1]["low"])
    low2  = float(candles_15m[-2]["low"])
    high1 = float(candles_15m[-1]["high"])
    high2 = float(candles_15m[-2]["high"])

    if direction == "long":
        sl_price = round(min(low1, low2, vwap_now), precision)     # 1 candle below VWAP
        if sl_price >= entry_price:
            print(f"  [{symbol}] SKIP — invalid SL (>= entry)")
            save_state(all_state)
            return None
    else:
        sl_price = round(max(high1, high2, vwap_now), precision)   # 1 candle above VWAP
        if sl_price <= entry_price:
            print(f"  [{symbol}] SKIP — invalid SL (<= entry)")
            save_state(all_state)
            return None

    # ── 13. STAGE 4 — Build candidate dict with score ────────────────────────
    ext_pct   = below_pct if direction == "long" else above_pct
    ext_ratio = ext_pct / 100.0

    candidate_score = score_candidate(ext_ratio, tp_dist_pct, slope_pct, vol_24h_usd)

    print(f"  [{symbol}] CANDIDATE ({direction.upper()})  score={candidate_score}  "
          f"entry={entry_price}  tp={tp_price}  sl={sl_price}")

    return {
        "symbol":      symbol,
        "row":         row,
        "direction":   direction,
        "score":       candidate_score,
        "entry_price": entry_price,
        "tp_price":    tp_price,
        "sl_price":    sl_price,
        "precision":   precision,
        "curr_ts":     curr_ts,
        "ext_ratio":   round(ext_ratio, 4),
        "tp_dist_pct": tp_dist_pct,
        "slope_pct":   slope_pct,
        "vol_24h_usd": round(vol_24h_usd, 0),
    }


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>VWAP Reversal Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy : <code>Session VWAP (hlc3) Reversal — {DAYS_LOOKBACK}-Day Bias — 15m — Long+Short</code>\n"
    f"\n"
    f"🔍 Stage 1 (Universe):\n"
    f"  <code>• Exclude stables / wrapped tokens</code>\n"
    f"  <code>• Per-coin 24h vol &gt;= ${MIN_24H_VOL_USDT:,} USD (daily candle)</code>\n"
    f"\n"
    f"🔍 Stage 2 (Extension):\n"
    f"  <code>• VWAP resets each UTC day — each day its own line</code>\n"
    f"  <code>• &gt;= {VWAP_EXT_MIN_PCT}% of last {DAYS_LOOKBACK} days' bars on ONE side of own-day VWAP</code>\n"
    f"  <code>• 200 EMA target {MIN_TP_PCT}%-{MAX_TP_PCT}% away</code>\n"
    f"\n"
    f"🔍 Stage 3 (Reversal):\n"
    f"  <code>• Reclaim today's VWAP + retest respected (tol {RESPECT_TOL_PCT}%)</code>\n"
    f"  <code>• VWAP angle &gt;= {MIN_VWAP_SLOPE_PCT}% over {VWAP_SLOPE_BARS} bars</code>\n"
    f"\n"
    f"📊 Stage 4 (Rank): <code>Top {MAX_OPEN_TRADES} by weighted score</code>\n"
    f"🎯 TP : <code>200 EMA (15m)</code>  |  🛑 SL: <code>1 candle beyond VWAP</code>\n"
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

        # ── STAGE 1: Universe filter (stables/wrapped only; volume per-coin) ─
        eligible = build_eligible_universe(all_symbols_rows)

        # Force-include symbols with active tracked state so TP monitoring and
        # reconciliation still run even if the symbol was filtered by Stage 1.
        eligible_set = {s for s, _ in eligible}
        for sym, sym_st in state.items():
            if (sym_st.get("in_position") or sym_st.get("tp_level")) and sym not in eligible_set:
                r = row_index.get(sym)
                if r is not None:
                    eligible.append((sym, r))
                    eligible_set.add(sym)
                    print(f"[FORCE-INCLUDE] {sym} — active state, monitoring only")

        # ── Slot calculation ─────────────────────────────────────────────────
        active_count    = len(global_positions)
        slots_available = max(0, MAX_OPEN_TRADES - active_count)
        print(f"[SLOTS] {active_count} open / {MAX_OPEN_TRADES} max -> {slots_available} slot(s)")

        # ── STAGES 2-3: Score each eligible symbol, collect candidates ───────
        candidates = []

        for symbol, row in eligible:
            print(f"--- {symbol} ---")
            try:
                cand = check_and_trade(
                    symbol, row, df, state, global_positions, global_orders
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
            print(f"  [{tag}] {c['symbol']} ({c['direction']})  score={c['score']}  "
                  f"ext={c['ext_ratio']}  "
                  f"tp_dist={c['tp_dist_pct']}%  "
                  f"slope={c['slope_pct']}%  "
                  f"24h=${c['vol_24h_usd']:,.0f}")

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
                msg += (f"  • {c['symbol']} ({c['direction']})  "
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