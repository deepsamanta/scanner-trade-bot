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
# STRATEGY PARAMETERS — SHORT bot (Path C only: resistance rejection)
#
# TIMEFRAME ARCHITECTURE:
#   - Primary analysis  : 4h (EMA + position-based arm triggers)
#   - Entry confirmation: 30m close below zone_low
#   - Pivot confluence  : 4h + 12h synth + 1D + 3D synth (≥3 of 4 TFs)
#   - Scan interval     : 30 minutes
#
# ARM TRIGGERS (only two — pump filter & trend qualifier removed):
#   (a) close ≥ +10% above EMA → find nearest resistance above price
#   (b) close within -5% to 0% of EMA → find resistance ABOVE price AND BELOW EMA
# =============================================================================

# ─── CORE ─────────────────────────────────────────────────────────────────────
EMA_PERIOD           = 200
TP_PCT               = 5         # Take Profit % below entry (fixed)

# ─── PATH C ARM TRIGGERS ─────────────────────────────────────────────────────
PATH_C_BELOW_EMA_PROXIMITY_PCT = 5.0     # -5% ≤ (close-EMA)/EMA < 0  →  resistance must lie BETWEEN price & EMA
PATH_C_ABOVE_EMA_EXTENDED_PCT  = 10.0    # (close-EMA)/EMA ≥ +10%     →  any resistance above price

# ─── PATH C ZONE / CONFLUENCE ────────────────────────────────────────────────
PATH_C_ENABLED_TIMEFRAMES   = ["240", "12H_synth", "1D", "3D_synth"]   # 4h + 12h synth + 1D + 3D synth
PATH_C_CANDLES              = 600        # bars per TF
PIVOT_STRENGTH              = 3          # bars on each side for pivot detection
PIVOT_ZONE_PCT              = 1.0        # ±% band for clustering pivots
MIN_TF_CONFLUENCE           = 3          # ≥3 of 4 TFs must defend the zone
PATH_C_MAX_WAIT_BARS        = 30         # max 4h bars to wait for rejection (≈5 days)
PATH_C_TOUCH_TOLERANCE_PCT  = 0.5        # wick tolerance into zone band
PATH_C_SL_ABOVE_ZONE_PCT    = 1.0        # SL: zone_high × (1 + 1.0%)

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY   = "240"
RESOLUTION_ENTRY     = "30"
CANDLE_SECONDS       = 4 * 3600
ENTRY_CANDLE_SECONDS = 30 * 60
SCAN_INTERVAL        = 1800

# ─── TIMEOUTS / MISC ─────────────────────────────────────────────────────────
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
        # ── Path C state ──────────────────────────────
        "path_c_armed":        False,
        "path_c_start_ts":     None,
        "path_c_zone_low":     None,
        "path_c_zone_high":    None,
        "path_c_zone_center":  None,
        "path_c_zone_touched": False,
        "path_c_tf_count":     None,
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
        requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATORS
# =====================================================

def compute_ema(closes, period):
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


# =====================================================
# CANDLE FETCH (4h primary + 30m entry)
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


def fetch_candles_tf(symbol, resolution_str, num_candles_needed):
    """
    Multi-TF candle fetch.
    Native: "1", "5", "15", "30", "60", "240", "1D"
    Synthetic: "12H_synth" (3 × 4h), "3D_synth" (3 × 1D)
    """
    res_to_seconds = {
        "1": 60, "5": 5*60, "15": 15*60, "30": 30*60,
        "60": 60*60, "240": 4*60*60, "1D": 24*60*60,
    }

    # Synthetic 12h from 3 × 4h
    if resolution_str == "12H_synth":
        return _build_synthetic(symbol, "240", 3, num_candles_needed)

    # Synthetic 3D from 3 × 1D
    if resolution_str == "3D_synth":
        return _build_synthetic(symbol, "1D", 3, num_candles_needed)

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
        return sorted(data, key=lambda x: x["time"])
    except Exception as e:
        print(f"[CANDLES-TF {resolution_str}] {symbol} fetch error: {e}")
        return []


def _build_synthetic(symbol, base_res, group_size, num_candles_needed):
    """Aggregate `group_size` consecutive base_res candles into one synthetic candle."""
    needed_base = num_candles_needed * group_size + 10
    base_candles = fetch_candles_tf(symbol, base_res, needed_base)
    if len(base_candles) < group_size:
        return []
    synthetic = []
    for i in range(0, len(base_candles) - (group_size - 1), group_size):
        group = base_candles[i:i + group_size]
        if len(group) != group_size:
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


# =====================================================
# PIVOT DETECTION (Path C)
# =====================================================

def find_pivots(highs, lows, strength):
    """Return flat list of pivot prices (both pivot highs and lows)."""
    pivot_prices = []
    n = len(highs)
    if n < 2 * strength + 1:
        return pivot_prices

    for i in range(strength, n - strength):
        h_center = highs[i]
        l_center = lows[i]

        is_pivot_high = True
        for k in range(1, strength + 1):
            if highs[i - k] >= h_center or highs[i + k] >= h_center:
                is_pivot_high = False
                break
        if is_pivot_high:
            pivot_prices.append(h_center)
            continue

        is_pivot_low = True
        for k in range(1, strength + 1):
            if lows[i - k] <= l_center or lows[i + k] <= l_center:
                is_pivot_low = False
                break
        if is_pivot_low:
            pivot_prices.append(l_center)

    return pivot_prices


def cluster_pivots_to_zones(pivots_by_tf, proximity_pct):
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
    prices = [p for p, _ in pivots]
    tfs    = {tf for _, tf in pivots}
    return {
        "center": sum(prices) / len(prices),
        "low":    min(prices),
        "high":   max(prices),
        "tfs":    tfs,
        "pivots": pivots,
    }


def find_nearest_resistance_zone_above(symbol, current_price, max_price=None):
    """
    Find nearest multi-TF confluence zone strictly above current_price.
    If max_price is set, zone's UPPER edge must be < max_price (used for the
    "below EMA within 5%" case where resistance must lie between price and EMA).
    """
    pivots_by_tf = {}
    for tf in PATH_C_ENABLED_TIMEFRAMES:
        candles = fetch_candles_tf(symbol, tf, PATH_C_CANDLES)
        if len(candles) < 2 * PIVOT_STRENGTH + 1:
            print(f"[PATH-C] {symbol} TF {tf} — insufficient candles ({len(candles)}), skipping")
            continue
        tf_highs = [float(c["high"]) for c in candles]
        tf_lows  = [float(c["low"])  for c in candles]
        pivots   = find_pivots(tf_highs, tf_lows, PIVOT_STRENGTH)
        pivots_by_tf[tf] = pivots

    if not pivots_by_tf:
        return None

    zones = cluster_pivots_to_zones(pivots_by_tf, PIVOT_ZONE_PCT)
    strong_zones = [z for z in zones if len(z["tfs"]) >= MIN_TF_CONFLUENCE]
    if not strong_zones:
        return None

    above_zones = [z for z in strong_zones if z["low"] > current_price]
    if max_price is not None:
        above_zones = [z for z in above_zones if z["high"] < max_price]
    if not above_zones:
        return None

    return min(above_zones, key=lambda z: z["center"])


# =====================================================
# 30-MINUTE ENTRY CONFIRMATION
# =====================================================

def _fetch_closed_30m_candles(symbol, num_candles=6):
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


def confirm_30m_touch_and_close_below(symbol, zone_low, zone_high,
                                       touch_tolerance_pct=None):
    """
    Path C trigger: at least one of last 6 × 30m bars wicked INTO the zone,
    AND the most recent 30m close is strictly below zone_low.
    """
    if touch_tolerance_pct is None:
        touch_tolerance_pct = PATH_C_TOUCH_TOLERANCE_PCT

    candles = _fetch_closed_30m_candles(symbol, 6)
    if not candles:
        return None

    last = candles[-1]
    if float(last["close"]) >= zone_low:
        return None

    touch_ceiling = zone_high * (1 + touch_tolerance_pct / 100.0)
    touch_floor   = zone_low  * (1 - touch_tolerance_pct / 100.0)
    for c in candles:
        c_high = float(c["high"])
        if touch_floor <= c_high <= touch_ceiling:
            return last
    return None


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
# PLACE SHORT ORDER  (no RR check)
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, entry_path):
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

    send_telegram(
        f"🔴 <b>NEW SHORT ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry  : <code>{entry}</code>\n"
        f"🎯 TP     : <code>{tp}</code>  (-{tp_pct_display}%)\n"
        f"🛑 SL     : <code>{sl}</code>  (+{sl_pct_display}%)\n"
        f"📦 Qty    : <code>{qty}</code>\n"
        f"💰 Margin : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    candles_needed = EMA_PERIOD + 30
    candles = fetch_candles(symbol, candles_needed)

    if len(candles) < EMA_PERIOD + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    # Drop in-progress 4h bar
    if len(candles) >= 2:
        now_ms = int(time.time() * 1000)
        last_candle_time = int(candles[-1]["time"])
        bar_elapsed_ms = now_ms - last_candle_time
        if bar_elapsed_ms < CANDLE_SECONDS * 1000:
            candles = candles[:-1]

    if len(candles) < EMA_PERIOD + 5:
        print(f"[SKIP] {symbol} — insufficient closed candles ({len(candles)})")
        return

    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    highs      = [float(c["high"])  for c in candles]
    last_close = closes[-1]
    last_high  = highs[-1]
    last_ts    = int(candles[-1]["time"])

    ema_values = compute_ema(closes, EMA_PERIOD)
    if ema_values[-1] is None:
        print(f"[SKIP] {symbol} — EMA not ready")
        return
    ema_now = ema_values[-1]

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
            st["tp_level"]    = round(entry_px * (1 - TP_PCT / 100), precision)
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
        all_state[symbol] = init_symbol_state()
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # PATH C — RESISTANCE REJECTION
    # =========================================================================
    ema_diff_pct = ((last_close - ema_now) / ema_now * 100.0) if ema_now else 0
    print(
        f"[SCAN] {symbol} | close={last_close} ema200={round(ema_now, precision)} | "
        f"emaDiff={round(ema_diff_pct, 2)}% | armed={st.get('path_c_armed', False)}"
    )

    # ── STAGE 1: already armed → watch for rejection ─────────────────────────
    if st.get("path_c_armed"):
        zone_low    = st.get("path_c_zone_low")
        zone_high   = st.get("path_c_zone_high")
        zone_center = st.get("path_c_zone_center")
        armed_ts    = st.get("path_c_start_ts")

        if None in (zone_low, zone_high, zone_center, armed_ts):
            print(f"[PATH-C] {symbol} — incomplete armed state, clearing")
            all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

        bars_waiting = max(0, int((last_ts - armed_ts) // (CANDLE_SECONDS * 1000)))
        broken_threshold = zone_high * (1 + 2.0 / 100.0)

        # Mark "touched"
        if not st.get("path_c_zone_touched"):
            if last_high >= zone_low * (1 - PATH_C_TOUCH_TOLERANCE_PCT / 100.0) \
               and last_high <= zone_high * (1 + PATH_C_TOUCH_TOLERANCE_PCT / 100.0):
                st["path_c_zone_touched"] = True
                print(f"[PATH-C] {symbol} — zone TOUCHED at high {last_high} ({zone_low}–{zone_high})")

        # Zone broken
        if last_close > broken_threshold:
            print(f"[PATH-C] {symbol} — zone BROKEN (close {last_close} > {broken_threshold:.6f})")
            send_telegram(
                f"❌ <b>PATH C CANCELLED — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 Close : <code>{last_close}</code>\n"
                f"🧱 Zone  : <code>{round(zone_low, precision)} – {round(zone_high, precision)}</code>\n"
                f"⚠️ Reason: zone broken (price &gt;2% above zone_high)"
            )
            all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

        # Timeout
        if bars_waiting > PATH_C_MAX_WAIT_BARS:
            print(f"[PATH-C] {symbol} — wait timed out ({bars_waiting} > {PATH_C_MAX_WAIT_BARS})")
            all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

        # Rejection confirmed on 30m close
        confirm_bar = confirm_30m_touch_and_close_below(symbol, zone_low, zone_high)
        if confirm_bar is not None:
            entry_price = float(confirm_bar["close"])
            print(f"[PATH-C] {symbol} — REJECTION CONFIRMED on 30m close {entry_price} < zone_low {zone_low}")

            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            tp_price = entry_price * (1 - TP_PCT / 100)
            sl_price = zone_high * (1 + PATH_C_SL_ABOVE_ZONE_PCT / 100)

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

        touched_str = "touched" if st.get("path_c_zone_touched") else "awaiting touch"
        print(f"[PATH-C] {symbol} — waiting ({bars_waiting}/{PATH_C_MAX_WAIT_BARS}b, {touched_str}), "
              f"zone {round(zone_low, precision)}–{round(zone_high, precision)}")
        save_state(all_state)
        return

    # ── STAGE 2: not armed → evaluate triggers ───────────────────────────────
    below_ema_proximity = (-PATH_C_BELOW_EMA_PROXIMITY_PCT) <= ema_diff_pct < 0
    above_ema_extended  = ema_diff_pct >= PATH_C_ABOVE_EMA_EXTENDED_PCT

    if not (below_ema_proximity or above_ema_extended):
        save_state(all_state)
        return

    # Determine zone search bounds
    if below_ema_proximity:
        # resistance must lie BETWEEN current price and EMA
        max_price   = ema_now
        trigger_str = f"below EMA within {PATH_C_BELOW_EMA_PROXIMITY_PCT}% (zone must be &lt; EMA)"
    else:
        max_price   = None
        trigger_str = f"above EMA ≥{PATH_C_ABOVE_EMA_EXTENDED_PCT}%"

    zone = find_nearest_resistance_zone_above(symbol, last_close, max_price=max_price)

    if zone is None or zone["low"] <= last_close:
        save_state(all_state)
        return

    tf_count   = len(zone["tfs"])
    tfs_str    = ",".join(sorted(zone["tfs"]))
    zone_low   = zone["low"]
    zone_high  = zone["high"]
    zone_cent  = zone["center"]
    dist_pct   = (zone_cent - last_close) / last_close * 100.0

    print(
        f"[PATH-C] {symbol} — ARMING zone {round(zone_low, precision)}–{round(zone_high, precision)} "
        f"(center {round(zone_cent, precision)}, {tf_count} TFs: {tfs_str}, "
        f"{round(dist_pct, 2)}% above close, trigger: {trigger_str})"
    )

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
        f"📊 EMA200     : <code>{round(ema_now, precision)}</code>\n"
        f"📏 EMA diff   : <code>{round(ema_diff_pct, 2)}%</code>\n"
        f"🧱 Zone low   : <code>{round(zone_low, precision)}</code>\n"
        f"🧱 Zone high  : <code>{round(zone_high, precision)}</code>\n"
        f"📊 Zone cent  : <code>{round(zone_cent, precision)}</code>\n"
        f"⏫ Dist above : <code>{round(dist_pct, 2)}%</code>\n"
        f"⚙️ Trigger    : <code>{trigger_str}</code>\n"
        f"🪢 Confluence : <code>{tf_count}/{len(PATH_C_ENABLED_TIMEFRAMES)} TFs ({tfs_str})</code>\n"
        f"⌛ Waiting up to {PATH_C_MAX_WAIT_BARS} × 4h bars for rejection"
    )
    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>SHORT Bot Started — Path C only</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy   : <code>200 EMA Resistance Rejection (Path C)</code>\n"
    f"⏱ Analysis   : <code>4h primary (closed bars only)</code>\n"
    f"⚡ Entry      : <code>30m close below zone_low</code>\n"
    f"🔁 Scan       : <code>Every 30 minutes</code>\n"
    f"🅰️ Trigger A : <code>close ≥ +{PATH_C_ABOVE_EMA_EXTENDED_PCT}% above EMA → any resistance above price</code>\n"
    f"🅱️ Trigger B : <code>close within -{PATH_C_BELOW_EMA_PROXIMITY_PCT}% of EMA → resistance between price &amp; EMA</code>\n"
    f"🧱 Pivots     : <code>N={PIVOT_STRENGTH} each side, ±{PIVOT_ZONE_PCT}% zone band</code>\n"
    f"🪢 TFs        : <code>4h + 12h synth + 1D + 3D synth (≥{MIN_TF_CONFLUENCE}/4 confluence)</code>\n"
    f"🎯 TP         : <code>{TP_PCT}% fixed below entry</code>\n"
    f"🛑 SL         : <code>zone_high × (1 + {PATH_C_SL_ABOVE_ZONE_PCT}%)</code>\n"
    f"⏳ Max wait   : <code>{PATH_C_MAX_WAIT_BARS} × 4h bars</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
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