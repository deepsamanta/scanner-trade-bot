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
# STRATEGY: Strict 1-Day Compression + Rolling 1H Breakout
#
# TIMEFRAME   : 1m candles (entry) + 1D candles (compression filter)
#
# STEP 1 — DAILY COMPRESSION FILTER (yesterday's completed daily candle):
#   • body_pct  = abs(close - open) / open * 100  ≤ MAX_DAILY_BODY_PCT (5%)
#   • range_pct = (high - low)      / open * 100  ≤ MAX_DAILY_RANGE_PCT (5%)
#   Both must pass — ensures entry only after a tight consolidation day.
#
# STEP 2 — ROLLING PUMP CHECK (on last completed 1m candle):
#   • move_15m = (close - open[14])  / open[14]  * 100   vs bar 15 back
#   • move_30m = (close - open[29])  / open[29]  * 100   vs bar 30 back
#   • move_45m = (close - open[44])  / open[44]  * 100   vs bar 45 back
#   • move_1h  = (close - open[59])  / open[59]  * 100   vs bar 60 back
#   ANY window >= MIN_PUMP_PCT (3%) triggers.
#
# STEP 3 — CANDLE QUALITY FILTERS:
#   • close > open                              (bullish candle)
#   • (close - low) / (high - low) >= 0.70     (strong close — top 70% of range)
#   • volume > EMA(volume, 20)                  (above-average volume)
#
# ENTRY  : Limit order at trigger candle close (long only)
# TP     : rounded_entry * (1 + TP_PCT / 100)  — computed from rounded entry
# SL     : rounded_entry * (1 - SL_PCT / 100)  — always 1.5% below entry
# =============================================================================

MAX_DAILY_BODY_PCT  = 5.0   # yesterday's body must be within this %
MAX_DAILY_RANGE_PCT = 7.0   # yesterday's wick range must be within this %
MIN_PUMP_PCT        = 2.1   # minimum move over any rolling window to trigger
TP_PCT              = 1.5   # fixed TP above entry close
SL_PCT              = 1.5   # fallback SL below entry (if candle low >= entry)

STRONG_CLOSE_RATIO  = 0.70  # close must be in top 70% of candle range
VOLUME_EMA_LEN      = 20    # EMA period for volume filter
NEAR_BOTTOM_PCT     = 100.0 # current price must be within 100% above 1000d low (new bottom)

CANDLES_DAILY  = 1000
CANDLES_ENTRY  = 120   # 90 bars (1.5h lookback) + 20 EMA seed + 10 buffer for in-progress drop
CANDLES_1M     = 5

RESOLUTION_DAILY = "1D"
RESOLUTION_ENTRY = "1"
RESOLUTION_1M    = "1"

CANDLE_SECONDS_DAY   = 86400
CANDLE_SECONDS_ENTRY = 60
CANDLE_SECONDS_1M    = 60

SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "compression_bot_state.json"


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
        "tp_completed":    False,   # NEW: day-level TP completed flag
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
# STRATEGY FUNCTIONS
# =====================================================

def compute_ema(values, length):
    """Standard EMA. Returns None if insufficient data."""
    if len(values) < length:
        return None
    k   = 2 / (length + 1)
    ema = sum(values[:length]) / length   # SMA seed
    for v in values[length:]:
        ema = v * k + ema * (1 - k)
    return ema


def check_daily_compression(daily_candles):
    """
    Evaluates yesterday's completed daily candle for tight compression.
    Returns (valid: bool, body_pct: float, range_pct: float).
    """
    if len(daily_candles) < 1:
        return False, 0.0, 0.0

    yest   = daily_candles[-1]
    o      = float(yest["open"])
    c      = float(yest["close"])
    h      = float(yest["high"])
    l      = float(yest["low"])

    if o == 0:
        return False, 0.0, 0.0

    body_pct  = abs(c - o) / o * 100
    range_pct = (h - l)   / o * 100

    valid = body_pct <= MAX_DAILY_BODY_PCT and range_pct <= MAX_DAILY_RANGE_PCT
    return valid, round(body_pct, 2), round(range_pct, 2)


def check_near_bottom(daily_candles, current_price):
    """
    Checks if current price is within NEAR_BOTTOM_PCT% above the 1000-day low,
    or is making a new bottom.
    Returns (valid: bool, low_1000d: float, pct_above_low: float).
    """
    if not daily_candles:
        return False, 0.0, 0.0

    low_1000d = min(float(c["low"]) for c in daily_candles)
    if low_1000d == 0:
        return False, 0.0, 0.0

    pct_above_low = (current_price - low_1000d) / low_1000d * 100
    valid = pct_above_low <= NEAR_BOTTOM_PCT   # also catches new bottom (pct <= 0)
    return valid, round(low_1000d, 8), round(pct_above_low, 2)



    """
    Checks if current 1m close has moved >= MIN_PUMP_PCT over any of the four
    rolling open anchors at 15, 30, 45, 60 bars back (1m resolution).
    Returns (triggered: bool, best_move_pct: float, window_label: str).
    """
    if len(candles_1m) < 61:
        return False, 0.0, ""

    curr_c = float(candles_1m[-1]["close"])

    windows = [
        ("15m", float(candles_1m[-15]["open"])),
        ("30m", float(candles_1m[-30]["open"])),
        ("45m", float(candles_1m[-45]["open"])),
        ("1h",  float(candles_1m[-60]["open"])),
    ]

    best_label = ""
    best_move  = 0.0
    triggered  = False

    for label, anchor_open in windows:
        if anchor_open == 0:
            continue
        move = (curr_c - anchor_open) / anchor_open * 100
        if move > best_move:
            best_move  = move
            best_label = label
        if move >= MIN_PUMP_PCT:
            triggered = True

    return triggered, round(best_move, 2), best_label


# =====================================================
# CANDLE FETCHER
# =====================================================

def check_rolling_pump(candles_1m):
    """
    Checks if current 1m close has moved >= MIN_PUMP_PCT over any of the
    5min-interval anchors from 5m to 1.5h (5, 10, 15 ... 90 bars back).
    Returns (triggered: bool, best_move_pct: float, window_label: str).
    """
    if len(candles_1m) < 91:
        return False, 0.0, ""

    curr_c = float(candles_1m[-1]["close"])

    best_label = ""
    best_move  = 0.0
    triggered  = False

    for bars in range(5, 91, 5):
        anchor_open = float(candles_1m[-bars]["open"])
        if anchor_open == 0:
            continue
        move = (curr_c - anchor_open) / anchor_open * 100
        if move > best_move:
            best_move  = move
            best_label = f"{bars}m"
        if move >= MIN_PUMP_PCT:
            triggered = True

    return triggered, round(best_move, 2), best_label


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
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)

    # FIX: verify TP is strictly derived from rounded entry to avoid exchange mismatch
    expected_tp = round(entry * (1 + TP_PCT / 100), precision)
    if tp != expected_tp:
        print(f"  [WARN] TP mismatch corrected: {tp} -> {expected_tp}")
        tp = expected_tp

    tp_pct = round(((tp - entry) / entry) * 100, 2)
    sl_pct = round(((entry - sl) / entry) * 100, 2)

    print(f"  [LONG] Entry={entry} TP={tp}(+{tp_pct}%) SL={sl}(-{sl_pct}%) Qty={qty}")

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
        f"🟢 <b>NEW LONG (COMPRESSION) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    # Return the exact rounded values used so state stores the same numbers
    return True, entry, tp


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch candles ──────────────────────────────────────────────────
    daily_all = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
    # Separate: completed bars for compression, all bars for 1000d low
    daily_completed = daily_all[:-1] if (
        daily_all and (now_ms - int(daily_all[-1]["time"])) < CANDLE_SECONDS_DAY * 1000
    ) else daily_all

    if not daily_all:
        print(f"  [{symbol}] SKIP — no daily candles at all")
        return

    candles_1m = fetch_candles(symbol, CANDLES_ENTRY, RESOLUTION_ENTRY, CANDLE_SECONDS_ENTRY)
    if candles_1m and (now_ms - int(candles_1m[-1]["time"])) < CANDLE_SECONDS_ENTRY * 1000:
        candles_1m = candles_1m[:-1]

    min_1m = VOLUME_EMA_LEN + 91
    if len(candles_1m) < min_1m:
        print(f"  [{symbol}] SKIP — insufficient 1m candles ({len(candles_1m)} < {min_1m})")
        return

    # ── 2. State init / backfill ──────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset ──────────────────────────────────────────────────
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — resetting daily state")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    precision = get_precision(float(candles_1m[-1]["close"]))

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────
    # Check BOTH sheet and in-state flag — either blocks trading for today
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""

    if tp_raw.upper() == "TP COMPLETED" or st.get("tp_completed") is True:
        print(f"  [{symbol}] SKIP — TP COMPLETED (sheet={tp_raw.upper() == 'TP COMPLETED'} "
              f"state={st.get('tp_completed')})")
        # Ensure state is cleared so no ghost in_position
        if st.get("in_position"):
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
        return

    # ── 5. Resolve TP target from state (authoritative) then sheet fallback ──
    # State is always written with the exact rounded value used in the order,
    # so it is the only reliable source. Sheet is only used to backfill when
    # state is missing (e.g. bot restarted mid-trade).
    tp_stored = st.get("tp_level")
    if not tp_stored:
        try:
            v = float(tp_raw)
            if v > 0:
                tp_stored    = v
                st["tp_level"] = v   # backfill state from sheet
        except (ValueError, TypeError):
            tp_stored = None

    if tp_stored and tp_stored > 0:
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit     = False
        hit_kind   = None
        hit_price  = None

        # Small tolerance (0.01%) to handle float comparison edge cases
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
            all_state[symbol]["tp_completed"]    = True   # block rest of day
            save_state(all_state)
            return

    # ── 6. Reconcile with exchange ────────────────────────────────────────
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
        return

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
        return

    # ── 7. Daily compression filter ───────────────────────────────────────
    if len(daily_completed) < 1:
        print(f"  [{symbol}] SKIP — no completed daily candle")
        save_state(all_state)
        return

    compression_ok, body_pct, range_pct = check_daily_compression(daily_completed)
    print(f"  [{symbol}] Daily candles={len(daily_all)} body={body_pct}% range={range_pct}% "
          f"compression={'OK' if compression_ok else 'FAIL'}")

    if not compression_ok:
        save_state(all_state)
        return

    # ── 7b. Near-bottom filter ────────────────────────────────────────────
    current_price = float(candles_1m[-1]["close"])
    bottom_ok, low_1000d, pct_above = check_near_bottom(daily_all, current_price)
    print(f"  [{symbol}] 1000d_low={low_1000d} current={current_price} "
          f"pct_above_low={pct_above}% bottom={'OK' if bottom_ok else 'FAIL'}")

    if not bottom_ok:
        save_state(all_state)
        return

    # ── 8. Last completed 1m candle ───────────────────────────────────────
    curr   = candles_1m[-1]
    curr_o = float(curr["open"]);  curr_h = float(curr["high"])
    curr_l = float(curr["low"]);   curr_c = float(curr["close"])
    curr_ts = int(curr["time"])

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — candle already processed")
        save_state(all_state)
        return

    # ── 9. Rolling pump check ─────────────────────────────────────────────
    pump_ok, best_move, best_window = check_rolling_pump(candles_1m)

    # ── 10. Candle quality filters ────────────────────────────────────────
    cond_bullish = curr_c > curr_o

    candle_range = curr_h - curr_l
    cond_strong_close = (
        candle_range > 0 and
        (curr_c - curr_l) / candle_range >= STRONG_CLOSE_RATIO
    )

    volumes   = [float(c["volume"]) for c in candles_1m]
    vol_ema20 = compute_ema(volumes, VOLUME_EMA_LEN)
    cond_volume = vol_ema20 is not None and float(curr["volume"]) > vol_ema20

    print(f"  [{symbol}] pump={pump_ok}({best_move}% over {best_window}) "
          f"bullish={cond_bullish} strong_close={cond_strong_close} volume={cond_volume}")

    st["last_candle_ts"] = curr_ts

    if not (pump_ok and cond_bullish and cond_strong_close and cond_volume):
        save_state(all_state)
        return

    # ── 11. Compute entry / TP / SL ───────────────────────────────────────
    # FIX: round entry first, then compute TP from rounded entry.
    # This ensures the TP stored in state/sheet/exchange is exactly
    # rounded_entry * (1 + TP_PCT/100) — no accumulated rounding error.
    entry_price  = round(curr_c, precision)
    tp_price_val = round(entry_price * (1 + TP_PCT / 100), precision)
    sl_price_val = round(entry_price * (1 - SL_PCT / 100), precision)

    print(f"  [{symbol}] ENTRY — Entry={entry_price} "
          f"TP={tp_price_val} SL={round(sl_price_val, precision)}")

    # ── 12. Place order ───────────────────────────────────────────────────
    placed, confirmed_entry, confirmed_tp = place_long_order(
        symbol, entry_price, tp_price_val, sl_price_val, precision
    )

    if placed:
        # Use the exact values returned from place_long_order (post-rounding + correction)
        st["in_position"] = True
        st["direction"]   = "long"
        st["entry_price"] = confirmed_entry
        st["tp_level"]    = confirmed_tp          # exact value sent to exchange
        st["sl_price"]    = round(sl_price_val, precision)
        st["last_entry_ts"] = curr_ts
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Compression Breakout Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy      : <code>1-Day Compression + Rolling 1H Breakout</code>\n"
    f"\n"
    f"📊 Daily Filter  :\n"
    f"  <code>Body  ≤ {MAX_DAILY_BODY_PCT}%  (yesterday open→close)</code>\n"
    f"  <code>Range ≤ {MAX_DAILY_RANGE_PCT}%  (yesterday high−low)</code>\n"
    f"\n"
    f"⚡ Pump Windows  :\n"
    f"  <code>move_15m / 30m / 45m / 1h ≥ {MIN_PUMP_PCT}% (any triggers)</code>\n"
    f"\n"
    f"🔍 Quality Filters:\n"
    f"  <code>① Bullish candle (close > open)</code>\n"
    f"  <code>② Strong close (top {int(STRONG_CLOSE_RATIO*100)}% of range)</code>\n"
    f"  <code>③ Volume > EMA({VOLUME_EMA_LEN}) of volume</code>\n"
    f"\n"
    f"🎯 TP            : <code>+{TP_PCT}% above entry close</code>\n"
    f"🛑 SL            : <code>-{SL_PCT}% below entry (fixed)</code>\n"
    f"⏱ Timeframe     : <code>1m</code>\n"
    f"🔁 Scan          : <code>Every {SCAN_INTERVAL}s</code>\n"
    f"💰 Capital       : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
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

        print(f"\n===== CYCLE {cycle} | {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} "
              f"| positions={len(global_positions)} orders={len(global_orders)} =====")

        symbols_checked = 0
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if not symbol:
                continue
            symbols_checked += 1
            print(f"--- Row {row + 1}: {symbol} ---")
            try:
                check_and_trade(symbol, row, df, state, global_positions, global_orders)
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")
                continue

        print(f"===== CYCLE {cycle} DONE — {symbols_checked} symbols =====")
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