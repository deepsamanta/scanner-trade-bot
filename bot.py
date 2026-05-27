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
# STRATEGY: Rizzy Bottom Reversal (Body Length Projection)
#
# TIMEFRAME   : 15m candles
#
# SETUP:
#   1. Two most recent confirmed pivot highs (PIVOT_LEN bars each side) →
#      structural descending trendline
#   2. Most recent confirmed pivot low
#   3. rizzy_depth   = trendline_value_at_pivot_low_bar − pivot_low
#   4. projected_target = pivot_low − rizzy_depth
#
# ENTRY (LONG ONLY — limit order at trigger candle close):
#   ① low  ≤ projected_target   — price hit the structural projection
#   ② low  <  BB lower band     — detached from reality (outside BB)
#   ③ close > high[prev candle] — breakout above prior bar's high
#   ④ close > open              — bullish candle confirmation
#
# EXIT:
#   TP : BB middle band (SMA 20) at time of entry — "return to reality"
#   SL : trigger candle's low
#      Fallback: TP_PCT above entry if basis ≤ entry / SL_PCT below entry if low ≥ entry
#
# BB PARAMS   : Length=20, StdDev=2.0
# PIVOT PARAMS: PIVOT_LEN=5 (bars required on each side for confirmation)
# =============================================================================

TP_PCT    = 3.0    # fallback TP % when BB basis ≤ entry price
SL_PCT    = 1.5    # fallback SL % when candle low ≥ entry price

BB_LENGTH = 20
BB_MULT   = 2.0
PIVOT_LEN = 5      # bars on each side required to confirm a structural pivot

CANDLES_15M     = 100
CANDLES_1M      = 5

RESOLUTION_15M   = "15"
RESOLUTION_1M    = "1"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1M  = 60

SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "rizzy_bot_state.json"


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
# RIZZY STRATEGY FUNCTIONS
# =====================================================

def compute_bb(closes):
    """
    Bollinger Bands from a list of closes.
    Returns (basis, upper, lower) or (None, None, None) if insufficient data.
    """
    if len(closes) < BB_LENGTH:
        return None, None, None
    recent   = closes[-BB_LENGTH:]
    basis    = sum(recent) / BB_LENGTH
    variance = sum((x - basis) ** 2 for x in recent) / BB_LENGTH
    std      = variance ** 0.5
    return basis, basis + BB_MULT * std, basis - BB_MULT * std


def find_pivots(candles):
    """
    Identify the two most recent confirmed structural pivot highs and the most
    recent confirmed pivot low. Confirmation requires PIVOT_LEN bars on each side.

    Returns:
        ph1, ph1_idx  — most recent pivot high (price, candle list index)
        ph2, ph2_idx  — second most recent pivot high
        pl,  pl_idx   — most recent pivot low
        (any field may be None if not found)
    """
    n  = len(candles)
    lb = PIVOT_LEN

    pivot_highs = []   # [(index, price)]
    pivot_lows  = []

    for i in range(lb, n - lb):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])

        if all(h >= float(candles[i + j]["high"]) for j in range(-lb, lb + 1) if j != 0):
            pivot_highs.append((i, h))

        if all(l <= float(candles[i + j]["low"])  for j in range(-lb, lb + 1) if j != 0):
            pivot_lows.append((i, l))

    ph1 = ph2 = pl = None
    ph1_idx = ph2_idx = pl_idx = None

    if len(pivot_highs) >= 2:
        ph2_idx, ph2 = pivot_highs[-2]
        ph1_idx, ph1 = pivot_highs[-1]
    elif len(pivot_highs) == 1:
        ph1_idx, ph1 = pivot_highs[-1]

    if pivot_lows:
        pl_idx, pl = pivot_lows[-1]

    return ph1, ph1_idx, ph2, ph2_idx, pl, pl_idx


def compute_projected_target(ph1, ph1_idx, ph2, ph2_idx, pl, pl_idx):
    """
    Rizzy projection:
      trendline drawn through ph2 → ph1 (two confirmed pivot highs)
      rizzy_depth     = trendline_value_at_pl_bar − pl
      projected_target = pl − rizzy_depth

    Returns float or None when the geometric setup is invalid.
    """
    if any(v is None for v in (ph1, ph1_idx, ph2, ph2_idx, pl, pl_idx)):
        return None
    if ph1_idx <= ph2_idx:
        return None
    if pl_idx <= ph2_idx:
        return None

    slope        = (ph1 - ph2) / (ph1_idx - ph2_idx)
    tl_at_pl_bar = ph1 + slope * (pl_idx - ph1_idx)
    rizzy_depth  = tl_at_pl_bar - pl

    if rizzy_depth <= 0:
        return None   # trendline not above pivot low — invalid setup

    return pl - rizzy_depth


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
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
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
        return False

    print(f"  [API] {symbol}: {result}")
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False

    send_telegram(
        f"🟢 <b>NEW LONG (RIZZY) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch 15m candles ──────────────────────────────────────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    # Drop in-progress bar
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]

    min_required = BB_LENGTH + PIVOT_LEN * 2 + 2
    if len(candles_15m) < min_required:
        print(f"  [{symbol}] SKIP — insufficient 15m candles ({len(candles_15m)} < {min_required})")
        return

    # ── 2. State init / backfill ──────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset ──────────────────────────────────────────────────
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    precision = get_precision(float(candles_15m[-1]["close"]))

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
    if tp_raw.upper() == "TP COMPLETED":
        print(f"  [{symbol}] SKIP — TP COMPLETED in sheet")
        save_state(all_state)
        return

    try:
        tp_stored = float(tp_raw)
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored and tp_stored > 0:
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit     = False
        hit_kind   = None
        hit_price  = None

        if last_close and last_close >= tp_stored:
            tp_hit, hit_kind, hit_price = True, "close", last_close
        if not tp_hit:
            rh = get_recent_high(symbol)
            if rh and rh >= tp_stored:
                tp_hit, hit_kind, hit_price = True, "wick", rh

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"  [{symbol}] TP HIT ({hit_kind}) price={hit_price} target={tp_stored}")
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"] = prev_last
            save_state(all_state)
            return

    # ── 5. Reconcile with exchange ────────────────────────────────────────
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

    # ── 6. Bollinger Bands ────────────────────────────────────────────────
    closes = [float(c["close"]) for c in candles_15m]
    basis, upper_band, lower_band = compute_bb(closes)
    if basis is None:
        print(f"  [{symbol}] SKIP — insufficient data for BB")
        save_state(all_state)
        return

    # ── 7. Structural pivots + Rizzy projection ───────────────────────────
    ph1, ph1_idx, ph2, ph2_idx, pl, pl_idx = find_pivots(candles_15m)
    projected_target = compute_projected_target(ph1, ph1_idx, ph2, ph2_idx, pl, pl_idx)

    if projected_target is None:
        print(f"  [{symbol}] SKIP — no valid Rizzy projection "
              f"(ph1={ph1}, ph2={ph2}, pl={pl})")
        save_state(all_state)
        return

    # ── 8. Entry conditions on last completed candle ──────────────────────
    curr = candles_15m[-1]
    prev = candles_15m[-2]

    curr_o = float(curr["open"]);  curr_h = float(curr["high"])
    curr_l = float(curr["low"]);   curr_c = float(curr["close"])
    prev_h = float(prev["high"])
    curr_ts = int(curr["time"])

    print(f"  [{symbol}] BB basis={round(basis, precision)} lower={round(lower_band, precision)} | "
          f"proj={round(projected_target, precision)} | "
          f"L={round(curr_l, precision)} C={round(curr_c, precision)} prevH={round(prev_h, precision)}")

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — candle already processed")
        save_state(all_state)
        return

    cond_target   = curr_l <= projected_target
    cond_reality  = curr_l < lower_band
    cond_breakout = curr_c > prev_h
    cond_bullish  = curr_c > curr_o

    print(f"  [{symbol}] ① target={cond_target} ② outside_BB={cond_reality} "
          f"③ breakout={cond_breakout} ④ bullish={cond_bullish}")

    st["last_candle_ts"] = curr_ts

    if not (cond_target and cond_reality and cond_breakout and cond_bullish):
        save_state(all_state)
        return

    # ── 9. Compute entry / TP / SL ────────────────────────────────────────
    entry_price  = curr_c
    # TP: BB basis (return to reality). Fallback to fixed % if basis ≤ entry.
    tp_price_val = basis if basis > entry_price else entry_price * (1 + TP_PCT / 100)
    # SL: trigger candle low. Fallback to fixed % if low ≥ entry.
    sl_price_val = curr_l if curr_l < entry_price else entry_price * (1 - SL_PCT / 100)

    print(f"  [{symbol}] RIZZY ENTRY — Entry={round(entry_price, precision)} "
          f"TP={round(tp_price_val, precision)} SL={round(sl_price_val, precision)}")

    # ── 10. Place order ───────────────────────────────────────────────────
    placed = place_long_order(symbol, entry_price, tp_price_val, sl_price_val, precision)

    if placed:
        st["in_position"] = True
        st["direction"]   = "long"
        st["entry_price"] = round(entry_price,  precision)
        st["tp_level"]    = round(tp_price_val, precision)
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
    f"✅ <b>Rizzy Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy     : <code>Rizzy Bottom Reversal (Body Length Projection)</code>\n"
    f"\n"
    f"📊 Setup        :\n"
    f"  <code>2 pivot highs → structural trendline</code>\n"
    f"  <code>rizzy_depth = trendline_at_low_bar − pivot_low</code>\n"
    f"  <code>projected_target = pivot_low − rizzy_depth</code>\n"
    f"\n"
    f"🔍 Entry (LONG) :\n"
    f"  <code>① Low ≤ projected target</code>\n"
    f"  <code>② Low below BB lower band (20, 2.0)</code>\n"
    f"  <code>③ Close > prior candle high (breakout)</code>\n"
    f"  <code>④ Close > Open (bullish candle)</code>\n"
    f"\n"
    f"🎯 TP           : <code>BB middle band (SMA 20)</code>\n"
    f"🛑 SL           : <code>Trigger candle low</code>\n"
    f"⏱ Timeframe    : <code>15m</code>\n"
    f"🔁 Scan         : <code>Every {SCAN_INTERVAL}s</code>\n"
    f"💰 Capital      : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
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