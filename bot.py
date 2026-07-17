
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
# STRATEGY: Institutional Quant Trend + Volatility Expansion Engine
#
# Scaled to handle the entire CoinDCX asset list cleanly by filtering out noise.
#
# STEP 1 — PROGRAMMATIC LIQUIDITY FILTER:
#   • Computes rolling 24-hour dollar volume dynamically.
#   • Automatically discards assets with < MIN_DAILY_VOL_USDT to prevent slippage.
#
# STEP 2 — MULTI-TIMEFRAME TREND & MOMENTUM ALIGNMENT:
#   • 1H HTF Trend Check: 1H Close must be above the 50 EMA.
#   • 15m Momentum Check: 15m Fast EMA (20) must be above the Slow EMA (50).
#   • Price Position: 15m Close must be above the 20 EMA.
#
# STEP 3 — INSTITUTIONAL VOLUME SURGE CONFIRMATION:
#   • The trigger 15m candle's volume must be >= 1.5x the average volume of the
#     preceding 20 candles. Ensures active institutional capital participation.
#
# STEP 4 — DYNAMIC RISK RISK SIZING (ATR):
#   • Computes 14-period Average True Range (ATR) on the 15m timeframe.
#   • Dynamic SL = Entry - (2.0 * ATR)
#   • Dynamic TP = Entry + (4.0 * ATR) -> Structural 2:1 Reward-to-Risk ratio.
# =============================================================================

ATR_LEN             = 14      # Period for ATR calculation
SL_ATR_MULT         = 2.0     # Distance multiplier for Stop Loss
TP_ATR_MULT         = 4.0     # Distance multiplier for Take Profit (2:1 RRR)
EMA_FAST            = 20      # Fast momentum window
EMA_SLOW            = 50      # Trend structure window
MIN_DAILY_VOL_USDT  = 2000000  # $2M minimum 24h rolling volume filter

# Allocation buffers for indicators
CANDLES_15M        = 200
CANDLES_1M         = 5
CANDLES_1H         = 100

RESOLUTION_15M     = "15"
RESOLUTION_1M      = "1"
RESOLUTION_DAILY   = "1D"
RESOLUTION_1H      = "60"

CANDLE_SECONDS_15M   = 900
CANDLE_SECONDS_1M    = 60
CANDLE_SECONDS_DAY   = 86400
CANDLE_SECONDS_1H    = 3600

CANDLES_DAILY          = 1000
SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "quant_bot_state.json"


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
# QUANT STRATEGY SYSTEM CORE FUNCTIONS
# =====================================================

def compute_ema(values, length):
    """Standard exponential moving average calculation loop."""
    if len(values) < length:
        return None
    k   = 2 / (length + 1)
    ema = sum(values[:length]) / length   # SMA baseline seed
    for v in values[length:]:
        ema = v * k + ema * (1 - k)
    return ema


def compute_atr(candles, length=14):
    """Computes standard rolling Average True Range (ATR) on raw candle series."""
    if len(candles) < length + 1:
        return 0.0
    tr_values = []
    for i in range(1, len(candles)):
        high = float(candles[i]["high"])
        low = float(candles[i]["low"])
        prev_close = float(candles[i-1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)
    if len(tr_values) < length:
        return 0.0
    return sum(tr_values[-length:]) / length


def check_market_filters(symbol, candles_15m, candles_1h):
    """
    Core Strategic Engine Filters.
    Returns: (passed: bool, reason: str, est_daily_vol: float)
    """
    # Safeguard allocation lengths
    if len(candles_15m) < max(EMA_SLOW, ATR_LEN + 1) or len(candles_1h) < EMA_SLOW:
        return False, "Insufficient Candles Data", 0.0

    # 1. Stage 1: Liquidity Filter (Approximate 24h rolling volume window in USDT)
    # 96 candles of 15m = 24 hours
    recent_24h = candles_15m[-96:] if len(candles_15m) >= 96 else candles_15m
    est_daily_volume = sum(float(c["volume"]) * float(c["close"]) for c in recent_24h)
    if est_daily_volume < MIN_DAILY_VOL_USDT:
        return False, f"Low Vol Filter (${round(est_daily_volume, 2)})", est_daily_volume

    # 2. Stage 2: Higher Timeframe Trend Alignment (1H Structural Bullishness)
    h1_closes = [float(c["close"]) for c in candles_1h]
    h1_ema50 = compute_ema(h1_closes, EMA_SLOW)
    if h1_ema50 is None or h1_closes[-1] <= h1_ema50:
        return False, "Bearish 1H Structure (Close <= 1H EMA50)", est_daily_volume

    # 3. Stage 3: Short Timeframe Momentum Convergence (15m Alignment)
    m15_closes = [float(c["close"]) for c in candles_15m]
    m15_ema20 = compute_ema(m15_closes, EMA_FAST)
    m15_ema50 = compute_ema(m15_closes, EMA_SLOW)
    
    if m15_ema20 is None or m15_ema50 is None:
        return False, "Processing Error Calculation", est_daily_volume
        
    if m15_ema20 <= m15_ema50:
        return False, "Negative Momentum Alignment (15m EMA20 <= EMA50)", est_daily_volume
        
    if m15_closes[-1] <= m15_ema20:
        return False, "Below Execution Base (15m Close <= EMA20)", est_daily_volume

    # 4. Stage 4: Volume Surge Acceleration Check (Institutional Footprint)
    m15_vols = [float(c["volume"]) for c in candles_15m]
    # Calculate average volume of the previous 20 candles before the current closed trigger candle
    avg_vol_20 = sum(m15_vols[-21:-1]) / 20 if len(m15_vols) >= 21 else sum(m15_vols[:-1]) / len(m15_vols[:-1])
    current_vol = m15_vols[-1]
    
    if avg_vol_20 > 0 and current_vol < (avg_vol_20 * 1.5):
        return False, f"Missing Vol Surge ({round(current_vol / avg_vol_20, 2)}x / 1.5x)", est_daily_volume

    return True, "All Engine Strategies Confirmed", est_daily_volume


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
        return False, None, None

    print(f"  [API] {symbol}: {result}")
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None

    send_telegram(
        f"🟢 <b>NEW QUANT LONG POSITION — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP (Dynamic ATR): <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL (Dynamic ATR): <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True, entry, tp


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch candles ──────────────────────────────────────────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    candles_1h  = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)

    # Drop the in-progress candles — only use fully closed bars
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]
    if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
        candles_1h = candles_1h[:-1]

    if not candles_15m or not candles_1h:
        print(f"  [{symbol}] SKIP — Could not parse historical context timelines.")
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
    precision = get_precision(float(candles_15m[-1]["close"]))

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""

    if tp_raw.upper() == "TP COMPLETED" or st.get("tp_completed") is True:
        print(f"  [{symbol}] SKIP — TP COMPLETED (sheet={tp_raw.upper() == 'TP COMPLETED'} "
              f"state={st.get('tp_completed')})")
        if st.get("in_position"):
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
        return

    # ── 5. Resolve TP target from state then sheet fallback ───────────────
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

    # ── 7. Last completed 15m candle — dedup guard ────────────────────────
    curr    = candles_15m[-1]
    curr_ts = int(curr["time"])
    curr_c  = float(curr["close"])

    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — 15m candle already processed")
        save_state(all_state)
        return

    # ── 8. Compute Engine Filters Execution ───────────────────────────────
    passed, reason, daily_vol = check_market_filters(symbol, candles_15m, candles_1h)
    print(f"  [{symbol}] Filter Status: {passed} | Reason: {reason} | Est 24h Vol: ${round(daily_vol, 2)}")

    st["last_candle_ts"] = curr_ts

    if not passed:
        save_state(all_state)
        return

    # ── 9. Compute Dynamic Entry / ATR TP / ATR SL ────────────────────────
    entry_price  = round(curr_c, precision)
    
    # Calculate True Volatility Space
    atr_val = compute_atr(candles_15m, ATR_LEN)
    if atr_val <= 0:
        print(f"  [{symbol}] SKIP — Invalid ATR calculated (${atr_val})")
        save_state(all_state)
        return

    tp_price_val = round(entry_price + (atr_val * TP_ATR_MULT), precision)
    sl_price_val = round(entry_price - (atr_val * SL_ATR_MULT), precision)

    # Protection Check: Stop Loss must not cross under zero or equal entry
    if sl_price_val <= 0 or sl_price_val >= entry_price or tp_price_val <= entry_price:
        print(f"  [{symbol}] SKIP — Mathematical anomaly in dynamic metrics computation.")
        save_state(all_state)
        return

    print(f"  [{symbol}] STRATEGY TRIGGER CONFIRMED — Entry={entry_price} TP={tp_price_val} SL={sl_price_val}")

    # ── 10. Place order ───────────────────────────────────────────────────
    placed, confirmed_entry, confirmed_tp = place_long_order(
        symbol, entry_price, tp_price_val, sl_price_val, precision
    )

    if placed:
        st["in_position"] = True
        st["direction"]   = "long"
        st["entry_price"] = confirmed_entry
        st["tp_level"]    = confirmed_tp
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
    f"🚀 <b>Institutional Multi-Strategy Quant Bot Active</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy      : <code>Trend Structure + Momentum Alignment + Volatility Breakout</code>\n"
    f"\n"
    f"📊 Quantitative Funnel Filters:\n"
    f"  <code>① Liquidity  : Minimum ${MIN_DAILY_VOL_USDT} USDT Rolling 24h Volume</code>\n"
    f"  <code>② HTF Trend  : 1H Close > 50 EMA Structural Support</code>\n"
    f"  <code>③ Shorter TF : 15m Momentum (EMA 20 > EMA 50) + Close > EMA 20</code>\n"
    f"  <code>④ Momentum   : Volume Surge Verification (Trigger Bar >= 1.5x of 20 MAs)</code>\n"
    f"\n"
    f"🎯 Dynamic Risk Setup (ATR Sized):\n"
    f"  <code>• Take Profit : Entry + ({TP_ATR_MULT} × ATR)</code>\n"
    f"  <code>• Stop Loss   : Entry - ({SL_ATR_MULT} × ATR) [2:1 Target Ratio]</code>\n"
    f"⏱ Execution Base : <code>15m Timeframe</code>\n"
    f"🔁 Scanning Delay: <code>Every {SCAN_INTERVAL} seconds</code>\n"
    f"💰 Exposure      : <code>{CAPITAL_USDT} USDT Allocation × {LEVERAGE}x Leverage</code>"
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

        print(f"===== CYCLE {cycle} DONE — {symbols_checked} symbols checked =====")
        save_state(state)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        consecutive_errors += 1
        print(f"BOT ERROR ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}")
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            send_telegram(
                f"🚨 <b>Bot Infrastructure Crash Alert</b>\n"
                f"❌ <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive failures recorded."
            )
            raise SystemExit(1)
        time.sleep(60)

