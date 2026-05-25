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
# STRATEGY PARAMETERS  (PDH/PDL Liquidity Sweep — LONG + SHORT)
# =============================================================================

TP_PCT          = 1.5    # fixed TP %
MIN_SL_PCT      = 0.5    # minimum SL distance from entry (%)
MIN_BODY_PCT    = 70     # body must be >= 70% of total candle range (high - low) for C2
SWEEP_EXPIRY_BARS = 12   # 5m bars before unresolved sweep expires (12 * 5m = 60 min)

CANDLES_DAILY   = 5
CANDLES_1M      = 300    # strictly used for tight TP wick detection
CANDLES_5M      = 100    # ~8.3 hours of 5m candles per scan for sweep logic
CANDLES_1H      = 30

RESOLUTION_DAILY   = "1D"
RESOLUTION_5M      = "5"
RESOLUTION_1M      = "1"
RESOLUTION_1H      = "60"

CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_5M  = 300
CANDLE_SECONDS_1M  = 60
CANDLE_SECONDS_1H  = 3600

SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "pdh_pdl_state.json"


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
        "in_position":   False,
        "direction":     None,
        "entry_price":   None,
        "tp_level":      None,
        "sl_price":      None,
        "last_entry_ts": 0,
        "current_day_str": None,
        "pdh":             None,
        "pdl":             None,
        "pdl_swept_1h": False,
        "pdh_swept_1h": False,
        "sweep_direction":   None,
        "sweep_ts":          0,
        "recent_swing_low":  None,   
        "recent_swing_high": None,   
        "sweep_o": None, "sweep_h": None,
        "sweep_l": None, "sweep_c": None,
        "candle1_ts": 0,
        "candle1_o":  None, "candle1_h": None,
        "candle1_l":  None, "candle1_c": None,
        "last_processed_5m_ts": 0,
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
                return
    except Exception as e:
        pass

# =====================================================
# SAFE EXCHANGE FETCHING (BULK FETCH)
# =====================================================

def get_all_positions():
    """Fetches ALL open positions once to prevent API rate limits."""
    try:
        body = {
            "timestamp": int(time.time() * 1000), 
            "page": "1", 
            "size": "100", 
            "margin_currency_short_name": ["USDT"]
        }
        payload, headers = sign_request(body)
        r = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/positions",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[API ERROR] Failed to fetch positions: HTTP {r.status_code} - {r.text}")
            return None # CRITICAL: Return None to prevent state wipe
            
        data = r.json()
        positions = data if isinstance(data, list) else data.get("data", [])
        
        if not isinstance(positions, list):
            print(f"[API ERROR] Unexpected position format: {data}")
            return None
            
        active_positions = []
        for p in positions:
            # Check multiple possible keys to ensure we catch the open quantity
            qty_str = str(p.get("size") or p.get("active_pos") or p.get("net_size") or "0")
            if abs(float(qty_str)) > 0:
                active_positions.append(p)
        return active_positions
    except Exception as e:
        print("[API ERROR] Exception in get_all_positions:", e)
        return None


def get_all_open_orders():
    """Fetches ALL open orders once to prevent API rate limits."""
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "status": "open,partially_filled", 
            "page": "1", 
            "size": "100",
            "margin_currency_short_name": ["USDT"]
        }
        payload, headers = sign_request(body)
        r = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[API ERROR] Failed to fetch orders: HTTP {r.status_code} - {r.text}")
            return None
            
        data = r.json()
        orders = data if isinstance(data, list) else data.get("data", [])
        
        if not isinstance(orders, list):
            print(f"[API ERROR] Unexpected orders format: {data}")
            return None
        return orders
    except Exception as e:
        print("[API ERROR] Exception in get_all_open_orders:", e)
        return None


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
# CANDLE & DATA HELPERS
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    return len(s.split(".")[1]) if "." in s else 0

def is_strong_bullish(o, h, l, c):
    if c <= o: return False
    rng = h - l
    if rng == 0: return False
    return ((c - o) / rng * 100) >= MIN_BODY_PCT

def is_strong_bearish(o, h, l, c):
    if c >= o: return False
    rng = h - l
    if rng == 0: return False
    return ((o - c) / rng * 100) >= MIN_BODY_PCT

def fetch_candles(symbol, num_candles, resolution_str, candle_seconds):
    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    params   = {"pair": pair_api, "from": now - (num_candles + 50) * candle_seconds, "to": now, "resolution": resolution_str, "pcode": "f"}
    try:
        data = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return sorted(data, key=lambda x: x["time"])
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} error: {e}")
        return []

def get_recent_high(symbol):
    try:
        now = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL, "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get("https://public.coindcx.com/market_data/candlesticks", params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return max(float(c["high"]) for c in candles) if candles else None
    except Exception:
        return None

def get_recent_low(symbol):
    try:
        now = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL, "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get("https://public.coindcx.com/market_data/candlesticks", params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        return min(float(c["low"]) for c in candles) if candles else None
    except Exception:
        return None

def get_quantity_step(symbol):
    try:
        pair = fut_pair(symbol)
        url  = f"https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument?pair={pair}&margin_currency_short_name=USDT"
        instrument = requests.get(url, timeout=REQUEST_TIMEOUT).json()["instrument"]
        qty_inc = Decimal(str(instrument["quantity_increment"]))
        min_qty = Decimal(str(instrument["min_quantity"]))
        return max(qty_inc, min_qty)
    except Exception:
        return Decimal("1")

def compute_qty(entry_price, symbol):
    step     = get_quantity_step(symbol)
    exposure = Decimal(str(CAPITAL_USDT)) * Decimal(str(LEVERAGE))
    raw_qty  = exposure / Decimal(str(entry_price))
    qty      = (raw_qty / step).quantize(Decimal("1")) * step
    if qty <= 0: qty = step
    return float(qty.quantize(step))

# =====================================================
# ORDERS
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision, si=None):
    entry, tp, sl = round(entry_price, precision), round(tp_price, precision), round(sl_price, precision)
    qty = compute_qty(entry_price, symbol)
    tp_pct, sl_pct = round(((tp - entry)/entry)*100, 2), round(((entry - sl)/entry)*100, 2)
    print(f"  [LONG] Entry={entry} TP={tp}(+{tp_pct}%) SL={sl}(-{sl_pct}%) Qty={qty}")
    body = {"timestamp": int(time.time() * 1000), "order": {"side": "buy", "pair": fut_pair(symbol), "order_type": "limit_order", "price": entry, "total_quantity": qty, "leverage": LEVERAGE, "take_profit_price": tp, "stop_loss_price": sl}}
    payload, headers = sign_request(body)
    try:
        result = requests.post(BASE_URL + "/exchange/v1/derivatives/futures/orders/create", data=payload, headers=headers, timeout=REQUEST_TIMEOUT).json()
    except Exception as e:
        print(f"  [ERROR] order request failed: {e}")
        return False
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] long rejected: {result}")
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False
    
    si = si or {}
    send_telegram(
        f"🟢 <b>NEW LONG (PDL SWEEP 5m) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry        : <code>{entry}</code>\n"
        f"🎯 TP           : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL           : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty          : <code>{qty}</code>\n"
        f"📌 Entry at close of C2. SL anchored to C1 Low."
    )
    return True

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, si=None):
    entry, tp, sl = round(entry_price, precision), round(tp_price, precision), round(sl_price, precision)
    qty = compute_qty(entry_price, symbol)
    tp_pct, sl_pct = round(((entry - tp)/entry)*100, 2), round(((sl - entry)/entry)*100, 2)
    print(f"  [SHORT] Entry={entry} TP={tp}(-{tp_pct}%) SL={sl}(+{sl_pct}%) Qty={qty}")
    body = {"timestamp": int(time.time() * 1000), "order": {"side": "sell", "pair": fut_pair(symbol), "order_type": "limit_order", "price": entry, "total_quantity": qty, "leverage": LEVERAGE, "take_profit_price": tp, "stop_loss_price": sl}}
    payload, headers = sign_request(body)
    try:
        result = requests.post(BASE_URL + "/exchange/v1/derivatives/futures/orders/create", data=payload, headers=headers, timeout=REQUEST_TIMEOUT).json()
    except Exception as e:
        print(f"  [ERROR] order request failed: {e}")
        return False
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] short rejected: {result}")
        send_telegram(f"❌ <b>SHORT REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False

    si = si or {}
    send_telegram(
        f"🔴 <b>NEW SHORT (PDH SWEEP 5m) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry        : <code>{entry}</code>\n"
        f"🎯 TP           : <code>{tp}</code>  (-{tp_pct}%)\n"
        f"🛑 SL           : <code>{sl}</code>  (+{sl_pct}%)\n"
        f"📦 Qty          : <code>{qty}</code>\n"
        f"📌 Entry at close of C2. SL anchored to C1 High."
    )
    return True

# =====================================================
# SWEEP CLEAR
# =====================================================
def _clear_sweep(st):
    st["sweep_direction"] = None; st["sweep_ts"] = 0
    st["recent_swing_low"] = None; st["recent_swing_high"] = None
    st["sweep_o"] = None; st["sweep_h"] = None; st["sweep_l"] = None; st["sweep_c"] = None
    st["candle1_ts"] = 0; st["candle1_o"] = None; st["candle1_h"] = None; st["candle1_l"] = None; st["candle1_c"] = None

def _reset_candle1(st):
    st["candle1_ts"] = 0; st["candle1_o"] = None; st["candle1_h"] = None; st["candle1_l"] = None; st["candle1_c"] = None

# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders):
    now_ms = int(time.time() * 1000)

    daily = fetch_candles(symbol, CANDLES_DAILY, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
    if daily and (now_ms - int(daily[-1]["time"])) < CANDLE_SECONDS_DAY * 1000:
        daily = daily[:-1]
    if not daily:
        print(f"  [{symbol}] SKIP — no completed daily candles")
        return

    prev_day  = daily[-1]
    pdh       = float(prev_day["high"])
    pdl       = float(prev_day["low"])
    precision = get_precision(prev_day["close"])

    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st: st[k] = v

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — PDH={pdh} PDL={pdl}")
        preserved = {k: st[k] for k in ("in_position", "direction", "entry_price", "tp_level", "sl_price", "last_entry_ts")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"], st["pdh"], st["pdl"] = today_str, pdh, pdl

    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
    if tp_raw.upper() == "TP COMPLETED":
        print(f"  [{symbol}] SKIP — TP COMPLETED in sheet")
        save_state(all_state)
        return

    try: tp_stored = float(tp_raw)
    except (ValueError, TypeError): tp_stored = None

    if tp_stored and tp_stored > 0:
        direction, tp_hit, hit_kind, hit_price = st.get("direction"), False, None, None
        last_1m = fetch_candles(symbol, 2, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        is_long = direction == "long" or (direction is None and last_close and tp_stored > last_close)

        if is_long:
            if last_close and last_close >= tp_stored: tp_hit, hit_kind, hit_price = True, "close", last_close
            elif (rh := get_recent_high(symbol)) and rh >= tp_stored: tp_hit, hit_kind, hit_price = True, "wick", rh
        else:
            if last_close and last_close <= tp_stored: tp_hit, hit_kind, hit_price = True, "close", last_close
            elif (rl := get_recent_low(symbol)) and rl <= tp_stored: tp_hit, hit_kind, hit_price = True, "wick", rl

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"  [{symbol}] TP HIT ({hit_kind}) price={hit_price} target={tp_stored}")
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"] = prev_last
            save_state(all_state)
            return

    # ── Reconcile with global exchange lists ────────────────────────────
    pair_name = fut_pair(symbol)
    position = next((p for p in global_positions if p.get("pair") == pair_name), None)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or 0)
            qty_str = str(position.get("size") or position.get("active_pos") or position.get("net_size") or "0")
            st["in_position"] = True
            st["direction"]   = "long" if float(qty_str) > 0 else "short"
            st["entry_price"] = entry_px
            print(f"  [{symbol}] RECONCILE — {st['direction']} found on exchange")

        tp_pos, sl_pos = extract_tp_sl(position)
        if st.get("tp_level") is None and tp_pos: st["tp_level"] = round(tp_pos, precision)
        if st.get("sl_price") is None and sl_pos: st["sl_price"] = round(sl_pos, precision)

        b_val, c_val = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else "", str(df.iloc[row, 2]).strip() if df.shape[1] > 2 else ""
        if st.get("tp_level") and b_val == "": update_sheet_tp(row, st["tp_level"])
        if st.get("sl_price") and c_val == "": update_sheet_sl(row, st["sl_price"])
        save_state(all_state)
        return

    # If memory thinks we are in a position, but API confirms we are NOT
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

    # ── 1H Guard & 5m Candles ────────────────────────────────────────────
    if not st["pdl_swept_1h"] or not st["pdh_swept_1h"]:
        candles_1h = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)
        if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
            candles_1h = candles_1h[:-1]
        today_start_ms = int(datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        todays_1h = [c for c in candles_1h if int(c["time"]) >= today_start_ms]
        for c1h in todays_1h:
            if float(c1h["low"])  < pdl and not st["pdl_swept_1h"]: st["pdl_swept_1h"] = True
            if float(c1h["high"]) > pdh and not st["pdh_swept_1h"]: st["pdh_swept_1h"] = True

    if st["pdl_swept_1h"] and st["pdh_swept_1h"]:
        print(f"  [{symbol}] SKIP — both sides already swept on 1h today")
        save_state(all_state)
        return

    candles_5m = fetch_candles(symbol, CANDLES_5M, RESOLUTION_5M, CANDLE_SECONDS_5M)
    if candles_5m and (now_ms - int(candles_5m[-1]["time"])) < CANDLE_SECONDS_5M * 1000:
        candles_5m = candles_5m[:-1]

    if len(candles_5m) < 5:
        print(f"  [{symbol}] SKIP — not enough 5m candles")
        return

    last_processed = st.get("last_processed_5m_ts", 0)
    new_candles    = [c for c in candles_5m if int(c["time"]) > last_processed]

    print(f"  [{symbol}] PDH={pdh} PDL={pdl} | sweep={st['sweep_direction'] or 'none'} "
          f"c1={'armed' if st['candle1_ts'] > 0 else 'waiting'} | new_5m={len(new_candles)}")

    if not new_candles:
        save_state(all_state)
        return

    # ── Signal Logic ─────────────────────────────────────────────────────
    entry_path, entry_price, sl_price_val, tp_price_val, signal_info = None, None, None, None, None

    for candle in new_candles:
        c_ts, o, h, l, c = int(candle["time"]), float(candle["open"]), float(candle["high"]), float(candle["low"]), float(candle["close"])
        sweep_dir = st["sweep_direction"]

        if sweep_dir is None:
            if l < pdl and not st["pdl_swept_1h"]:
                st["sweep_direction"] = "long"; st["sweep_ts"] = c_ts; st["recent_swing_low"] = l
                st["sweep_o"] = o; st["sweep_h"] = h; st["sweep_l"] = l; st["sweep_c"] = c
                print(f"  [{symbol}] SWEEP-LONG 5m low={l} < PDL={pdl}")
            elif h > pdh and not st["pdh_swept_1h"]:
                st["sweep_direction"] = "short"; st["sweep_ts"] = c_ts; st["recent_swing_high"] = h
                st["sweep_o"] = o; st["sweep_h"] = h; st["sweep_l"] = l; st["sweep_c"] = c
                print(f"  [{symbol}] SWEEP-SHORT 5m high={h} > PDH={pdh}")

        elif sweep_dir == "long":
            if l < st["recent_swing_low"]: st["recent_swing_low"] = l
            if (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_5M * 1000) > SWEEP_EXPIRY_BARS:
                print(f"  [{symbol}] SWEEP-EXPIRE long — resetting")
                _clear_sweep(st); st["last_processed_5m_ts"] = c_ts; continue

            if st["candle1_ts"] == 0:
                if c > o:
                    st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c
            else:
                if c_ts == st["candle1_ts"] + CANDLE_SECONDS_5M * 1000:
                    if is_strong_bullish(o, h, l, c) and c > st["candle1_c"]:
                        entry_price, natural_sl = c, st["candle1_l"]
                        min_sl = entry_price * (1 - MIN_SL_PCT / 100)
                        sl_price_val = min(natural_sl, min_sl)
                        tp_price_val = entry_price * (1 + TP_PCT / 100)
                        entry_path = "long_sweep"
                        st["last_processed_5m_ts"] = c_ts
                        break
                    else:
                        _reset_candle1(st)
                        if c > o: st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c
                else:
                    _reset_candle1(st)
                    if c > o: st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c

        elif sweep_dir == "short":
            if h > st["recent_swing_high"]: st["recent_swing_high"] = h
            if (c_ts - st["sweep_ts"]) // (CANDLE_SECONDS_5M * 1000) > SWEEP_EXPIRY_BARS:
                print(f"  [{symbol}] SWEEP-EXPIRE short — resetting")
                _clear_sweep(st); st["last_processed_5m_ts"] = c_ts; continue

            if st["candle1_ts"] == 0:
                if c < o:
                    st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c
            else:
                if c_ts == st["candle1_ts"] + CANDLE_SECONDS_5M * 1000:
                    if is_strong_bearish(o, h, l, c) and c < st["candle1_c"]:
                        entry_price, natural_sl = c, st["candle1_h"]
                        min_sl = entry_price * (1 + MIN_SL_PCT / 100)
                        sl_price_val = max(natural_sl, min_sl)
                        tp_price_val = entry_price * (1 - TP_PCT / 100)
                        entry_path = "short_sweep"
                        st["last_processed_5m_ts"] = c_ts
                        break
                    else:
                        _reset_candle1(st)
                        if c < o: st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c
                else:
                    _reset_candle1(st)
                    if c < o: st["candle1_ts"] = c_ts; st["candle1_o"] = o; st["candle1_h"] = h; st["candle1_l"] = l; st["candle1_c"] = c

        st["last_processed_5m_ts"] = c_ts

    if entry_path is None:
        if new_candles: st["last_processed_5m_ts"] = int(new_candles[-1]["time"])
        save_state(all_state)
        return

    # ── Final Safety Checks & Execution ───────────────────────────────────
    if entry_path == "long_sweep" and sl_price_val >= entry_price: return
    if entry_path == "short_sweep" and sl_price_val <= entry_price: return

    # No need to double-check API here; we trust the global fetch from seconds ago
    if entry_path == "long_sweep":
        placed = place_long_order(symbol, entry_price, tp_price_val, sl_price_val, precision)
    else:
        placed = place_short_order(symbol, entry_price, tp_price_val, sl_price_val, precision)

    if placed:
        st["in_position"] = True
        st["direction"] = "long" if entry_path == "long_sweep" else "short"
        st["entry_price"] = round(entry_price, precision)
        st["tp_level"] = round(tp_price_val, precision)
        st["sl_price"] = round(sl_price_val, precision)
        st["last_entry_ts"] = st["last_processed_5m_ts"]
        _clear_sweep(st)
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)

# =====================================================
# MAIN LOOP
# =====================================================

cycle = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>PDH/PDL Sweep Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🔒 API Check : <code>Global Batch Mode (Rate Limit Safe)</code>\n"
    f"⚡ Entry     : <code>C1=5m spike, C2=5m body≥70% beyond C1</code>\n"
    f"🔁 Scan      : <code>Every 90 seconds</code>\n"
    f"🎯 TP        : <code>entry ± {TP_PCT}%</code>\n"
    f"🛑 SL        : <code>Anchored to C1 Extreme</code>\n"
)

while True:
    try:
        df = get_sheet_data()
        if df.empty:
            print("[WARN] Sheet returned empty — retrying")
            time.sleep(SCAN_INTERVAL)
            continue

        # CRITICAL FIX: Fetch API Data ONCE globally
        global_positions = get_all_positions()
        global_orders    = get_all_open_orders()

        if global_positions is None or global_orders is None:
            print("[WARN] API failed to fetch positions/orders (Rate Limit/Network). Skipping cycle to protect state.")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0
        print(f"\n===== CYCLE {cycle} | {datetime.utcnow().strftime('%H:%M:%S UTC')} =====")

        symbols_checked = 0
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if not symbol: continue
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
            send_telegram(f"🚨 <b>Bot Crashed</b>\n❌ <code>{str(e)[:200]}</code>")
            raise SystemExit(1)
        time.sleep(60)