import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import os
import gspread

from decimal import Decimal, getcontext
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY: VWAP Periodic Close [LuxAlgo] levels + Break-and-Hold entries
#
# LEVELS (completed periods only, Pine defaults):
#   DAILY x1 (15m) | WEEKLY x3 (1h) | MONTHLY x2 (4h)
#   Running/current-period VWAP never shown, never used.
#
# ENTRY — SHORT:
#   c3 close ABOVE ALL levels
#   c2 close BELOW the HIGHEST level      (cross candle = candle 1)
#   c1 close BELOW the HIGHEST level      (confirmation = candle 2)
#   -> SHORT at c1 close
#   SL = crossed level * (1 + 3%)
#   TP = nearest level BELOW entry; if none or farther than 5% -> entry * (1 - 5%)
#
# ENTRY — LONG (mirror):
#   c3 close BELOW ALL levels
#   c2 close ABOVE the LOWEST level       (cross candle = candle 1)
#   c1 close ABOVE the LOWEST level       (confirmation = candle 2)
#   -> LONG at c1 close
#   SL = crossed level * (1 - 3%)
#   TP = nearest level ABOVE entry; if none or farther than 5% -> entry * (1 + 5%)
#
# Max 10 concurrent positions | 1 trade per coin | no re-entry while
# position/order open | per-candle dedup | cross alerts still sent.
# =============================================================================

ENABLE_DAILY   = True
ENABLE_WEEKLY  = True
ENABLE_MONTHLY = True

HISTORICAL_LEVELS = {"D": 1, "W": 3, "M": 2}

# ── Trade params ──────────────────────────────────────────────────────────────
MAX_OPEN_TRADES = 10
SL_PCT          = 3.0     # SL distance beyond the crossed level
TP_MAX_PCT      = 5.0     # TP cap / fallback

STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "UST", "LUSD",
    "FDUSD", "PYUSD", "USDD", "USDN", "GUSD", "SUSD", "CUSD", "USDX", "OUSD",
}
WRAPPED = {"WBTC", "WETH", "WBNB", "WMATIC", "WAVAX", "WSOL", "WFTM"}

# ── Candles ──────────────────────────────────────────────────────────────────
RESOLUTION_15M = "15"
RESOLUTION_1H  = "60"
RESOLUTION_4H  = "240"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

FETCH_CHUNK = 300

# ── Timing ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL          = 300
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "vwap_pcl_state.json"


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
        return pd.DataFrame(data)
    except Exception as e:
        print("Sheet read error:", e)
        return pd.DataFrame()


# =====================================================
# STATE
# =====================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[STATE] Load error: {e} — starting fresh")
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE] Save error: {e}")


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


def is_excluded(symbol):
    base = symbol.replace("USDT", "")
    return base in STABLECOINS or base in WRAPPED


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
# EXCHANGE FETCHERS
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
# QUANTITY / PRECISION
# =====================================================

def get_precision(raw_close):
    s = str(raw_close)
    return len(s.split(".")[1]) if "." in s else 0


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
# PERIOD BOUNDARIES
# =====================================================

def period_start_ts(tf, n_hist):
    now = datetime.now(timezone.utc)
    if tf == "D":
        start = (now - timedelta(days=n_hist)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif tf == "W":
        monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        start  = monday - timedelta(weeks=n_hist)
    elif tf == "M":
        y, m = now.year, now.month - n_hist
        while m <= 0:
            m += 12
            y -= 1
        start = datetime(y, m, 1, tzinfo=timezone.utc)
    else:
        raise ValueError(tf)
    return int(start.timestamp())


def period_key(ts_ms, tf):
    dt = datetime.fromtimestamp(int(ts_ms) // 1000, timezone.utc)
    if tf == "D":
        return dt.strftime("%Y-%m-%d")
    if tf == "W":
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if tf == "M":
        return dt.strftime("%Y-%m")
    raise ValueError(tf)


# =====================================================
# PAGINATED CANDLE FETCHER
# =====================================================

def fetch_candles_range(symbol, from_ts, resolution_str, candle_seconds):
    url     = "https://public.coindcx.com/market_data/candlesticks"
    now     = int(time.time())
    by_time = {}
    cur     = from_ts

    while cur < now:
        chunk_to = min(cur + FETCH_CHUNK * candle_seconds, now)
        params = {
            "pair":       fut_pair(symbol),
            "from":       cur,
            "to":         chunk_to,
            "resolution": resolution_str,
            "pcode":      "f",
        }
        try:
            data = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("data", [])
        except Exception as e:
            print(f"[CANDLES {resolution_str}] {symbol} chunk error: {e}")
            data = []
        for c in data:
            by_time[int(c["time"])] = c
        cur = chunk_to

    return sorted(by_time.values(), key=lambda x: x["time"])


def drop_forming_candle(candles, candle_seconds):
    now_ms = int(time.time() * 1000)
    if candles and (now_ms - int(candles[-1]["time"])) < candle_seconds * 1000:
        return candles[:-1]
    return candles


# =====================================================
# CLOSE LEVELS — completed periods ONLY
# =====================================================

def compute_close_levels(candles, tf, max_levels):
    close_levels = []
    cum_pv       = 0.0
    cum_v        = 0.0
    cur_key      = None
    last_vwap    = None

    for c in candles:
        k = period_key(c["time"], tf)
        if k != cur_key:
            if cur_key is not None and last_vwap is not None:
                close_levels.append((cur_key, last_vwap))
                if len(close_levels) > max_levels:
                    close_levels.pop(0)
            cur_key = k
            cum_pv  = 0.0
            cum_v   = 0.0

        h, l, cl = float(c["high"]), float(c["low"]), float(c["close"])
        v        = float(c["volume"])
        cum_pv  += ((h + l + cl) / 3.0) * v
        cum_v   += v
        last_vwap = cum_pv / cum_v if cum_v > 0 else cl

    return close_levels


def fmt_levels(close_levels):
    if not close_levels:
        return "--"
    return "  ".join(f"({i}){k}={v:.8g}"
                     for i, (k, v) in enumerate(reversed(close_levels), start=1))


# =====================================================
# ORDER PLACEMENT
# =====================================================

def place_order(symbol, direction, entry_price, tp_price, sl_price, precision,
                crossed_label, crossed_level, tp_label):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)
    qty   = compute_qty(entry_price, symbol)

    if direction == "long":
        side, emoji, label = "buy", "🟢", "LONG"
        tp_pct = round(((tp - entry) / entry) * 100, 2)
        sl_pct = round(((entry - sl) / entry) * 100, 2)
    else:
        side, emoji, label = "sell", "🔴", "SHORT"
        tp_pct = round(((entry - tp) / entry) * 100, 2)
        sl_pct = round(((sl - entry) / entry) * 100, 2)

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
        return False

    print(f"  [API] {symbol}: {result}")
    if "order" not in result and not isinstance(result, list):
        print(f"  [ERROR] {label.lower()} rejected: {result}")
        send_telegram(f"❌ <b>{label} REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False

    send_telegram(
        f"{emoji} <b>NEW {label} (VWAP LEVEL BREAK) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🧱 Broke  : <code>{crossed_label}</code> @ <code>{crossed_level:.8g}</code>\n"
        f"✅ Held   : <code>2 consecutive 15m closes</code>\n"
        f"📍 Entry  : <code>{entry}</code>\n"
        f"🎯 TP     : <code>{tp}</code>  (+{tp_pct}%  = {tp_label})\n"
        f"🛑 SL     : <code>{sl}</code>  (-{sl_pct}%  = {SL_PCT}% beyond level)\n"
        f"📦 Qty    : <code>{qty}</code>\n"
        f"💰 Margin : <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# PER-SYMBOL SCAN
# =====================================================

TF_LABEL = {"D": "DAILY", "W": "WEEKLY", "M": "MONTHLY"}

TF_FETCH = {
    "D": (RESOLUTION_15M, CANDLE_SECONDS_15M),
    "W": (RESOLUTION_1H,  CANDLE_SECONDS_1H),
    "M": (RESOLUTION_4H,  CANDLE_SECONDS_4H),
}


def scan_symbol(symbol, all_state, global_positions, global_orders, slots_left):
    """
    Returns True if a new trade was placed (consumes a slot).
    """
    st = all_state.setdefault(symbol, {"levels": {}, "last_ts": 0})
    st.setdefault("levels", {})
    st.setdefault("last_ts", 0)

    tfs = []
    if ENABLE_DAILY:
        tfs.append("D")
    if ENABLE_WEEKLY:
        tfs.append("W")
    if ENABLE_MONTHLY:
        tfs.append("M")

    # ── Build levels ──────────────────────────────────────────────────────────
    all_levels    = {}          # (tf, pkey) -> (index, value)
    daily_candles = None

    for tf in tfs:
        res, secs = TF_FETCH[tf]
        from_ts   = period_start_ts(tf, HISTORICAL_LEVELS[tf])
        candles   = drop_forming_candle(
            fetch_candles_range(symbol, from_ts, res, secs), secs)

        if len(candles) < 5:
            print(f"  [{symbol}] {TF_LABEL[tf]} SKIP — insufficient candles ({len(candles)})")
            continue

        if tf == "D":
            daily_candles = candles

        levels = compute_close_levels(candles, tf, HISTORICAL_LEVELS[tf])
        print(f"  [{symbol}] {TF_LABEL[tf]:8s} close_levels: {fmt_levels(levels)}")

        for i, (k, v) in enumerate(reversed(levels), start=1):
            all_levels[(tf, k)] = (i, v)

    if not all_levels or daily_candles is None or len(daily_candles) < 3:
        return False

    # sorted flat list: [(label, pkey, value)], ascending by value
    flat = sorted(
        [(f"{TF_LABEL[tf]}({idx})", pkey, lvl)
         for (tf, pkey), (idx, lvl) in all_levels.items()],
        key=lambda x: x[2],
    )
    level_values = [x[2] for x in flat]
    lowest_label,  lowest_pkey,  lowest_lvl  = flat[0]
    highest_label, highest_pkey, highest_lvl = flat[-1]

    c3 = float(daily_candles[-3]["close"])
    c2 = float(daily_candles[-2]["close"])
    c1 = float(daily_candles[-1]["close"])
    curr_ts = int(daily_candles[-1]["time"])

    already_processed = curr_ts <= st.get("last_ts", 0)

    # ── Cross alerts (kept from previous version) ─────────────────────────────
    lvl_state  = st["levels"]
    valid_keys = set()

    for (tf, pkey), (idx, lvl) in all_levels.items():
        skey = f"{tf}:{pkey}"
        valid_keys.add(skey)
        side            = "ABOVE" if c1 > lvl else "BELOW"
        prev_side       = lvl_state.get(skey)
        lvl_state[skey] = side

        if already_processed or prev_side is None:
            continue

        cross = None
        if c2 <= lvl and c1 > lvl:
            cross = "above"
        elif c2 >= lvl and c1 < lvl:
            cross = "below"

        if cross:
            label = f"{TF_LABEL[tf]}({idx})"
            dist  = (c1 / lvl - 1) * 100
            emoji = "🟢⬆️" if cross == "above" else "🔴⬇️"
            print(f"  [{symbol}] *** CROSS {cross.upper()} {label} {pkey}={lvl:.8g} ***")
            send_telegram(
                f"{emoji} <b>{symbol} — CROSS {cross.upper()} {label} VWAP CLOSE LEVEL</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 Close : <code>{c1:.8g}</code>  (15m)\n"
                f"🧱 Level : <code>{lvl:.8g}</code>  ({label} · {pkey})\n"
                f"📐 Dist  : <code>{dist:+.3f}%</code>"
            )

    for k in list(lvl_state.keys()):
        if k not in valid_keys:
            del lvl_state[k]

    if already_processed:
        return False
    st["last_ts"] = curr_ts

    # ── Trade guards ──────────────────────────────────────────────────────────
    pair_name = fut_pair(symbol)
    if any(p.get("pair") == pair_name for p in global_positions):
        print(f"  [{symbol}] SKIP ENTRY — position open")
        return False
    if any(o.get("pair") == pair_name for o in global_orders):
        print(f"  [{symbol}] SKIP ENTRY — order on book")
        return False
    if slots_left <= 0:
        return False

    precision = get_precision(float(daily_candles[-1]["close"]))
    direction = None

    # ── SHORT: above ALL -> 2 closes below HIGHEST level ─────────────────────
    if c3 > highest_lvl and c2 < highest_lvl and c1 < highest_lvl:
        direction     = "short"
        crossed_label = f"{highest_label} · {highest_pkey}"
        crossed_lvl   = highest_lvl
        entry         = c1
        sl            = crossed_lvl * (1 + SL_PCT / 100)
        below = [(lb, pk, lv) for lb, pk, lv in flat if lv < entry]
        if below:
            tp_label_, tp_pkey_, nearest = max(below, key=lambda x: x[2])
            dist_pct = (1 - nearest / entry) * 100
            if dist_pct <= TP_MAX_PCT:
                tp, tp_label = nearest, f"{tp_label_} · {tp_pkey_}"
            else:
                tp, tp_label = entry * (1 - TP_MAX_PCT / 100), f"{TP_MAX_PCT}% cap"
        else:
            tp, tp_label = entry * (1 - TP_MAX_PCT / 100), f"{TP_MAX_PCT}% (no level below)"

    # ── LONG: below ALL -> 2 closes above LOWEST level ───────────────────────
    elif c3 < lowest_lvl and c2 > lowest_lvl and c1 > lowest_lvl:
        direction     = "long"
        crossed_label = f"{lowest_label} · {lowest_pkey}"
        crossed_lvl   = lowest_lvl
        entry         = c1
        sl            = crossed_lvl * (1 - SL_PCT / 100)
        above = [(lb, pk, lv) for lb, pk, lv in flat if lv > entry]
        if above:
            tp_label_, tp_pkey_, nearest = min(above, key=lambda x: x[2])
            dist_pct = (nearest / entry - 1) * 100
            if dist_pct <= TP_MAX_PCT:
                tp, tp_label = nearest, f"{tp_label_} · {tp_pkey_}"
            else:
                tp, tp_label = entry * (1 + TP_MAX_PCT / 100), f"{TP_MAX_PCT}% cap"
        else:
            tp, tp_label = entry * (1 + TP_MAX_PCT / 100), f"{TP_MAX_PCT}% (no level above)"

    if direction is None:
        return False

    # sanity: TP/SL must be on correct sides of entry
    if direction == "short" and (tp >= entry or sl <= entry):
        print(f"  [{symbol}] SKIP ENTRY — invalid TP/SL geometry (short)")
        return False
    if direction == "long" and (tp <= entry or sl >= entry):
        print(f"  [{symbol}] SKIP ENTRY — invalid TP/SL geometry (long)")
        return False

    print(f"  [{symbol}] SIGNAL {direction.upper()} — broke {crossed_label} @ {crossed_lvl:.8g}, "
          f"held 2 candles (c3={c3:.8g} c2={c2:.8g} c1={c1:.8g})")

    return place_order(symbol, direction, entry, tp, sl, precision,
                       crossed_label, crossed_lvl, tp_label)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>VWAP Level Break Bot Started (LIVE TRADING)</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🧱 Levels : <code>D x{HISTORICAL_LEVELS['D']} | W x{HISTORICAL_LEVELS['W']} | "
    f"M x{HISTORICAL_LEVELS['M']} (completed periods)</code>\n"
    f"🔴 SHORT : <code>Above ALL -> 2x 15m closes below highest level</code>\n"
    f"🟢 LONG  : <code>Below ALL -> 2x 15m closes above lowest level</code>\n"
    f"🛑 SL    : <code>{SL_PCT}% beyond crossed level</code>\n"
    f"🎯 TP    : <code>Nearest level (max {TP_MAX_PCT}%) or {TP_MAX_PCT}%</code>\n"
    f"📊 Max   : <code>{MAX_OPEN_TRADES} concurrent positions</code>\n"
    f"💰 <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code> | 🔁 <code>{SCAN_INTERVAL}s</code>"
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
            print("[WARN] Exchange API fetch failed — skipping cycle")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        active_count = len(global_positions)
        slots_left   = max(0, MAX_OPEN_TRADES - active_count)

        print(f"\n===== CYCLE {cycle} | "
              f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} | "
              f"positions={active_count}/{MAX_OPEN_TRADES} slots={slots_left} =====")

        symbols = []
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if symbol and not is_excluded(symbol):
                symbols.append(symbol)

        print(f"[UNIVERSE] {len(symbols)} symbols — scanning ALL")

        for symbol in symbols:
            print(f"--- {symbol} ---")
            try:
                placed = scan_symbol(symbol, state, global_positions,
                                     global_orders, slots_left)
                if placed:
                    slots_left -= 1
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")

        save_state(state)
        print(f"===== CYCLE {cycle} DONE =====")
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        consecutive_errors += 1
        print(f"BOT ERROR ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}")
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            send_telegram(f"🚨 <b>Bot Crashed</b>\n❌ <code>{str(e)[:200]}</code>")
            raise SystemExit(1)
        time.sleep(60)