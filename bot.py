import pandas as pd
import requests
import time
import json
import os
import gspread

from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials

from config import SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY: VWAP Periodic Close [LuxAlgo] — Python replication — SIGNAL ONLY
#
# Levels tracked (Pine defaults) — COMPLETED periods only:
#   DAILY   x1  (from 15m candles)
#   WEEKLY  x3  (from 1h candles)
#   MONTHLY x2  (from 4h candles)
#
# The running/current-period VWAP is used ONLY internally to compute the
# final close level when a period ends. It is NOT logged and NOT alerted.
#
# ALERTS: 15m closed candle CROSSES above/below any tracked close level.
#   above: close[-2] <= level and close[-1] > level
#   below: close[-2] >= level and close[-1] < level
# Deduped per candle, first observation never alerts.
#
# UNIVERSE: ALL sheet coins (stables/wrapped excluded). NO volume filter.
# NO TRADING.
# =============================================================================

ENABLE_DAILY   = True
ENABLE_WEEKLY  = True
ENABLE_MONTHLY = True

HISTORICAL_LEVELS = {"D": 1, "W": 3, "M": 2}   # Pine defaults

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
    """
    hlc3 VWAP per period; store ONLY completed periods' final values.
    Current period's running VWAP is discarded (used internally only).
    Returns [(period_key, final_vwap)], newest LAST, max max_levels.
    """
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

    # current (unfinished) period intentionally NOT appended
    return close_levels


def fmt_levels(close_levels):
    if not close_levels:
        return "--"
    return "  ".join(f"({i}){k}={v:.8g}"
                     for i, (k, v) in enumerate(reversed(close_levels), start=1))


# =====================================================
# PER-SYMBOL SCAN — cross vs CLOSE LEVELS on 15m closed candles
# =====================================================

TF_LABEL = {"D": "DAILY", "W": "WEEKLY", "M": "MONTHLY"}

TF_FETCH = {
    "D": (RESOLUTION_15M, CANDLE_SECONDS_15M),
    "W": (RESOLUTION_1H,  CANDLE_SECONDS_1H),
    "M": (RESOLUTION_4H,  CANDLE_SECONDS_4H),
}


def scan_symbol(symbol, all_state):
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

    # ── Build all close levels (completed periods only) ──────────────────────
    all_levels    = {}          # (tf, period_key) -> (index, value)
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
            daily_candles = candles      # reuse 15m series for cross detection

        levels = compute_close_levels(candles, tf, HISTORICAL_LEVELS[tf])
        print(f"  [{symbol}] {TF_LABEL[tf]:8s} close_levels: {fmt_levels(levels)}")

        for i, (k, v) in enumerate(reversed(levels), start=1):
            all_levels[(tf, k)] = (i, v)

    if not all_levels or daily_candles is None or len(daily_candles) < 2:
        return

    # ── Cross detection: 15m closed candle vs every level ────────────────────
    c2      = float(daily_candles[-2]["close"])
    c1      = float(daily_candles[-1]["close"])
    curr_ts = int(daily_candles[-1]["time"])

    already_processed = curr_ts <= st.get("last_ts", 0)
    st["last_ts"] = max(st.get("last_ts", 0), curr_ts)

    lvl_state  = st["levels"]
    valid_keys = set()

    for (tf, pkey), (idx, lvl) in all_levels.items():
        skey = f"{tf}:{pkey}"
        valid_keys.add(skey)
        side = "ABOVE" if c1 > lvl else "BELOW"

        prev_side       = lvl_state.get(skey)
        lvl_state[skey] = side

        if already_processed or prev_side is None:
            continue                      # dedup / first observation — no alert

        cross = None
        if c2 <= lvl and c1 > lvl:
            cross = "above"
        elif c2 >= lvl and c1 < lvl:
            cross = "below"

        if cross:
            label = f"{TF_LABEL[tf]}({idx})"
            dist  = (c1 / lvl - 1) * 100
            emoji = "🟢⬆️" if cross == "above" else "🔴⬇️"
            print(f"  [{symbol}] *** CROSS {cross.upper()} {label} close level "
                  f"{pkey}={lvl:.8g} ***")
            send_telegram(
                f"{emoji} <b>{symbol} — CROSS {cross.upper()} {label} VWAP CLOSE LEVEL</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 Close : <code>{c1:.8g}</code>  (15m)\n"
                f"🧱 Level : <code>{lvl:.8g}</code>  ({label} · {pkey})\n"
                f"📐 Dist  : <code>{dist:+.3f}%</code>"
            )

    # prune stale level-side entries (rolled-off periods)
    for k in list(lvl_state.keys()):
        if k not in valid_keys:
            del lvl_state[k]


# =====================================================
# TRADING LOGIC — DISABLED (commented per request)
# =====================================================
# def compute_qty(entry_price, symbol):
#     step     = get_quantity_step(symbol)
#     exposure = Decimal(str(CAPITAL_USDT)) * Decimal(str(LEVERAGE))
#     raw_qty  = exposure / Decimal(str(entry_price))
#     qty      = (raw_qty / step).quantize(Decimal("1")) * step
#     return float(qty.quantize(step))
#
# def place_reversal_order(symbol, direction, entry_price, tp_price, sl_price, precision, vwap=None):
#     body = {
#         "timestamp": int(time.time() * 1000),
#         "order": {
#             "side": "buy" if direction == "long" else "sell",
#             "pair": fut_pair(symbol),
#             "order_type": "limit_order", "price": round(entry_price, precision),
#             "total_quantity": compute_qty(entry_price, symbol), "leverage": LEVERAGE,
#             "take_profit_price": round(tp_price, precision),
#             "stop_loss_price": round(sl_price, precision),
#         },
#     }
#     payload, headers = sign_request(body)
#     requests.post(BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
#                   data=payload, headers=headers, timeout=REQUEST_TIMEOUT)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>VWAP Close Level Bot Started (SIGNAL ONLY)</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Levels : <code>Completed-period VWAP closes only (LuxAlgo)</code>\n"
    f"🧱 Tracked: <code>D x{HISTORICAL_LEVELS['D']} | W x{HISTORICAL_LEVELS['W']} | "
    f"M x{HISTORICAL_LEVELS['M']}</code>\n"
    f"🔔 Alerts : <code>15m close crosses above/below any level</code>\n"
    f"🌐 Universe: <code>ALL sheet coins (no volume filter)</code>\n"
    f"🚫 Trading: <code>DISABLED — alerts only</code>\n"
    f"🔁 Scan   : <code>Every {SCAN_INTERVAL}s</code>"
)

while True:
    try:
        df = get_sheet_data()
        if df.empty:
            print("[WARN] Sheet returned empty — retrying")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"\n===== CYCLE {cycle} | {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} =====")

        symbols = []
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if symbol and not is_excluded(symbol):
                symbols.append(symbol)

        print(f"[UNIVERSE] {len(symbols)} symbols — scanning ALL (no volume filter)")

        for symbol in symbols:
            print(f"--- {symbol} ---")
            try:
                scan_symbol(symbol, state)
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