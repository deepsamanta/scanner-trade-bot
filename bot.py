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
#   vwap = ta.vwap(hlc3, timeframe.change(period)) — reset at anchor:
#        DAILY   : new UTC day        (15m candles)
#        WEEKLY  : new ISO week (Mon) (1h candles)
#        MONTHLY : new calendar month (4h candles)
#   Historical close levels (Pine defaults): D x1 | W x3 | M x2
#
# DATA COVERAGE FIX:
#   CoinDCX caps candles per request -> old fixed-count fetch left the
#   window starting mid-period ("[levels partial]", wrong old levels).
#   Now: compute the EXACT period-boundary start ts (Monday of W-3,
#   1st of M-2, start of D-1) and PAGINATE fetches in chunks until the
#   full range is covered. Every kept level is complete by construction.
#
# NO TRADING. Alerts on close crossing running VWAP (closed candles only).
# =============================================================================

ENABLE_DAILY   = True
ENABLE_WEEKLY  = True
ENABLE_MONTHLY = True

HISTORICAL_LEVELS = {"D": 1, "W": 3, "M": 2}   # Pine defaults

# ── Universe filter ──────────────────────────────────────────────────────────
MIN_24H_VOL_USDT = 1_000_000

STABLECOINS = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FRAX", "UST", "LUSD",
    "FDUSD", "PYUSD", "USDD", "USDN", "GUSD", "SUSD", "CUSD", "USDX", "OUSD",
}
WRAPPED = {"WBTC", "WETH", "WBNB", "WMATIC", "WAVAX", "WSOL", "WFTM"}

# ── Candles ──────────────────────────────────────────────────────────────────
RESOLUTION_15M   = "15"
RESOLUTION_1H    = "60"
RESOLUTION_4H    = "240"
RESOLUTION_DAILY = "1D"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

FETCH_CHUNK = 300          # candles per API request (safe under CoinDCX cap)

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


def init_symbol_state():
    return {
        "D": {"last_ts": 0, "side": None},
        "W": {"last_ts": 0, "side": None},
        "M": {"last_ts": 0, "side": None},
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
# PERIOD BOUNDARY CALCULATION (the fix, part 1)
# =====================================================

def period_start_ts(tf, n_hist):
    """
    UTC epoch seconds of the start of the OLDEST period we need:
    current period minus n_hist periods, aligned to the period boundary.
      D: 00:00 UTC of (today - n_hist days)
      W: 00:00 UTC Monday of (this ISO week - n_hist weeks)
      M: 00:00 UTC 1st of (this month - n_hist months)
    """
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


# =====================================================
# PAGINATED CANDLE FETCHER (the fix, part 2)
# =====================================================

def fetch_candles_range(symbol, from_ts, resolution_str, candle_seconds):
    """
    Fetch ALL candles from from_ts to now, in FETCH_CHUNK-sized requests,
    deduped by candle time. Guarantees full range coverage regardless of
    the API's per-request cap.
    """
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
# VOLUME FILTER (cached once per UTC day per symbol)
# =====================================================

_vol_cache = {}


def fetch_24h_volume(symbol):
    today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cached = _vol_cache.get(symbol)
    if cached and cached[0] == today:
        return cached[1]
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - 3 * 86400,
                  "to": now, "resolution": RESOLUTION_DAILY, "pcode": "f"}
        r = requests.get("https://public.coindcx.com/market_data/candlesticks",
                         params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return 0.0
        data = r.json()
        candle_list = data.get("data", data) if isinstance(data, dict) else data
        if not candle_list:
            return 0.0
        daily   = sorted(candle_list, key=lambda x: x["time"])[-1]
        vol_usd = float(daily.get("volume", 0) or 0) * float(daily.get("close", 0) or 0)
        _vol_cache[symbol] = (today, vol_usd)
        return vol_usd
    except Exception as e:
        print(f"  [VOL] {symbol} error: {e}")
        return 0.0


# =====================================================
# PINE REPLICATION — periodic VWAP + historical close levels
# =====================================================

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


def compute_periodic_vwap(candles, tf, max_levels):
    """
    ta.vwap(hlc3, timeframe.change(tf)) + historical close levels.
    Data window starts exactly at a period boundary (period_start_ts),
    so every completed period in it is FULL — no partial handling needed.
    close_levels: [(period_key, final_vwap)], newest LAST, max max_levels.
    """
    vwaps        = []
    cum_pv       = 0.0
    cum_v        = 0.0
    cur_key      = None
    close_levels = []
    period_bars  = 0

    for c in candles:
        k = period_key(c["time"], tf)
        if k != cur_key:
            if vwaps:
                close_levels.append((cur_key, vwaps[-1]))
                if len(close_levels) > max_levels:
                    close_levels.pop(0)
            cur_key     = k
            cum_pv      = 0.0
            cum_v       = 0.0
            period_bars = 0

        h, l, cl = float(c["high"]), float(c["low"]), float(c["close"])
        v        = float(c["volume"])
        cum_pv  += ((h + l + cl) / 3.0) * v
        cum_v   += v
        vwaps.append(cum_pv / cum_v if cum_v > 0 else cl)
        period_bars += 1

    return vwaps, cur_key, close_levels, period_bars


def detect_cross(candles, vwaps):
    if len(candles) < 2 or len(vwaps) < 2:
        return None
    c2, c1 = float(candles[-2]["close"]), float(candles[-1]["close"])
    v2, v1 = vwaps[-2], vwaps[-1]
    if c2 <= v2 and c1 > v1:
        return "above"
    if c2 >= v2 and c1 < v1:
        return "below"
    return None


def fmt_levels(close_levels):
    if not close_levels:
        return "--"
    return "  ".join(f"({i}){k}={v:.8g}"
                     for i, (k, v) in enumerate(reversed(close_levels), start=1))


# =====================================================
# PER-SYMBOL SCAN
# =====================================================

TF_LABEL = {"D": "DAILY", "W": "WEEKLY", "M": "MONTHLY"}

TF_FETCH = {
    "D": (RESOLUTION_15M, CANDLE_SECONDS_15M),
    "W": (RESOLUTION_1H,  CANDLE_SECONDS_1H),
    "M": (RESOLUTION_4H,  CANDLE_SECONDS_4H),
}


def scan_symbol(symbol, all_state):
    st = all_state.setdefault(symbol, init_symbol_state())
    for k in ("D", "W", "M"):
        st.setdefault(k, {"last_ts": 0, "side": None})

    vol_usd = fetch_24h_volume(symbol)
    if vol_usd < MIN_24H_VOL_USDT:
        print(f"  [{symbol}] SKIP — 24h vol ${vol_usd:,.0f} < ${MIN_24H_VOL_USDT:,.0f}")
        return

    checks = []
    if ENABLE_DAILY:
        checks.append("D")
    if ENABLE_WEEKLY:
        checks.append("W")
    if ENABLE_MONTHLY:
        checks.append("M")

    for tf in checks:
        res, secs = TF_FETCH[tf]
        from_ts   = period_start_ts(tf, HISTORICAL_LEVELS[tf])
        candles   = drop_forming_candle(
            fetch_candles_range(symbol, from_ts, res, secs), secs)

        if len(candles) < 5:
            print(f"  [{symbol}] {TF_LABEL[tf]} SKIP — insufficient candles ({len(candles)})")
            continue

        # coverage check: first candle must sit in the oldest required period
        first_key    = period_key(candles[0]["time"], tf)
        expected_key = period_key(from_ts * 1000, tf)
        if first_key != expected_key:
            print(f"  [{symbol}] {TF_LABEL[tf]} WARN — data starts {first_key}, "
                  f"needed {expected_key} (coin may be newly listed)")

        vwaps, cur_key, levels, pbars = compute_periodic_vwap(
            candles, tf, HISTORICAL_LEVELS[tf])

        close   = float(candles[-1]["close"])
        vwap    = vwaps[-1]
        curr_ts = int(candles[-1]["time"])
        side    = "ABOVE" if close > vwap else "BELOW"
        dist    = (close / vwap - 1) * 100 if vwap else 0

        print(f"  [{symbol}] {TF_LABEL[tf]:8s} vwap={vwap:.8g}  close={close:.8g}  "
              f"{side} {abs(dist):.3f}%  period={cur_key}({pbars}b)")
        print(f"  [{symbol}] {TF_LABEL[tf]:8s} close_levels: {fmt_levels(levels)}")

        tf_st = st[tf]
        if curr_ts <= tf_st.get("last_ts", 0):
            continue
        tf_st["last_ts"] = curr_ts

        cross     = detect_cross(candles, vwaps)
        prev_side = tf_st.get("side")
        tf_st["side"] = side

        if cross and prev_side is not None:
            emoji = "🟢⬆️" if cross == "above" else "🔴⬇️"
            print(f"  [{symbol}] *** CROSS {cross.upper()} {TF_LABEL[tf]} VWAP ***")

            lvl_lines = ""
            for i, (k, v) in enumerate(reversed(levels), start=1):
                lvl_lines += f"  ({i}) <code>{k}</code> = <code>{v:.8g}</code>\n"

            send_telegram(
                f"{emoji} <b>{symbol} — CROSS {cross.upper()} {TF_LABEL[tf]} VWAP</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 Close : <code>{close:.8g}</code>\n"
                f"📉 VWAP  : <code>{vwap:.8g}</code>  (hlc3, {TF_LABEL[tf].lower()} anchor)\n"
                f"📐 Dist  : <code>{dist:+.3f}%</code>\n"
                f"🕐 Period: <code>{cur_key}</code> ({pbars} bars)\n"
                f"🧱 Prev {TF_LABEL[tf].lower()} close levels:\n{lvl_lines}"
                f"💵 24h vol: <code>${vol_usd:,.0f}</code>"
            )


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
    f"✅ <b>VWAP Periodic Close Bot Started (SIGNAL ONLY)</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Replicates: <code>LuxAlgo VWAP Periodic Close (hlc3)</code>\n"
    f"🕐 Anchors : <code>D(15m) + W(1h) + M(4h)</code>\n"
    f"🧱 Levels  : <code>D x{HISTORICAL_LEVELS['D']} | W x{HISTORICAL_LEVELS['W']} | "
    f"M x{HISTORICAL_LEVELS['M']}</code>\n"
    f"📡 Fetch   : <code>Paginated from exact period boundaries</code>\n"
    f"🔔 Alerts  : <code>Close crosses above/below running VWAP</code>\n"
    f"🚫 Trading : <code>DISABLED — alerts only</code>\n"
    f"💵 Filter  : <code>24h vol >= ${MIN_24H_VOL_USDT:,}</code>\n"
    f"🔁 Scan    : <code>Every {SCAN_INTERVAL}s</code>"
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

        print(f"[UNIVERSE] {len(symbols)} symbols after stable/wrapped filter")

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