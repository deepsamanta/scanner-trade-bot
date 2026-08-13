import pandas as pd
import requests
import time
import json
import os
import gspread

from datetime import datetime, timezone
from google.oauth2.service_account import Credentials

from config import SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY: VWAP Periodic Close [LuxAlgo] — Python replication — SIGNAL ONLY
#
# Pine logic replicated:
#   vwap = ta.vwap(hlc3, timeframe.change(period))
#   -> cumulative hlc3*vol / vol, RESET at each period boundary:
#        DAILY   : new UTC day        (15m candles)
#        WEEKLY  : new ISO week (Mon) (1h candles)
#        MONTHLY : new calendar month (4h candles — full prev-month coverage)
#   When a period ends, its final VWAP is stored as a "VWAP Close Level".
#   Historical close levels kept (Pine defaults):
#        DAILY x1  |  WEEKLY x3  |  MONTHLY x2
#
# THIS BOT DOES NOT TRADE. Alerts on close crossing the RUNNING VWAP
# (closed candles only, deduped). All VWAP data + historical levels logged.
# =============================================================================

ENABLE_DAILY   = True
ENABLE_WEEKLY  = True
ENABLE_MONTHLY = True

# Pine "Historical Closes" defaults
HISTORICAL_LEVELS = {"D": 1, "W": 3, "M": 2}

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

CANDLES_15M      = 400    # ~4 UTC days (daily VWAP + 1 prev daily level)
CANDLES_1H       = 900    # ~37 days (weekly VWAP + 3 prev weekly levels)
CANDLES_4H       = 560    # ~93 days (monthly VWAP + 2 prev monthly levels)

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

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


def drop_forming_candle(candles, candle_seconds):
    now_ms = int(time.time() * 1000)
    if candles and (now_ms - int(candles[-1]["time"])) < candle_seconds * 1000:
        return candles[:-1]
    return candles


# =====================================================
# VOLUME FILTER (cached once per UTC day per symbol)
# =====================================================

_vol_cache = {}   # symbol -> (day_str, vol_usd)


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
        iso = dt.isocalendar()                      # week starts Monday (Pine 'W')
        return f"{iso[0]}-W{iso[1]:02d}"
    if tf == "M":
        return dt.strftime("%Y-%m")
    raise ValueError(tf)


def compute_periodic_vwap(candles, tf, max_levels):
    """
    ta.vwap(hlc3, timeframe.change(tf)) replicated + Pine's Historical Closes.
    Returns:
      vwaps        — per-bar running VWAP (aligned 1:1 with candles)
      cur_key      — current period key
      close_levels — list of (period_key, final_vwap), newest LAST,
                     trimmed to max_levels (like Pine's shift/delete).
                     close_levels[-1] = most recent completed period.
                     NOTE: the FIRST period in the data window may be partial
                     (window may not start exactly at a period boundary) —
                     with the candle counts above, all kept levels are full.
      period_bars  — bars in the current period
      complete     — True if the first level's period started inside the window
                     (i.e., we saw its opening bar), so all levels are exact
    """
    vwaps        = []
    cum_pv       = 0.0
    cum_v        = 0.0
    cur_key      = None
    close_levels = []
    period_bars  = 0
    first_key    = None
    boundaries   = 0

    for c in candles:
        k = period_key(c["time"], tf)
        if k != cur_key:                      # timeframe.change -> anchor reset
            if vwaps:                         # close the period that just ended
                close_levels.append((cur_key, vwaps[-1]))
                if len(close_levels) > max_levels + 1:   # +1: drop partial-first later
                    close_levels.pop(0)
                boundaries += 1
            else:
                first_key = k
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

    # The very first period in the window is likely partial (window start
    # mid-period) — drop its level if it's still in the list, then trim.
    if close_levels and first_key is not None and close_levels[0][0] == first_key:
        # only trustworthy if the window happened to start exactly at the boundary;
        # safest to keep it only when we have surplus levels
        if len(close_levels) > max_levels:
            close_levels.pop(0)
    close_levels = close_levels[-max_levels:]

    complete = len(close_levels) == max_levels
    return vwaps, cur_key, close_levels, period_bars, complete


def detect_cross(candles, vwaps):
    """
    Cross on the LAST CLOSED bar:
      above: close[-2] <= vwap[-2] and close[-1] > vwap[-1]
      below: close[-2] >= vwap[-2] and close[-1] < vwap[-1]
    """
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
    """'W32=0.074236 | W31=0.0812' style, newest first, numbered like Pine (1)=newest."""
    if not close_levels:
        return "--"
    parts = []
    for i, (k, v) in enumerate(reversed(close_levels), start=1):
        parts.append(f"({i}){k}={v:.8g}")
    return "  ".join(parts)


# =====================================================
# PER-SYMBOL SCAN
# =====================================================

TF_LABEL = {"D": "DAILY", "W": "WEEKLY", "M": "MONTHLY"}


def scan_symbol(symbol, all_state):
    st = all_state.setdefault(symbol, init_symbol_state())
    for k in ("D", "W", "M"):
        st.setdefault(k, {"last_ts": 0, "side": None})

    vol_usd = fetch_24h_volume(symbol)
    if vol_usd < MIN_24H_VOL_USDT:
        print(f"  [{symbol}] SKIP — 24h vol ${vol_usd:,.0f} < ${MIN_24H_VOL_USDT:,.0f}")
        return

    candles_15m = drop_forming_candle(
        fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M),
        CANDLE_SECONDS_15M)
    candles_1h  = drop_forming_candle(
        fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H),
        CANDLE_SECONDS_1H)
    candles_4h  = drop_forming_candle(
        fetch_candles(symbol, CANDLES_4H, RESOLUTION_4H, CANDLE_SECONDS_4H),
        CANDLE_SECONDS_4H)

    if len(candles_15m) < 10 or len(candles_1h) < 30 or len(candles_4h) < 30:
        print(f"  [{symbol}] SKIP — insufficient candles "
              f"(15m={len(candles_15m)} 1h={len(candles_1h)} 4h={len(candles_4h)})")
        return

    checks = []
    if ENABLE_DAILY:
        checks.append(("D", candles_15m))
    if ENABLE_WEEKLY:
        checks.append(("W", candles_1h))
    if ENABLE_MONTHLY:
        checks.append(("M", candles_4h))          # FIX: full prev-month coverage

    for tf, candles in checks:
        vwaps, cur_key, levels, pbars, complete = compute_periodic_vwap(
            candles, tf, HISTORICAL_LEVELS[tf])

        close   = float(candles[-1]["close"])
        vwap    = vwaps[-1]
        curr_ts = int(candles[-1]["time"])
        side    = "ABOVE" if close > vwap else "BELOW"
        dist    = (close / vwap - 1) * 100 if vwap else 0

        # ── LOG every coin's vwap data + historical levels ───────────────────
        print(f"  [{symbol}] {TF_LABEL[tf]:8s} vwap={vwap:.8g}  close={close:.8g}  "
              f"{side} {abs(dist):.3f}%  period={cur_key}({pbars}b)"
              f"{'' if complete else '  [levels partial]'}")
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
    f"M x{HISTORICAL_LEVELS['M']}</code> (Pine defaults)\n"
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