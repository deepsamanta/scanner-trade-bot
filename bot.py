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
# STRATEGY PARAMETERS  (Trendline Break + Anti-Fakeout — SHORT ONLY)
#
# TIMEFRAME ARCHITECTURE (mirror of the LONG bot):
#   - Trendline / S-R levels  : 1h closed candles
#   - Entry confirmation      : 15m closed candles
#   - Scan interval           : 15 minutes
#
# ENTRY PATHS:
#   - tl_break  : fresh cross-DOWN through lower_lvl on the latest 15m bar
#                 + ALL anti-fakeout filters pass on that same bar.
#   - tl_retest : when a fresh cross-down fires but filters fail, the bot ARMS
#                 a retest watch. If price pulls back UP to lower_lvl and gets
#                 rejected (15m wick touches the line, then closes back below
#                 with a bearish body), it shorts. Captures the case where the
#                 broken support acts as new resistance.
# =============================================================================

# ─── TRENDLINE TOUCH CONFIRMATION ───────────────────────────────────────────
# Before either path can fire, the trendline must have been TOUCHED at least
# this many times since its anchor pivot. A "touch" = a 1h bar whose low
# reached within TOUCH_TOLERANCE_PCT of the line but whose close stayed
# above (price tried to break the support and failed). Consecutive bars in
# the same approach are collapsed via state machine: price must rise
# ≥ TOUCH_GAP_PCT above the line before the next attempt counts.
MIN_TL_TOUCHES       = 2
TOUCH_TOLERANCE_PCT  = 0.5
TOUCH_GAP_PCT        = 1.0

# ─── TRENDLINE CALC (1h) ─────────────────────────────────────────────────────
SWING_LOOKBACK   = 14
SLOPE_MULT       = 1.0
ATR_PERIOD_TL    = 14

# ─── RISK ────────────────────────────────────────────────────────────────────
TP_PCT           = 3.0         # fixed take-profit: entry × (1 − TP_PCT/100)
SL_ABOVE_TL_PCT  = 1.5         # SL placed X% above the lower trendline (broken support → resistance)

# ─── ANTI-FAKEOUT FILTERS (applied on 15m entry candle, tl_break path) ──────
USE_BODY_BREAK   = True
USE_STRONG_BAR   = True
MIN_BODY_PCT     = 50.0
USE_VOLUME       = True
VOL_MULT         = 1.2
VOL_SMA_PERIOD   = 20
USE_ATR_DIST     = True
ATR_MULT         = 0.25
ATR_PERIOD_15M   = 14
USE_COOLDOWN     = True
COOLDOWN_BARS    = 5

# ─── RETEST PATH (tl_retest) ─────────────────────────────────────────────────
USE_RETEST_PATH      = True
RETEST_MAX_BARS      = 20
RETEST_TOUCH_PCT     = 0.3
RETEST_MIN_BODY_PCT  = 30
RETEST_CANCEL_PCT    = 1.0     # cancel retest if 15m close rises X% above TL

# ─── CANDLE COUNTS ───────────────────────────────────────────────────────────
TL_CANDLES_1H    = 500
ENTRY_CANDLES    = 60

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY    = "60"            # 1h
RESOLUTION_ENTRY      = "15"            # 15m
CANDLE_SECONDS        = 60 * 60
ENTRY_CANDLE_SECONDS  = 15 * 60
SCAN_INTERVAL         = 15 * 60

# ─── REQUEST TIMEOUTS ────────────────────────────────────────────────────────
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10

# ─── GSPREAD RE-AUTH INTERVAL ────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60

# ─── LOCAL STATE FILE ────────────────────────────────────────────────────────
STATE_FILE           = "short_bot_state.json"
# =============================================================================


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
        "in_position":        False,
        "entry_path":         None,
        "entry_price":        None,
        "tp_level":           None,
        "sl_price":           None,
        "last_entry_ts":      0,        # ms ts of last entry (cooldown anchor)

        # ── Retest path state ──────────────────────────────
        "retest_armed":       False,
        "retest_armed_ts":    0,
        "retest_lower_lvl":   None,     # lower_lvl snapshot at arming
        "retest_last_pl":     None,     # for telegram/log only
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
# SIGN REQUEST
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


# =====================================================
# TELEGRAM
# =====================================================

def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }
        r = requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
        if r.status_code != 200:
            print(f"[TELEGRAM] Non-200 response {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


# =====================================================
# PRECISION
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATORS
# =====================================================

def compute_atr(highs, lows, closes, period):
    """Wilder's ATR (matches Pine ta.atr / RMA smoothing)."""
    n = len(closes)
    if n == 0:
        return []
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        ))
    atr = [None] * n
    if n < period:
        return atr
    atr[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def compute_trendlines(opens, highs, lows, closes, length, mult):
    """
    LuxAlgo-style Trendlines with Breaks (short-side fields).

    Pivot detection uses the CANDLE BODY (max(open,close) for highs,
    min(open,close) for lows), so wicks/spikes never anchor a trendline.
    ATR (used for slope decay) still uses true high/low.

    Returns final-bar values:
        lower_lvl      = projected support level (cur_lower + slope_pl × length)
        last_pl        = most recent pivot low body-bottom (anchors lower TL)
        last_ph        = most recent pivot high body-top (context)
        touches_below  = count of failed-break attempts on the lower TL
                         since its anchor pivot confirmed
    """
    n = len(closes)
    atr_arr = compute_atr(highs, lows, closes, ATR_PERIOD_TL)

    body_tops    = [max(opens[i], closes[i]) for i in range(n)]
    body_bottoms = [min(opens[i], closes[i]) for i in range(n)]

    cur_upper = 0.0
    cur_lower = 0.0
    cur_slope_ph = 0.0
    cur_slope_pl = 0.0
    cur_last_ph = None
    cur_last_pl = None
    have_upper = False
    have_lower = False

    lower_lvl_history = [None] * n
    last_pl_bar_idx   = None     # bar index where most recent pivot low confirmed

    for i in range(n):
        ph = None
        pl = None

        # Pivot at index (i-length) is confirmed at bar i — body-based
        if i >= 2 * length:
            c    = i - length
            bt_c = body_tops[c]
            bb_c = body_bottoms[c]
            is_ph = True
            is_pl = True
            for k in range(1, length + 1):
                if body_tops[c - k]    >= bt_c or body_tops[c + k]    >= bt_c:
                    is_ph = False
                if body_bottoms[c - k] <= bb_c or body_bottoms[c + k] <= bb_c:
                    is_pl = False
                if not is_ph and not is_pl:
                    break
            if is_ph:
                ph = bt_c
            if is_pl:
                pl = bb_c

        slope = (atr_arr[i] / length * mult) if (atr_arr[i] is not None) else 0.0

        # Upper trendline (descending resistance — context only here)
        if ph is not None:
            cur_slope_ph = slope
            cur_upper    = ph
            cur_last_ph  = ph
            have_upper   = True
        elif have_upper:
            cur_upper -= cur_slope_ph

        # Lower trendline (ascending support — anchors SL for SHORT entries)
        if pl is not None:
            cur_slope_pl    = slope
            cur_lower       = pl
            cur_last_pl     = pl
            have_lower      = True
            last_pl_bar_idx = i
        elif have_lower:
            cur_lower += cur_slope_pl

        if have_lower:
            lower_lvl_history[i] = cur_lower + cur_slope_pl * length

    upper_lvl = (cur_upper - cur_slope_ph * length) if have_upper else None
    lower_lvl = (cur_lower + cur_slope_pl * length) if have_lower else None

    # ── Count touches on the CURRENT lower trendline ───────────────────────
    # A touch = a 1h bar whose low reached within TOUCH_TOLERANCE_PCT above
    # the line, but whose close stayed above (line held as support).
    # Sequential bars in the same approach are collapsed: only count a NEW
    # touch after price has risen ≥ TOUCH_GAP_PCT above the line in between.
    touches_below = 0
    if last_pl_bar_idx is not None and last_pl_bar_idx + 1 < n:
        in_approach = False
        for j in range(last_pl_bar_idx + 1, n):
            lvl = lower_lvl_history[j]
            if lvl is None or lvl <= 0:
                continue
            approach_thresh = lvl * (1 + TOUCH_TOLERANCE_PCT / 100)
            gap_thresh      = lvl * (1 + TOUCH_GAP_PCT       / 100)

            approached = lows[j]   <= approach_thresh
            close_high = closes[j] >  lvl

            if approached and close_high:
                if not in_approach:
                    touches_below += 1
                    in_approach = True
            else:
                if lows[j] > gap_thresh:
                    in_approach = False

    return {
        "upper_lvl":     upper_lvl,
        "lower_lvl":     lower_lvl,
        "last_ph":       cur_last_ph,
        "last_pl":       cur_last_pl,
        "touches_below": touches_below,
    }


# =====================================================
# CANDLE FETCH
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
        candles  = sorted(data, key=lambda x: x["time"])
        return candles
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} fetch error: {e}")
        return []


# =====================================================
# RECENT LOW (TP wick detection between scans — SHORT)
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


def extract_tp_sl(obj):
    """Pull (tp, sl) out of a CoinDCX position/order dict."""
    if not isinstance(obj, dict):
        return None, None
    tp_keys = ["take_profit_price", "take_profit_trigger", "tp_price"]
    sl_keys = ["stop_loss_price", "stop_loss_trigger", "sl_price"]

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
# PLACE SHORT ORDER
# =====================================================

def place_short_order(symbol, entry_price, tp_price, sl_price, precision, entry_path, signal_info=None):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)

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
            data=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
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

    sig_block = ""
    if signal_info:
        if entry_path == "tl_retest":
            sig_block = (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"<b>📊 Retest signal:</b>\n"
                f"📍 c15        : <code>{signal_info.get('c15')}</code>\n"
                f"⏫ high15     : <code>{signal_info.get('h15')}</code>\n"
                f"📉 lowerLvl   : <code>{signal_info.get('lower_lvl')}</code>\n"
                f"🔴 lastPH     : <code>{signal_info.get('last_ph')}</code>\n"
                f"🟢 lastPL     : <code>{signal_info.get('last_pl')}</code>\n"
                f"🪢 Touches    : <code>{signal_info.get('touches_below')}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"🧱 armed @TL  : <code>{signal_info.get('armed_lower_lvl')}</code>\n"
                f"⏳ bars armed : <code>{signal_info.get('bars_armed')}</code> / {RETEST_MAX_BARS}\n"
                f"<b>✅ Retest conditions:</b>\n"
                f"• wick touched line : <code>True</code> "
                f"(high {signal_info.get('h15')} within ±{RETEST_TOUCH_PCT}% of lowerLvl)\n"
                f"• close below line  : <code>True</code> "
                f"(c15 {signal_info.get('c15')} &lt; lowerLvl)\n"
                f"• bearish bar       : <code>True</code> "
                f"(c15 &lt; o15)\n"
                f"• body strong       : <code>True</code> "
                f"(body={signal_info.get('body_pct')}% ≥ {RETEST_MIN_BODY_PCT}%)\n"
                f"• cooldown          : <code>True</code> "
                f"({signal_info.get('bars_since_last')} bars, need ≥{COOLDOWN_BARS})"
            )
        else:
            # tl_break
            sig_block = (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"<b>📊 Signal that fired:</b>\n"
                f"📍 c15        : <code>{signal_info.get('c15')}</code>\n"
                f"⏪ prev_c15   : <code>{signal_info.get('prev_c15')}</code>\n"
                f"📉 lowerLvl   : <code>{signal_info.get('lower_lvl')}</code>\n"
                f"🔴 lastPH     : <code>{signal_info.get('last_ph')}</code>\n"
                f"🟢 lastPL     : <code>{signal_info.get('last_pl')}</code>\n"
                f"🪢 Touches    : <code>{signal_info.get('touches_below')}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"<b>✅ Filters passed:</b>\n"
                f"• fresh cross : <code>True</code> "
                f"(prev≥{signal_info.get('lower_lvl')} &amp; c15&lt;{signal_info.get('lower_lvl')})\n"
                f"• body break  : <code>True</code> "
                f"(max(o,c)={signal_info.get('max_oc')} &lt; lowerLvl)\n"
                f"• strong bar  : <code>True</code> "
                f"(body={signal_info.get('body_pct')}% ≥ {signal_info.get('min_body_pct')}%)\n"
                f"• volume      : <code>True</code> "
                f"(vol={signal_info.get('vol')} &gt; SMA20×{signal_info.get('vol_mult')}={signal_info.get('vol_threshold')})\n"
                f"• ATR distance: <code>True</code> "
                f"(c15 &lt; lowerLvl − {signal_info.get('atr_mult')}×ATR = {signal_info.get('atr_threshold')})\n"
                f"• cooldown    : <code>True</code> "
                f"({signal_info.get('bars_since_last')} bars since last entry, need ≥{signal_info.get('cooldown_bars')})"
            )

    send_telegram(
        f"🔴 <b>NEW SHORT ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (-{tp_pct_display}%)\n"
        f"🛑 SL      : <code>{sl}</code>  (+{sl_pct_display}%)\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
        f"{sig_block}"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC  (Trendline Break — SHORT only)
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    # ─── Fetch 1h for trendline ──────────────────────────────
    candles_1h = fetch_candles(symbol, TL_CANDLES_1H, RESOLUTION_PRIMARY, CANDLE_SECONDS)
    min_1h_needed = SWING_LOOKBACK * 2 + ATR_PERIOD_TL + 5

    if len(candles_1h) < min_1h_needed:
        print(f"[SKIP] {symbol} — insufficient 1h candles ({len(candles_1h)})")
        return

    # Drop in-progress 1h bar
    now_ms = int(time.time() * 1000)
    if len(candles_1h) >= 1 and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS * 1000:
        candles_1h = candles_1h[:-1]
    if len(candles_1h) < min_1h_needed:
        print(f"[SKIP] {symbol} — insufficient closed 1h candles ({len(candles_1h)})")
        return

    highs_1h  = [float(c["high"])  for c in candles_1h]
    lows_1h   = [float(c["low"])   for c in candles_1h]
    closes_1h = [float(c["close"]) for c in candles_1h]
    opens_1h  = [float(c["open"])  for c in candles_1h]

    tl = compute_trendlines(opens_1h, highs_1h, lows_1h, closes_1h, SWING_LOOKBACK, SLOPE_MULT)
    lower_lvl     = tl["lower_lvl"]
    last_ph       = tl["last_ph"]
    last_pl       = tl["last_pl"]
    touches_below = tl["touches_below"]

    if lower_lvl is None or last_ph is None or last_pl is None:
        print(f"[SKIP] {symbol} — trendline / pivots not ready yet")
        return

    precision     = get_precision(candles_1h[-1]["close"])
    last_close_1h = closes_1h[-1]

    # Per-symbol state
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # Backfill any missing fields for older state files
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # =========================================================================
    # TP COMPLETED MONITORING (SHORT: TP is BELOW entry, so check for new lows)
    # =========================================================================
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED marker in sheet, not re-entering")
        save_state(all_state)
        return

    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        tp_hit = False
        hit_kind = None
        hit_price = None

        if last_close_1h <= tp_stored:
            tp_hit    = True
            hit_kind  = "close"
            hit_price = last_close_1h

        if not tp_hit:
            recent_low = get_recent_low(symbol)
            if recent_low is not None and recent_low <= tp_stored:
                tp_hit    = True
                hit_kind  = "wick"
                hit_price = recent_low

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} — {hit_kind} {hit_price} ≤ stored TP {tp_stored}")
            send_telegram(
                f"🎯 <b>TP HIT ({hit_kind}) — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 {hit_kind.capitalize():8}: <code>{hit_price}</code>\n"
                f"🎯 TP       : <code>{tp_stored}</code>\n"
                f"✅ Marked <b>TP COMPLETED</b> — no further entries on this coin"
            )
            if st.get("in_position"):
                prev_last_entry = st.get("last_entry_ts", 0)
                all_state[symbol] = init_symbol_state()
                all_state[symbol]["last_entry_ts"] = prev_last_entry
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close_1h)
            st["in_position"] = True
            st["entry_path"]  = st.get("entry_path") or "tl_break"
            st["entry_price"] = entry_px
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")

        tp_pos, sl_pos = extract_tp_sl(position)
        if st.get("tp_level") is None and tp_pos is not None:
            st["tp_level"] = round(tp_pos, precision)
        if st.get("sl_price") is None and sl_pos is not None:
            st["sl_price"] = round(sl_pos, precision)

        b_val = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
        c_val = str(df.iloc[row, 2]).strip() if df.shape[1] > 2 else ""
        if st.get("tp_level") is not None and b_val == "":
            update_sheet_tp(row, st["tp_level"])
        if st.get("sl_price") is not None and c_val == "":
            update_sheet_sl(row, st["sl_price"])

        save_state(all_state)
        return

    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")
        send_telegram(
            f"✅ <b>POSITION CLOSED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path     : <code>{st.get('entry_path')}</code>\n"
            f"📍 Entry    : <code>{st.get('entry_price')}</code>\n"
            f"🎯 TP was   : <code>{st.get('tp_level')}</code>\n"
            f"🛑 SL was   : <code>{st.get('sl_price')}</code>"
        )
        prev_last_entry = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last_entry
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # FETCH 15m FOR ENTRY CONFIRMATION
    # =========================================================================
    candles_15m = fetch_candles(symbol, ENTRY_CANDLES, RESOLUTION_ENTRY, ENTRY_CANDLE_SECONDS)
    min_15m_needed = max(VOL_SMA_PERIOD, ATR_PERIOD_15M) + 5

    if len(candles_15m) < min_15m_needed:
        print(f"[SKIP] {symbol} — insufficient 15m candles ({len(candles_15m)})")
        return

    if len(candles_15m) >= 1 and (now_ms - int(candles_15m[-1]["time"])) < ENTRY_CANDLE_SECONDS * 1000:
        candles_15m = candles_15m[:-1]
    if len(candles_15m) < min_15m_needed:
        return

    last15 = candles_15m[-1]
    prev15 = candles_15m[-2]

    o15 = float(last15["open"])
    h15 = float(last15["high"])
    l15 = float(last15["low"])
    c15 = float(last15["close"])
    v15 = float(last15.get("volume", 0))
    ts15 = int(last15["time"])
    prev_c15 = float(prev15["close"])

    # ─── 15m filter calcs ────────────────────────────────────
    bar_range = h15 - l15
    bar_body  = abs(c15 - o15)
    body_pct  = (bar_body / bar_range * 100) if bar_range > 0 else 0

    vols = [float(c.get("volume", 0)) for c in candles_15m[-VOL_SMA_PERIOD:]]
    vol_sma = sum(vols) / len(vols) if vols else 0

    highs_15m  = [float(c["high"])  for c in candles_15m]
    lows_15m   = [float(c["low"])   for c in candles_15m]
    closes_15m = [float(c["close"]) for c in candles_15m]
    atr_arr_15 = compute_atr(highs_15m, lows_15m, closes_15m, ATR_PERIOD_15M)
    atr_15 = atr_arr_15[-1] if atr_arr_15[-1] is not None else 0

    # ─── Filter conditions (tl_break path — SHORT mirror) ───
    fresh_cross = (c15 < lower_lvl) and (prev_c15 >= lower_lvl)        # first cross-down
    body_break  = (not USE_BODY_BREAK) or (max(o15, c15) < lower_lvl)  # body fully below
    strong_bar  = (not USE_STRONG_BAR) or (body_pct >= MIN_BODY_PCT)
    vol_ok      = (not USE_VOLUME)     or (v15 > vol_sma * VOL_MULT)
    atr_ok      = (not USE_ATR_DIST)   or (c15 < lower_lvl - atr_15 * ATR_MULT)

    last_entry_ts = st.get("last_entry_ts", 0) or 0
    if USE_COOLDOWN and last_entry_ts > 0:
        bars_since  = (ts15 - last_entry_ts) // (ENTRY_CANDLE_SECONDS * 1000)
        cooldown_ok = bars_since >= COOLDOWN_BARS
    else:
        cooldown_ok = True

    short_sig = (
        fresh_cross and body_break and strong_bar and vol_ok and atr_ok
        and cooldown_ok and (touches_below >= MIN_TL_TOUCHES)
    )

    print(
        f"[SCAN] {symbol} | c15={c15} prev_c15={prev_c15} | "
        f"lowerLvl={round(lower_lvl, precision)} lastPL={round(last_pl, precision)} "
        f"lastPH={round(last_ph, precision)} touches={touches_below}/{MIN_TL_TOUCHES} | "
        f"fresh={fresh_cross} body={body_break} "
        f"strong={strong_bar}({round(body_pct, 1)}%) "
        f"vol={vol_ok}({round(v15, 2)} vs {round(vol_sma * VOL_MULT, 2)}) "
        f"atr={atr_ok} cd={cooldown_ok}"
    )

    # =========================================================================
    # RETEST PATH — arm / evaluate / cancel
    # =========================================================================
    retest_sig         = False
    retest_signal_info = None

    if USE_RETEST_PATH:
        # ARM: fresh cross-down fired but main short_sig didn't go through
        if fresh_cross and not short_sig and touches_below >= MIN_TL_TOUCHES:
            st["retest_armed"]      = True
            st["retest_armed_ts"]   = ts15
            st["retest_lower_lvl"]  = lower_lvl
            st["retest_last_pl"]    = last_pl
            print(f"[RETEST-ARM] {symbol} — armed at lowerLvl={round(lower_lvl, precision)} "
                  f"touches={touches_below} "
                  f"(filters that blocked main: body={body_break} strong={strong_bar} "
                  f"vol={vol_ok} atr={atr_ok} cd={cooldown_ok})")
            send_telegram(
                f"🟠 <b>RETEST ARMED — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 c15        : <code>{round(c15, precision)}</code>\n"
                f"📉 lowerLvl   : <code>{round(lower_lvl, precision)}</code>\n"
                f"🔴 lastPH     : <code>{round(last_ph, precision)}</code>\n"
                f"🟢 lastPL     : <code>{round(last_pl, precision)}</code>\n"
                f"🪢 Touches    : <code>{touches_below}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"⏳ Waiting up to {RETEST_MAX_BARS} × 15m bars for pullback-rejection"
            )
        elif fresh_cross and not short_sig and touches_below < MIN_TL_TOUCHES:
            print(f"[RETEST-NO-ARM] {symbol} — fresh cross but only {touches_below}/{MIN_TL_TOUCHES} touches, "
                  f"trendline not tested enough")

        # EVALUATE: if armed, check for entry conditions on this 15m bar
        if st.get("retest_armed"):
            armed_ts  = st.get("retest_armed_ts", 0)
            armed_lvl = st.get("retest_lower_lvl")

            if armed_ts and armed_lvl:
                bars_armed = max(0, (ts15 - armed_ts) // (ENTRY_CANDLE_SECONDS * 1000))

                # Cancel: timed out
                if bars_armed > RETEST_MAX_BARS:
                    print(f"[RETEST-CANCEL] {symbol} — timed out after {bars_armed} bars")
                    send_telegram(
                        f"⌛ <b>RETEST EXPIRED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⏳ {bars_armed} &gt; {RETEST_MAX_BARS} × 15m bars, no rejection occurred"
                    )
                    st["retest_armed"]     = False
                    st["retest_armed_ts"]  = 0
                    st["retest_lower_lvl"] = None
                    st["retest_last_pl"]   = None

                # Cancel: close rose well above the trendline (line reclaimed)
                elif c15 > lower_lvl * (1 + RETEST_CANCEL_PCT / 100):
                    print(f"[RETEST-CANCEL] {symbol} — close {c15} rose &gt;{RETEST_CANCEL_PCT}% "
                          f"above lowerLvl {round(lower_lvl, precision)} → line reclaimed")
                    send_telegram(
                        f"❌ <b>RETEST CANCELLED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 c15      : <code>{round(c15, precision)}</code>\n"
                        f"📉 lowerLvl : <code>{round(lower_lvl, precision)}</code>\n"
                        f"⚠️ Reason   : line reclaimed (close rose &gt;{RETEST_CANCEL_PCT}% above)"
                    )
                    st["retest_armed"]     = False
                    st["retest_armed_ts"]  = 0
                    st["retest_lower_lvl"] = None
                    st["retest_last_pl"]   = None

                elif bars_armed < 1:
                    print(f"[RETEST-WAIT] {symbol} — just armed this bar, "
                          f"waiting for next 15m bar before checking rejection")

                else:
                    # TRIGGER conditions:
                    #   1. wick of this 15m candle came within ±RETEST_TOUCH_PCT of lowerLvl from below
                    #   2. close is back below lowerLvl
                    #   3. candle is bearish (c15 < o15)
                    #   4. body is reasonably strong (≥ RETEST_MIN_BODY_PCT of range)
                    #   5. cooldown clear
                    touch_floor = lower_lvl * (1 - RETEST_TOUCH_PCT / 100)
                    touch_ceil  = lower_lvl * (1 + RETEST_TOUCH_PCT / 100)

                    cond_touch     = touch_floor <= h15 <= touch_ceil
                    cond_close_dn  = c15 < lower_lvl
                    cond_bearish   = c15 < o15
                    cond_body_ok   = body_pct >= RETEST_MIN_BODY_PCT
                    cond_cd_ok     = cooldown_ok
                    cond_touches   = touches_below >= MIN_TL_TOUCHES

                    retest_sig = (cond_touch and cond_close_dn and cond_bearish
                                  and cond_body_ok and cond_cd_ok and cond_touches
                                  and not short_sig)

                    print(
                        f"[RETEST-EVAL] {symbol} | armed_lvl={round(armed_lvl, precision)} "
                        f"lowerLvl={round(lower_lvl, precision)} bars={bars_armed}/{RETEST_MAX_BARS} "
                        f"touches={touches_below}/{MIN_TL_TOUCHES} | "
                        f"touch={cond_touch}(h15={h15}) closeDn={cond_close_dn} "
                        f"bear={cond_bearish} body={cond_body_ok}({round(body_pct, 1)}%) "
                        f"cd={cond_cd_ok} → retest={retest_sig}"
                    )

                    if retest_sig:
                        retest_signal_info = {
                            "c15":             round(c15, precision),
                            "h15":             round(h15, precision),
                            "lower_lvl":       round(lower_lvl, precision),
                            "armed_lower_lvl": round(armed_lvl, precision),
                            "last_pl":         round(last_pl, precision),
                            "last_ph":         round(last_ph, precision),
                            "body_pct":        round(body_pct, 1),
                            "bars_armed":      int(bars_armed),
                            "touches_below":   touches_below,
                            "bars_since_last": (
                                (ts15 - last_entry_ts) // (ENTRY_CANDLE_SECONDS * 1000)
                                if last_entry_ts > 0 else "n/a"
                            ),
                        }

    # =========================================================================
    # ENTRY DISPATCH — main path takes priority
    # =========================================================================
    entry_path = None
    sig_info   = None

    if short_sig:
        entry_path = "tl_break"
        bars_since_last = (
            (ts15 - last_entry_ts) // (ENTRY_CANDLE_SECONDS * 1000)
            if last_entry_ts > 0 else "n/a"
        )
        sig_info = {
            "c15":             round(c15, precision),
            "prev_c15":        round(prev_c15, precision),
            "lower_lvl":       round(lower_lvl, precision),
            "last_pl":         round(last_pl, precision),
            "last_ph":         round(last_ph, precision),
            "max_oc":          round(max(o15, c15), precision),
            "body_pct":        round(body_pct, 1),
            "min_body_pct":    MIN_BODY_PCT,
            "vol":             round(v15, 2),
            "vol_mult":        VOL_MULT,
            "vol_threshold":   round(vol_sma * VOL_MULT, 2),
            "atr_mult":        ATR_MULT,
            "atr_threshold":   round(lower_lvl - atr_15 * ATR_MULT, precision),
            "bars_since_last": bars_since_last,
            "cooldown_bars":   COOLDOWN_BARS,
            "touches_below":   touches_below,
        }
    elif retest_sig:
        entry_path = "tl_retest"
        sig_info   = retest_signal_info

    if entry_path is None:
        save_state(all_state)
        return

    # ─── Compute SL/TP (same geometry for both paths) ───────
    # SL = SL_ABOVE_TL_PCT% above the lower trendline (broken support → resistance)
    # TP = fixed TP_PCT% below entry
    entry_price = c15
    sl_price    = lower_lvl * (1 + SL_ABOVE_TL_PCT / 100)
    tp_price    = entry_price * (1 - TP_PCT / 100)
    risk        = sl_price - entry_price

    if risk <= 0 or sl_price <= entry_price:
        print(f"[SKIP] {symbol} — invalid risk (entry {entry_price} ≥ SL {sl_price})")
        save_state(all_state)
        return

    # Last-second guards
    if get_position_by_pair(symbol) is not None:
        print(f"[ABORT] {symbol} — position appeared just before placement")
        return
    if has_open_order(symbol):
        print(f"[ABORT] {symbol} — order appeared just before placement")
        return

    placed = place_short_order(symbol, entry_price, tp_price, sl_price, precision, entry_path, sig_info)
    if placed:
        st["in_position"]   = True
        st["entry_path"]    = entry_path
        st["entry_price"]   = round(entry_price, precision)
        st["tp_level"]      = round(tp_price,    precision)
        st["sl_price"]      = round(sl_price,    precision)
        st["last_entry_ts"] = ts15
        # Clear retest arming on any successful entry
        st["retest_armed"]     = False
        st["retest_armed_ts"]  = 0
        st["retest_lower_lvl"] = None
        st["retest_last_pl"]   = None
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
    f"✅ <b>SHORT Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy   : <code>Trendline Break + Anti-Fakeout (SHORT only)</code>\n"
    f"⏱ TL / S-R   : <code>1h closed bars (lookback={SWING_LOOKBACK}, slope×{SLOPE_MULT})</code>\n"
    f"⚡ Entry      : <code>15m close confirmation</code>\n"
    f"🔁 Scan       : <code>Every 15 minutes</code>\n"
    f"🧪 Filters    : <code>body-break, body≥{MIN_BODY_PCT:.0f}%, vol&gt;SMA{VOL_SMA_PERIOD}×{VOL_MULT}, "
    f"break≥{ATR_MULT}×ATR{ATR_PERIOD_15M}, cooldown={COOLDOWN_BARS}×15m</code>\n"
    f"🛤 Paths      : <code>tl_break + tl_retest "
    f"(retest: {'ON' if USE_RETEST_PATH else 'OFF'}, "
    f"max {RETEST_MAX_BARS} bars, ±{RETEST_TOUCH_PCT}% touch, body≥{RETEST_MIN_BODY_PCT}%)</code>\n"
    f"🪢 Touches    : <code>require ≥ {MIN_TL_TOUCHES} prior failed-break attempts on the trendline</code>\n"
    f"🎯 TP         : <code>entry × (1 − {TP_PCT}%)</code>\n"
    f"🛑 SL         : <code>lowerLvl × (1 + {SL_ABOVE_TL_PCT}%)</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in scan interval")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"----- TRADE SCAN — CYCLE {cycle} -----")

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
                f"🚨 <b>SHORT Bot Crashed — Restarting</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ Error : <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive errors — triggering restart"
            )
            raise SystemExit(1)

        time.sleep(60)