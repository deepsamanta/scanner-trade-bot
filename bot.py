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
# STRATEGY: MOMENTUM IGNITION v2 — catch real pumps/dumps, kill fakeouts
#
# PHILOSOPHY: A real pump/dump has a signature that fakeouts can't fully fake:
#   sustained volume + strong candle bodies + follow-through + book support
#   + still moving at execution time. Each filter below attacks one fakeout
#   pattern. A fake move must fool ALL 9 simultaneously to trap us.
#
# THE 9 FILTER STACK:
#   ① 2-CANDLE CONFIRMATION — signal candle breaks level, next candle must
#      hold beyond it. Kills: single-candle stop hunts.
#   ② VOLUME SPIKE >= 2.0x  — signal candle vs 20-bar avg.
#      Kills: low-participation drift breaks.
#   ③ VOLUME ACCELERATION   — volume must be BUILDING (signal vol > previous
#      vol). Fakeouts are one spike then dead; real momentum snowballs.
#      Kills: single-spike pump-and-fades.
#   ④ BODY DOMINANCE >= 60% — signal candle body/range >= 0.6 and closing in
#      the move direction. A breakout candle with a long rejection wick means
#      the spike got sold into. Kills: wick-and-reject traps.
#   ⑤ MIN STRENGTH 0.3%     — close must clear the level decisively.
#      Kills: micro-tick breaks that mean nothing.
#   ⑥ EMA SLOPE FILTER      — 4H EMA50 must actively trend with the trade.
#      Kills: range-chop whipsaw (the #1 whipsaw source).
#   ⑦ 1H DIRECTIONAL CONFIRM — latest 1H candle directional + rising 1H vol.
#      Kills: 15m noise against the higher timeframe.
#   ⑧ ORDER BOOK GATE       — spread < 0.3%, slippage < 0.5% for our size,
#      no opposing wall (long blocked if asks dominate, short if bids dominate)
#      + up to +10 score pts when book leans with us.
#      Kills: illiquid traps + absorption walls.
#   ⑨ LIVE PRICE RE-CHECK   — at execution moment, fetch live 1m price. If
#      price already reversed back through the level, the move died while we
#      scanned. Abort. Kills: entering into an already-dead move.
#
# NOTE ON COMPRESSION: the old hard "must be compressed" gate is now a SCORE
#   BONUS (+8 pts) instead of a blocker. Reason: many violent pumps ignite
#   from already-volatile conditions; a hard compression gate misses them.
#   Coiled setups still rank higher, but raw momentum is never excluded.
#
# DIRECTION — per-coin: both long and short evaluated every cycle via the
#   coin's own 4H EMA50. BTC daily 200 EMA = +10 pts alignment bonus only.
#
# TP/SL — ATR-BASED: TP = ATR x 3.0, SL = ATR x 1.5 (floors 4% / 2%). R:R 2:1.
#
# STALE-TRADE ALERT: if a position hasn't moved >= +1% in our favor within
#   8 x 15m candles (2 hours), a Telegram warning fires — momentum trades
#   should work fast; a stalling one is statistically more likely to be a trap.
# =============================================================================

# ── Trade params ──────────────────────────────────────────────────────────────
MAX_OPEN_TRADES   = 12

# ── ATR-based TP/SL ──────────────────────────────────────────────────────────
# Structure-based SL + R-multiple TP (fully volatility-adaptive, no fixed % floors)
SL_LEVEL_BUFFER   = 0.5    # SL sits this many ATR BEYOND the broken level
SL_MIN_ATR        = 0.8    # SL distance never tighter than this x ATR (whipsaw guard)
SL_MAX_ATR        = 2.5    # SL distance never wider than this x ATR (risk cap)
RR_TARGET         = 1.6    # TP = this x actual risk distance (achievable, scales per coin)

# ── INSTITUTIONAL DEFENSE ─────────────────────────────────────────────────────
# RETEST ENTRY: never chase the breakout candle (that's where institutions
# sell into bot buying). Instead a LIMIT order rests at the broken level —
# the pullback that used to stop us out now FILLS us at institutional prices.
RETEST_ENTRY           = True
RETEST_BUFFER_ATR      = 0.15  # limit sits this many ATR beyond the level (toward
                               # current price) so a wick-touch of the zone fills us
PENDING_EXPIRY_CANDLES = 6     # cancel unfilled retest order after 6 x 15m (stale signal)

# VWAP FILTER: session VWAP (anchored 00:00 UTC) = the institutional benchmark.
# Price above VWAP = buyers in control -> longs only. Below = shorts only.
# Never fight the side institutions are actually executing on.
VWAP_FILTER            = True

# ── Universe filter ───────────────────────────────────────────────────────────
MIN_24H_VOL_USDT  = 3_000_000

STABLECOINS = {
    "USDT","USDC","BUSD","DAI","TUSD","USDP","FRAX","UST","LUSD",
    "FDUSD","PYUSD","USDD","USDN","GUSD","SUSD","CUSD","USDX","OUSD",
}
WRAPPED = {"WBTC","WETH","WBNB","WMATIC","WAVAX","WSOL","WFTM"}

# ── Momentum strategy params ──────────────────────────────────────────────────
EMA200_DAILY_LEN  = 200
EMA50_4H_LEN      = 50
EMA_SLOPE_BARS    = 5
ATR_LEN           = 14
ATR_COMPRESS_PCT  = 2.5    # compression now a score bonus, not a gate
COMPRESS_BONUS    = 8      # score pts if coiled before ignition
BREAKOUT_BARS     = 20
VOL_SPIKE_MULT    = 2.0    # filter ②
MIN_BREAKOUT_PCT  = 0.3    # filter ⑤
BODY_DOMINANCE    = 0.60   # filter ④ — body must be >= 60% of candle range
HTF_VOL_BARS      = 2
RANGE_LOOKBACK    = 480
RANGE_SKIP_PCT    = 15
REGIME_BONUS_PTS  = 10

# ── Order book gate params (filter ⑧) ─────────────────────────────────────────
OB_DEPTH            = 50
MAX_SPREAD_PCT      = 0.30
MAX_SLIPPAGE_PCT    = 0.50
DEPTH_RANGE_PCT     = 1.0
MIN_IMBALANCE_LONG  = 0.40
MAX_IMBALANCE_SHORT = 0.60
OB_BONUS_MAX_PTS    = 10

# ── Stale trade alert ─────────────────────────────────────────────────────────
STALE_CANDLES     = 8      # 8 x 15m = 2 hours
STALE_MIN_MOVE_PCT = 1.0   # expect >= +1% in favor by then

# ── Candle counts ─────────────────────────────────────────────────────────────
CANDLES_15M       = 550
CANDLES_4H        = 70
CANDLES_1H        = 30
CANDLES_1M        = 5

# ── Resolutions ───────────────────────────────────────────────────────────────
RESOLUTION_15M    = "15"
RESOLUTION_1M     = "1"
RESOLUTION_DAILY  = "1D"
RESOLUTION_1H     = "60"
RESOLUTION_4H     = "240"

CANDLE_SECONDS_15M = 900
CANDLE_SECONDS_1M  = 60
CANDLE_SECONDS_DAY = 86400
CANDLE_SECONDS_1H  = 3600
CANDLE_SECONDS_4H  = 14400

SCAN_INTERVAL          = 120
REQUEST_TIMEOUT        = 15
TELEGRAM_TIMEOUT       = 10
GSHEET_REAUTH_INTERVAL = 45 * 60
STATE_FILE             = "atl_bot_state.json"


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
        "stale_alerted":   False,
        "pending_since":   0,
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
                print(f"[TELEGRAM] Rate limited — waiting {retry_after}s")
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


def cancel_order(order_id):
    """Cancel a resting futures order by id. Used to expire stale retest limits."""
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "id":        str(order_id),
        }
        payload, headers = sign_request(body)
        r = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/cancel",
            data=payload, headers=headers, timeout=REQUEST_TIMEOUT,
        )
        result = r.json() if r.status_code == 200 else {"http": r.status_code}
        print(f"  [CANCEL] order {order_id} -> {result}")
        return r.status_code == 200
    except Exception as e:
        print(f"  [CANCEL] order {order_id} error: {e}")
        return False


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
# MATH UTILITIES
# =====================================================

def compute_ema(values, length):
    if len(values) < length:
        return None
    k   = 2 / (length + 1)
    ema = sum(values[:length]) / length
    for v in values[length:]:
        ema = v * k + ema * (1 - k)
    return ema


def compute_atr(candles, length):
    if len(candles) < length + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h  = float(candles[i]["high"])
        l  = float(candles[i]["low"])
        pc = float(candles[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < length:
        return None
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


def compute_session_vwap(candles_15m):
    """
    Session VWAP anchored at 00:00 UTC — the institutional benchmark price.
    VWAP = cumulative(typical_price x volume) / cumulative(volume)
    where typical_price = (high + low + close) / 3.
    If the UTC session is too young (< 4 candles), falls back to a rolling
    24h VWAP (last 96 x 15m candles) so early-session readings aren't noise.
    Returns (vwap: float | None, source: str).
    """
    if not candles_15m:
        return None, "no data"
    now = datetime.now(timezone.utc)
    midnight_ms = int(datetime(now.year, now.month, now.day,
                               tzinfo=timezone.utc).timestamp() * 1000)
    session = [c for c in candles_15m if int(c["time"]) >= midnight_ms]
    source  = "session (00:00 UTC)"
    if len(session) < 4:
        session = candles_15m[-96:]
        source  = "rolling 24h (session too young)"
    pv_sum = 0.0
    v_sum  = 0.0
    for c in session:
        tp = (float(c["high"]) + float(c["low"]) + float(c["close"])) / 3
        v  = float(c["volume"])
        pv_sum += tp * v
        v_sum  += v
    if v_sum <= 0:
        return None, "zero volume"
    return pv_sum / v_sum, source


def compute_atr_tp_sl(entry, candles_15m, direction, precision, level=None):
    """
    STRUCTURE-BASED SL + R-MULTIPLE TP — fully adaptive per coin, no fixed % floors.

    SL (whipsaw-resistant):
      Placed SL_LEVEL_BUFFER x ATR BEYOND the broken level — not a blind
      distance from entry. Logic: after a real breakout, the broken level acts
      as support (long) / resistance (short). Normal noise retests the level;
      only a genuine breakout FAILURE trades meaningfully beyond it. The 0.5
      ATR buffer means wick-retests of the level don't tag the stop.
      Distance is clamped to [SL_MIN_ATR, SL_MAX_ATR] x ATR:
        - floor stops it being inside the noise band (whipsaw guard)
        - cap stops one entry far from the level carrying oversized risk.

    TP (always achievable):
      TP = RR_TARGET x actual risk distance. Because risk is derived from the
      coin's own ATR + structure, TP automatically scales per coin:
        quiet coin  -> small ATR -> tight SL -> small, easily reachable TP
        wild coin   -> big ATR   -> wide SL  -> big TP that matches its moves
      No coin is ever assigned a 4% target it statistically can't reach.
    """
    atr = compute_atr(candles_15m[-50:], ATR_LEN) if len(candles_15m) >= ATR_LEN + 1 else None
    if not atr or atr <= 0:
        atr = entry * 0.01   # emergency fallback: treat 1% as one ATR

    if direction == "long":
        if level and level < entry:
            structure_sl = level - SL_LEVEL_BUFFER * atr
            sl_dist      = entry - structure_sl
        else:
            sl_dist = SL_MIN_ATR * atr
    else:
        if level and level > entry:
            structure_sl = level + SL_LEVEL_BUFFER * atr
            sl_dist      = structure_sl - entry
        else:
            sl_dist = SL_MIN_ATR * atr

    # Clamp: never inside the noise band, never oversized
    sl_dist = max(SL_MIN_ATR * atr, min(sl_dist, SL_MAX_ATR * atr))
    tp_dist = sl_dist * RR_TARGET

    if direction == "long":
        return round(entry + tp_dist, precision), round(entry - sl_dist, precision)
    else:
        return round(entry - tp_dist, precision), round(entry + sl_dist, precision)


# =====================================================
# ORDER BOOK GATE  (filter ⑧)
# =====================================================

def fetch_orderbook(symbol):
    try:
        url = (f"https://public.coindcx.com/market_data/v3/orderbook/"
               f"{fut_pair(symbol)}-futures/{OB_DEPTH}")
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            print(f"  [OB] {symbol}  HTTP {r.status_code}")
            return None, None
        data = r.json()
        raw_bids = data.get("bids", {}) or {}
        raw_asks = data.get("asks", {}) or {}
        bids = sorted(((float(p), float(q)) for p, q in raw_bids.items()),
                      key=lambda x: x[0], reverse=True)
        asks = sorted(((float(p), float(q)) for p, q in raw_asks.items()),
                      key=lambda x: x[0])
        if not bids or not asks:
            print(f"  [OB] {symbol}  empty book (bids={len(bids)} asks={len(asks)})")
            return None, None
        return bids, asks
    except Exception as e:
        print(f"  [OB] {symbol}  error: {e}")
        return None, None


def estimate_slippage(levels, qty_needed):
    remaining = qty_needed
    cost      = 0.0
    for price, qty in levels:
        take       = min(remaining, qty)
        cost      += take * price
        remaining -= take
        if remaining <= 0:
            break
    filled_qty = qty_needed - max(remaining, 0)
    if filled_qty <= 0:
        return None, False
    return cost / filled_qty, remaining <= 0


def check_orderbook_gate(symbol, direction, entry_price, order_qty):
    bids, asks = fetch_orderbook(symbol)
    if bids is None:
        return True, "book unavailable (not blocking)", 0.5, 0.0

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid      = (best_bid + best_ask) / 2

    spread_pct = ((best_ask - best_bid) / mid) * 100 if mid > 0 else 999.0
    if spread_pct > MAX_SPREAD_PCT:
        return False, f"spread {spread_pct:.3f}% > {MAX_SPREAD_PCT}%", 0.5, round(spread_pct, 4)

    consume_side = asks if direction == "long" else bids
    best_price   = best_ask if direction == "long" else best_bid
    avg_fill, fully = estimate_slippage(consume_side, order_qty)
    if avg_fill is None or not fully:
        return False, (f"insufficient depth for qty={order_qty} "
                       f"within {OB_DEPTH} levels"), 0.5, round(spread_pct, 4)
    slip_pct = abs((avg_fill - best_price) / best_price) * 100 if best_price > 0 else 999.0
    if slip_pct > MAX_SLIPPAGE_PCT:
        return False, (f"slippage {slip_pct:.3f}% > {MAX_SLIPPAGE_PCT}%"), 0.5, round(spread_pct, 4)

    lo_bound  = mid * (1 - DEPTH_RANGE_PCT / 100)
    hi_bound  = mid * (1 + DEPTH_RANGE_PCT / 100)
    bid_depth = sum(q for p, q in bids if p >= lo_bound)
    ask_depth = sum(q for p, q in asks if p <= hi_bound)
    total     = bid_depth + ask_depth
    imbalance = (bid_depth / total) if total > 0 else 0.5

    print(f"  [OB] {symbol}  spread={spread_pct:.3f}%  slip={slip_pct:.3f}%  "
          f"bid_depth={bid_depth:,.2f}  ask_depth={ask_depth:,.2f}  imb={imbalance:.3f}")

    if direction == "long" and imbalance < MIN_IMBALANCE_LONG:
        return False, f"sell wall overhead (imb {imbalance:.3f} < {MIN_IMBALANCE_LONG})", imbalance, round(spread_pct, 4)
    if direction == "short" and imbalance > MAX_IMBALANCE_SHORT:
        return False, f"buy wall below (imb {imbalance:.3f} > {MAX_IMBALANCE_SHORT})", imbalance, round(spread_pct, 4)

    return True, "ok", imbalance, round(spread_pct, 4)


def orderbook_score_bonus(imbalance, direction):
    if direction == "long":
        lean = max(0.0, imbalance - 0.5) / 0.5
    else:
        lean = max(0.0, 0.5 - imbalance) / 0.5
    return round(lean * OB_BONUS_MAX_PTS, 4)


# =====================================================
# LIVE PRICE RE-CHECK  (filter ⑨)
# =====================================================

def get_live_price(symbol):
    """Latest 1m close — freshest price available before firing an order."""
    try:
        candles = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        if not candles:
            return None
        return float(candles[-1]["close"])
    except Exception:
        return None


def live_momentum_intact(symbol, direction, level):
    """
    Filter ⑨: just before entry, verify the live price is STILL beyond the
    broken level. If price fell back through, the move died while we scanned.
    Returns (intact: bool, live_price: float|None).
    """
    live = get_live_price(symbol)
    if live is None:
        return True, None   # data hiccup — don't block, order book already passed
    if direction == "long":
        return live > level, live
    else:
        return live < level, live


# =====================================================
# STAGE 1 — VOLUME FROM DAILY FUTURES CANDLE
# =====================================================

def fetch_24h_volume(symbol):
    try:
        now        = int(time.time())
        from_time  = now - (3 * 24 * 60 * 60)
        url        = "https://public.coindcx.com/market_data/candlesticks"
        params     = {
            "pair":       fut_pair(symbol),
            "from":       from_time,
            "to":         now,
            "resolution": RESOLUTION_DAILY,
            "pcode":      "f",
        }
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"  [VOL] {symbol}  HTTP {response.status_code}")
            return 0.0, None
        data        = response.json()
        candle_list = data.get("data", data) if isinstance(data, dict) else data
        if not candle_list:
            print(f"  [VOL] {symbol}  no daily candles returned")
            return 0.0, None
        candle_list = sorted(candle_list, key=lambda x: x["time"])
        daily       = candle_list[-1]
        volume_qty  = float(daily.get("volume", 0) or 0)
        close_px    = float(daily.get("close",  0) or 0)
        vol_usd     = volume_qty * close_px
        print(f"  [VOL] {symbol}  raw_daily_candle={daily}")
        print(f"  [VOL] {symbol}  volume_qty={volume_qty:,.2f} x close={close_px} "
              f"=> 24h_usd=${vol_usd:,.0f}")
        return vol_usd, daily
    except Exception as e:
        print(f"  [VOL] {symbol}  error: {e}")
        return 0.0, None


def is_excluded(symbol):
    base = symbol.replace("USDT", "")
    return base in STABLECOINS or base in WRAPPED


def build_eligible_universe(all_symbols_rows):
    eligible    = []
    skip_stable = 0
    for symbol, row in all_symbols_rows:
        if is_excluded(symbol):
            skip_stable += 1
            print(f"  [{symbol}] DISCARDED — stablecoin or wrapped token")
            continue
        eligible.append((symbol, row))
    print(f"\n[UNIVERSE] {len(all_symbols_rows)} in sheet | "
          f"-{skip_stable} stables/wrapped | "
          f"-> {len(eligible)} proceeding to per-coin scan")
    return eligible


def get_btc_regime():
    try:
        candles = fetch_candles("BTCUSDT", 220, RESOLUTION_DAILY, CANDLE_SECONDS_DAY)
        if len(candles) < EMA200_DAILY_LEN:
            return True, 0.0, 0.0
        closes  = [float(c["close"]) for c in candles]
        ema200  = compute_ema(closes, EMA200_DAILY_LEN)
        is_bull = closes[-1] > ema200 if ema200 else True
        label   = "BULL (longs +10 pts)" if is_bull else "BEAR (shorts +10 pts)"
        print(f"[BTC REGIME] close={closes[-1]:,.2f}  EMA200={ema200:,.2f}  {label}")
        return is_bull, closes[-1], ema200 or 0.0
    except Exception as e:
        print(f"[BTC REGIME] Error: {e} — defaulting BULL")
        return True, 0.0, 0.0


# =====================================================
# STAGE 2 — STRUCTURAL SCREEN
# =====================================================

def check_4h_uptrend(candles_4h):
    if len(candles_4h) < EMA50_4H_LEN:
        return False, 0.0, 0.0
    closes = [float(c["close"]) for c in candles_4h]
    ema50  = compute_ema(closes, EMA50_4H_LEN)
    if ema50 is None:
        return False, 0.0, 0.0
    last = closes[-1]
    return last > ema50, round(last, 8), round(ema50, 8)


def check_4h_downtrend(candles_4h):
    if len(candles_4h) < EMA50_4H_LEN:
        return False, 0.0, 0.0
    closes = [float(c["close"]) for c in candles_4h]
    ema50  = compute_ema(closes, EMA50_4H_LEN)
    if ema50 is None:
        return False, 0.0, 0.0
    last = closes[-1]
    return last < ema50, round(last, 8), round(ema50, 8)


def check_ema_slope(candles_4h, direction):
    """Filter ⑥ — EMA50 must actively trend with the trade. Flat = whipsaw zone."""
    needed = EMA50_4H_LEN + EMA_SLOPE_BARS + 1
    if len(candles_4h) < needed:
        return True, 0.0, 0.0
    closes   = [float(c["close"]) for c in candles_4h]
    ema_now  = compute_ema(closes,                 EMA50_4H_LEN)
    ema_prev = compute_ema(closes[:-EMA_SLOPE_BARS], EMA50_4H_LEN)
    if ema_now is None or ema_prev is None:
        return True, 0.0, 0.0
    ok = ema_now > ema_prev if direction == "long" else ema_now < ema_prev
    return ok, round(ema_now, 8), round(ema_prev, 8)


def check_1h_compression(candles_1h):
    """Now a SCORE BONUS check, not a gate. Coiled = better setup, but raw momentum still qualifies."""
    atr = compute_atr(candles_1h, ATR_LEN)
    if atr is None:
        return False, 0.0, 0.0
    last_close = float(candles_1h[-1]["close"])
    atr_pct    = (atr / last_close) * 100 if last_close > 0 else 999.0
    return atr_pct < ATR_COMPRESS_PCT, round(atr_pct, 4), round(atr, 8)


def check_range_not_extended(candles_15m):
    if len(candles_15m) < RANGE_LOOKBACK:
        return False, 0.0
    window  = candles_15m[-RANGE_LOOKBACK:]
    lo      = min(float(c["low"])  for c in window)
    hi      = max(float(c["high"]) for c in window)
    rng_pct = round(((hi - lo) / lo) * 100, 2) if lo > 0 else 0.0
    return rng_pct >= RANGE_SKIP_PCT, rng_pct


# =====================================================
# STAGE 3 — MOMENTUM IGNITION SIGNAL
# (filters ① ② ③ ④ ⑤ packed into one check per direction)
# =====================================================

def check_momentum_ignition_long(candles_15m):
    """
    LONG ignition:
      signal  = candles[-2], confirm = candles[-1], base = 20 bars before signal
      ① confirm close > 20-bar high (follow-through held)
      ② signal vol >= 2.0x base avg (spike)
      ③ signal vol > previous candle vol (volume BUILDING, not one-off)
      ④ signal body/range >= 0.60 and bullish close (no rejection wick)
      ⑤ signal close >= 0.3% above the 20-bar high (decisive)
    Returns (ok, confirm_close, level, vol_ratio, strength_pct, body_pct, vol_building, fail_reason)
    """
    needed = BREAKOUT_BARS + 3
    if len(candles_15m) < needed:
        return False, 0, 0, 0, 0, 0, False, "insufficient candles"

    signal   = candles_15m[-2]
    confirm  = candles_15m[-1]
    prior    = candles_15m[-3]
    base     = candles_15m[-(BREAKOUT_BARS + 2):-2]

    sig_o, sig_h = float(signal["open"]),  float(signal["high"])
    sig_l, sig_c = float(signal["low"]),   float(signal["close"])
    sig_v        = float(signal["volume"])
    conf_c       = float(confirm["close"])
    prior_v      = float(prior["volume"])

    level   = max(float(c["close"]) for c in base)
    avg_vol = sum(float(c["volume"]) for c in base) / len(base) if base else 0

    vol_ratio    = sig_v / avg_vol if avg_vol > 0 else 0.0
    strength_pct = ((sig_c - level) / level * 100) if level > 0 else 0.0
    rng          = sig_h - sig_l
    body_pct     = (abs(sig_c - sig_o) / rng) if rng > 0 else 0.0
    vol_building = sig_v > prior_v
    is_bullish   = sig_c > sig_o

    if not (sig_c > level):
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "signal didn't break level"
    if strength_pct < MIN_BREAKOUT_PCT:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"strength {strength_pct:.3f}% < {MIN_BREAKOUT_PCT}%"
    if vol_ratio < VOL_SPIKE_MULT:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"vol {vol_ratio:.2f}x < {VOL_SPIKE_MULT}x"
    if not vol_building:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "volume not building (single spike)"
    if not is_bullish or body_pct < BODY_DOMINANCE:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"weak body {body_pct:.2f} < {BODY_DOMINANCE} (rejection wick)"
    if not (conf_c > level):
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "confirm candle fell back below level"

    return True, round(conf_c, 8), round(level, 8), round(vol_ratio, 2), \
           round(strength_pct, 4), round(body_pct, 3), vol_building, "ok"


def check_momentum_ignition_short(candles_15m):
    """SHORT ignition — exact mirror of the long check."""
    needed = BREAKOUT_BARS + 3
    if len(candles_15m) < needed:
        return False, 0, 0, 0, 0, 0, False, "insufficient candles"

    signal   = candles_15m[-2]
    confirm  = candles_15m[-1]
    prior    = candles_15m[-3]
    base     = candles_15m[-(BREAKOUT_BARS + 2):-2]

    sig_o, sig_h = float(signal["open"]),  float(signal["high"])
    sig_l, sig_c = float(signal["low"]),   float(signal["close"])
    sig_v        = float(signal["volume"])
    conf_c       = float(confirm["close"])
    prior_v      = float(prior["volume"])

    level   = min(float(c["close"]) for c in base)
    avg_vol = sum(float(c["volume"]) for c in base) / len(base) if base else 0

    vol_ratio    = sig_v / avg_vol if avg_vol > 0 else 0.0
    strength_pct = ((level - sig_c) / level * 100) if level > 0 else 0.0
    rng          = sig_h - sig_l
    body_pct     = (abs(sig_c - sig_o) / rng) if rng > 0 else 0.0
    vol_building = sig_v > prior_v
    is_bearish   = sig_c < sig_o

    if not (sig_c < level):
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "signal didn't break level"
    if strength_pct < MIN_BREAKOUT_PCT:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"strength {strength_pct:.3f}% < {MIN_BREAKOUT_PCT}%"
    if vol_ratio < VOL_SPIKE_MULT:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"vol {vol_ratio:.2f}x < {VOL_SPIKE_MULT}x"
    if not vol_building:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "volume not building (single spike)"
    if not is_bearish or body_pct < BODY_DOMINANCE:
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, f"weak body {body_pct:.2f} < {BODY_DOMINANCE} (rejection wick)"
    if not (conf_c < level):
        return False, conf_c, level, vol_ratio, strength_pct, body_pct, vol_building, "confirm candle bounced back above level"

    return True, round(conf_c, 8), round(level, 8), round(vol_ratio, 2), \
           round(strength_pct, 4), round(body_pct, 3), vol_building, "ok"


def check_1h_bullish_confirmation(candles_1h):
    needed = HTF_VOL_BARS * 2
    if len(candles_1h) < needed:
        return False, 0.0, 0.0, False
    recent_vols = [float(c["volume"]) for c in candles_1h[-HTF_VOL_BARS:]]
    prev_vols   = [float(c["volume"]) for c in candles_1h[-(HTF_VOL_BARS * 2):-HTF_VOL_BARS]]
    avg_recent  = sum(recent_vols) / len(recent_vols)
    avg_prev    = sum(prev_vols)   / len(prev_vols)
    last        = candles_1h[-1]
    is_bullish  = float(last["close"]) > float(last["open"])
    return (avg_recent > avg_prev and is_bullish), round(avg_recent, 2), round(avg_prev, 2), is_bullish


def check_1h_bearish_confirmation(candles_1h):
    needed = HTF_VOL_BARS * 2
    if len(candles_1h) < needed:
        return False, 0.0, 0.0, False
    recent_vols = [float(c["volume"]) for c in candles_1h[-HTF_VOL_BARS:]]
    prev_vols   = [float(c["volume"]) for c in candles_1h[-(HTF_VOL_BARS * 2):-HTF_VOL_BARS]]
    avg_recent  = sum(recent_vols) / len(recent_vols)
    avg_prev    = sum(prev_vols)   / len(prev_vols)
    last        = candles_1h[-1]
    is_bearish  = float(last["close"]) < float(last["open"])
    return (avg_recent > avg_prev and is_bearish), round(avg_recent, 2), round(avg_prev, 2), is_bearish


# =====================================================
# STAGE 4 — SCORING
# =====================================================

def score_candidate(vol_ratio, move_strength_pct, ema_proximity_pct,
                    vol_24h_usd, direction, btc_bull,
                    ob_bonus=0.0, body_pct=0.0, compressed=False):
    """
    Base:
      vol spike      25 pts  (5x = max)
      move strength  15 pts  (5% = max)
      body quality   12 pts  (body_pct 0.6->1.0 scales 0->12)
      EMA proximity  18 pts  (0% = max, 10%+ = 0)
      liquidity      30 pts  ($50M = max)
    Bonuses:
      BTC regime alignment  +10
      order book lean       up to +10
      compression (coiled)  +8
    """
    s1 = min(vol_ratio / 5.0,              1.0) * 25
    s2 = min(move_strength_pct / 5.0,      1.0) * 15
    s3 = max(0.0, (body_pct - BODY_DOMINANCE) / (1 - BODY_DOMINANCE)) * 12
    s4 = max(0, 1 - ema_proximity_pct / 10)    * 18
    s5 = min(vol_24h_usd / 50_000_000,     1.0) * 30
    regime_aligned = (direction == "long" and btc_bull) or (direction == "short" and not btc_bull)
    s6 = REGIME_BONUS_PTS if regime_aligned else 0
    s7 = COMPRESS_BONUS if compressed else 0
    return round(s1 + s2 + s3 + s4 + s5 + s6 + s7 + ob_bonus, 4)


# =====================================================
# CANDLE FETCHERS
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


def get_recent_low(symbol):
    try:
        now    = int(time.time())
        params = {"pair": fut_pair(symbol), "from": now - SCAN_INTERVAL,
                  "to": now, "resolution": "1", "pcode": "f"}
        candles = requests.get(
            "https://public.coindcx.com/market_data/candlesticks",
            params=params, timeout=REQUEST_TIMEOUT,
        ).json().get("data", [])
        return min(float(c["low"]) for c in candles) if candles else None
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
# PLACE ORDERS
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision):
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
    tp_pct = round(((tp - entry) / entry) * 100, 2)
    sl_pct = round(((entry - sl) / entry) * 100, 2)
    print(f"  [LONG] Entry={entry}  TP={tp}(+{tp_pct}%)  SL={sl}(-{sl_pct}%)  Qty={qty}")
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
        send_telegram(f"❌ <b>LONG REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None
    send_telegram(
        f"🟢 <b>NEW LONG (MOMENTUM IGNITION) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (+{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (-{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True, entry, tp


def place_short_order(symbol, entry_price, tp_price, sl_price, precision):
    entry  = round(entry_price, precision)
    tp     = round(tp_price,    precision)
    sl     = round(sl_price,    precision)
    qty    = compute_qty(entry_price, symbol)
    tp_pct = round(((entry - tp) / entry) * 100, 2)
    sl_pct = round(((sl - entry) / entry) * 100, 2)
    print(f"  [SHORT] Entry={entry}  TP={tp}(-{tp_pct}%)  SL={sl}(+{sl_pct}%)  Qty={qty}")
    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side": "sell", "pair": fut_pair(symbol),
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
        send_telegram(f"❌ <b>SHORT REJECTED — {symbol}</b>\n<code>{str(result)[:200]}</code>")
        return False, None, None
    send_telegram(
        f"🔴 <b>NEW SHORT (MOMENTUM IGNITION) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry : <code>{entry}</code>\n"
        f"🎯 TP    : <code>{tp}</code>  (-{tp_pct}%)\n"
        f"🛑 SL    : <code>{sl}</code>  (+{sl_pct}%)\n"
        f"📦 Qty   : <code>{qty}</code>\n"
        f"💰 Margin: <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
    )
    return True, entry, tp


# =====================================================
# EXECUTE ENTRY (order book gate + live price re-check)
# =====================================================

def execute_entry(cand, all_state):
    symbol      = cand["symbol"]
    row         = cand["row"]
    entry_price = cand["entry_price"]
    tp_price    = cand["tp_price"]
    sl_price    = cand["sl_price"]
    precision   = cand["precision"]
    curr_ts     = cand["curr_ts"]
    direction   = cand["direction"]
    level       = cand["level"]

    # ── Filter ⑨: LIVE PRICE RE-CHECK ────────────────────────────────────────
    intact, live = live_momentum_intact(symbol, direction, level)
    if not intact:
        print(f"  [{symbol}] ENTRY ABORTED — momentum died "
              f"(live={live} vs level={level}, direction={direction})")
        send_telegram(
            f"⚠️ <b>ENTRY ABORTED (MOVE DIED) — {symbol}</b>\n"
            f"Direction: <code>{direction.upper()}</code>\n"
            f"Live price <code>{live}</code> reversed back through "
            f"level <code>{level}</code> before entry."
        )
        return
    print(f"  [{symbol}] Live momentum intact — live={live} vs level={level}")

    # ── Filter ⑧: ORDER BOOK final gate ──────────────────────────────────────
    order_qty = compute_qty(entry_price, symbol)
    ob_ok, ob_reason, ob_imb, ob_spread = check_orderbook_gate(
        symbol, direction, entry_price, order_qty)
    if not ob_ok:
        print(f"  [{symbol}] ENTRY BLOCKED by order book — {ob_reason}")
        send_telegram(
            f"🚫 <b>ENTRY BLOCKED (ORDER BOOK) — {symbol}</b>\n"
            f"Direction: <code>{direction.upper()}</code>\n"
            f"Reason: <code>{ob_reason}</code>"
        )
        return
    print(f"  [{symbol}] Order book gate PASS — imb={ob_imb:.3f} spread={ob_spread}%")

    st = all_state.setdefault(symbol, init_symbol_state())

    if direction == "long":
        placed, confirmed_entry, confirmed_tp = place_long_order(
            symbol, entry_price, tp_price, sl_price, precision)
    else:
        placed, confirmed_entry, confirmed_tp = place_short_order(
            symbol, entry_price, tp_price, sl_price, precision)

    if placed:
        if RETEST_ENTRY:
            # Resting limit — NOT in position until the retest actually fills.
            # Reconciliation flips in_position when the exchange shows the fill.
            st["in_position"]   = False
            st["pending_since"] = int(time.time() * 1000)
        else:
            st["in_position"]   = True
        st["direction"]     = direction
        st["entry_price"]   = confirmed_entry
        st["tp_level"]      = confirmed_tp
        st["sl_price"]      = round(sl_price, precision)
        st["last_entry_ts"] = curr_ts
        st["stale_alerted"] = False
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# STALE TRADE MONITOR
# =====================================================

def check_stale_trade(symbol, st):
    """
    Momentum trades should work FAST. If a position hasn't moved
    STALE_MIN_MOVE_PCT in our favor within STALE_CANDLES x 15m,
    fire a one-time Telegram warning — stalling momentum = likely trap.
    """
    if not st.get("in_position") or st.get("stale_alerted"):
        return
    entry_ts = st.get("last_entry_ts", 0)
    entry_px = st.get("entry_price")
    if not entry_ts or not entry_px:
        return
    age_ms = int(time.time() * 1000) - entry_ts
    if age_ms < STALE_CANDLES * CANDLE_SECONDS_15M * 1000:
        return
    live = get_live_price(symbol)
    if live is None:
        return
    if st.get("direction") == "long":
        move_pct = (live - entry_px) / entry_px * 100
    else:
        move_pct = (entry_px - live) / entry_px * 100
    if move_pct < STALE_MIN_MOVE_PCT:
        st["stale_alerted"] = True
        print(f"  [{symbol}] STALE TRADE — {move_pct:.2f}% after "
              f"{STALE_CANDLES} candles (expected >= {STALE_MIN_MOVE_PCT}%)")
        send_telegram(
            f"🐌 <b>STALE MOMENTUM — {symbol}</b>\n"
            f"Direction: <code>{st.get('direction','').upper()}</code>\n"
            f"Entry: <code>{entry_px}</code>  Live: <code>{live}</code>\n"
            f"Move: <code>{move_pct:+.2f}%</code> after 2 hours "
            f"(expected >= +{STALE_MIN_MOVE_PCT}%)\n"
            f"⚠️ Stalling momentum is statistically more likely a trap — "
            f"consider manual exit."
        )


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state, global_positions, global_orders, btc_bull=True):
    now_ms    = int(time.time() * 1000)
    pair_name = fut_pair(symbol)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── 1. Fetch 15m candles ──────────────────────────────────────────────────
    candles_15m = fetch_candles(symbol, CANDLES_15M, RESOLUTION_15M, CANDLE_SECONDS_15M)
    if candles_15m and (now_ms - int(candles_15m[-1]["time"])) < CANDLE_SECONDS_15M * 1000:
        candles_15m = candles_15m[:-1]

    min_15m = RANGE_LOOKBACK + BREAKOUT_BARS + 5
    if len(candles_15m) < min_15m:
        print(f"  [{symbol}] DISCARDED — not enough history "
              f"({len(candles_15m)} candles, need {min_15m})")
        return None

    # ── 2. State init / backfill ──────────────────────────────────────────────
    st = all_state.setdefault(symbol, init_symbol_state())
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # ── 3. New-day reset ──────────────────────────────────────────────────────
    if st["current_day_str"] != today_str:
        print(f"  [{symbol}] NEW DAY — resetting daily state")
        preserved = {k: st[k] for k in
                     ("in_position", "direction", "entry_price",
                      "tp_level", "sl_price", "last_entry_ts", "stale_alerted")}
        st = init_symbol_state()
        st.update(preserved)
        all_state[symbol] = st

    st["current_day_str"] = today_str
    precision = get_precision(float(candles_15m[-1]["close"]))

    # ── 4. TP COMPLETED check ─────────────────────────────────────────────────
    tp_raw = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
    if tp_raw.upper() == "TP COMPLETED" or st.get("tp_completed") is True:
        print(f"  [{symbol}] SKIP — TP already completed")
        if st.get("in_position"):
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
        return None

    # ── 5. Resolve TP target + check if hit ───────────────────────────────────
    tp_stored = st.get("tp_level")
    if not tp_stored:
        try:
            v = float(tp_raw)
            if v > 0:
                tp_stored      = v
                st["tp_level"] = v
        except (ValueError, TypeError):
            tp_stored = None

    # TP-hit monitoring only applies to FILLED positions. A pending retest
    # limit has a tp_level stored but no position — a price wick to that TP
    # must not falsely mark "TP COMPLETED" on a trade that never happened.
    is_pending_unfilled = bool(st.get("pending_since")) and not st.get("in_position")

    if tp_stored and tp_stored > 0 and not is_pending_unfilled:
        existing_dir = st.get("direction") or "long"
        last_1m    = fetch_candles(symbol, CANDLES_1M, RESOLUTION_1M, CANDLE_SECONDS_1M)
        last_close = float(last_1m[-1]["close"]) if last_1m else None
        tp_hit = False; hit_kind = None; hit_price = None
        if existing_dir == "long":
            tp_threshold = tp_stored * 0.9999
            if last_close and last_close >= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rh = get_recent_high(symbol)
                if rh and rh >= tp_threshold:
                    tp_hit, hit_kind, hit_price = True, "wick", rh
        else:
            tp_threshold = tp_stored * 1.0001
            if last_close and last_close <= tp_threshold:
                tp_hit, hit_kind, hit_price = True, "close", last_close
            if not tp_hit:
                rl = get_recent_low(symbol)
                if rl and rl <= tp_threshold:
                    tp_hit, hit_kind, hit_price = True, "wick", rl
        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"  [{symbol}] TP HIT ({hit_kind}) price={hit_price} target={tp_stored}")
            prev_last = st.get("last_entry_ts", 0)
            all_state[symbol] = init_symbol_state()
            all_state[symbol]["last_entry_ts"]   = prev_last
            all_state[symbol]["current_day_str"] = today_str
            all_state[symbol]["tp_completed"]    = True
            save_state(all_state)
            return None

    # ── 6. Reconcile with exchange ────────────────────────────────────────────
    position = next((p for p in global_positions if p.get("pair") == pair_name), None)
    if position is not None:
        if st.get("pending_since"):
            # Our resting retest limit just FILLED — the pullback came to us.
            print(f"  [{symbol}] RETEST FILLED — pullback reached our limit")
            send_telegram(
                f"🎯 <b>RETEST FILLED — {symbol}</b>\n"
                f"The pullback to the level filled our resting limit.\n"
                f"Entry: <code>{st.get('entry_price')}</code>  "
                f"Direction: <code>{(st.get('direction') or '').upper()}</code>"
            )
            st["pending_since"] = 0
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

        # ── Stale momentum monitor for open positions ─────────────────────────
        check_stale_trade(symbol, st)

        save_state(all_state)
        return None

    if st.get("in_position"):
        print(f"  [{symbol}] POSITION CLOSED — resetting state")
        prev_last = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last
        st = all_state[symbol]
        save_state(all_state)

    # ── Pending retest order management ──────────────────────────────────────
    # A resting retest limit is a signal with a shelf life. If it hasn't filled
    # within PENDING_EXPIRY_CANDLES x 15m, the setup is stale — cancel it so
    # capital and the slot are freed for fresh signals.
    my_orders = [o for o in global_orders if o.get("pair") == pair_name]
    if my_orders:
        pending_since = st.get("pending_since", 0)
        age_ms        = now_ms - pending_since if pending_since else 0
        expiry_ms     = PENDING_EXPIRY_CANDLES * CANDLE_SECONDS_15M * 1000
        if pending_since and age_ms > expiry_ms:
            print(f"  [{symbol}] PENDING ORDER EXPIRED "
                  f"({age_ms // 60000}min > {PENDING_EXPIRY_CANDLES}x15m) — cancelling")
            for o in my_orders:
                oid = o.get("id") or o.get("order_id")
                if oid:
                    cancel_order(oid)
            st["pending_since"] = 0
            st["tp_level"]      = None
            st["sl_price"]      = None
            st["direction"]     = None
            st["entry_price"]   = None
            update_sheet_tp(row, "")
            update_sheet_sl(row, "")
            send_telegram(
                f"⌛ <b>RETEST ORDER EXPIRED — {symbol}</b>\n"
                f"Unfilled after {PENDING_EXPIRY_CANDLES} x 15m candles.\n"
                f"Signal stale — order cancelled, slot freed."
            )
            save_state(all_state)
            return None
        print(f"  [{symbol}] SKIP — retest order resting on book "
              f"(age {age_ms // 60000}min / expiry {PENDING_EXPIRY_CANDLES * 15}min)")
        save_state(all_state)
        return None

    # ── 7. Candle dedup guard ─────────────────────────────────────────────────
    curr    = candles_15m[-1]
    curr_ts = int(curr["time"])
    if curr_ts <= st.get("last_candle_ts", 0):
        print(f"  [{symbol}] SKIP — same 15m candle already processed")
        save_state(all_state)
        return None

    # ── 7b. Volume from DAILY futures candle ─────────────────────────────────
    vol_24h_usd, raw_daily = fetch_24h_volume(symbol)
    if raw_daily is None:
        print(f"  [{symbol}] DISCARDED — could not fetch daily candle for volume")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    if vol_24h_usd < MIN_24H_VOL_USDT:
        print(f"  [{symbol}] DISCARDED — 24h vol ${vol_24h_usd:,.0f} "
              f"< threshold ${MIN_24H_VOL_USDT:,.0f}")
        st["last_candle_ts"] = curr_ts
        save_state(all_state)
        return None
    print(f"  [{symbol}] Volume PASS — 24h vol ${vol_24h_usd:,.0f}")

    # ── 8. Fetch 4H and 1H candles ───────────────────────────────────────────
    candles_4h = fetch_candles(symbol, CANDLES_4H, RESOLUTION_4H, CANDLE_SECONDS_4H)
    candles_1h = fetch_candles(symbol, CANDLES_1H, RESOLUTION_1H, CANDLE_SECONDS_1H)
    if candles_4h and (now_ms - int(candles_4h[-1]["time"])) < CANDLE_SECONDS_4H * 1000:
        candles_4h = candles_4h[:-1]
    if candles_1h and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS_1H * 1000:
        candles_1h = candles_1h[:-1]

    # ── 9. Shared structural checks ───────────────────────────────────────────
    # Compression: SCORE BONUS now, not a gate
    compressed, atr_pct, _ = check_1h_compression(candles_1h)
    print(f"  [{symbol}] Compression: {'COILED (+' + str(COMPRESS_BONUS) + ' pts)' if compressed else 'not coiled (no bonus)'} "
          f"— 1H ATR={atr_pct}%")

    extended, rng_pct = check_range_not_extended(candles_15m)
    if extended:
        print(f"  [{symbol}] DISCARDED — already extended (5-day range={rng_pct}% >= {RANGE_SKIP_PCT}%)")
        st["last_candle_ts"] = curr_ts; save_state(all_state); return None
    print(f"  [{symbol}] Range PASS — 5-day range={rng_pct}%")

    # ── 10. Evaluate BOTH directions ─────────────────────────────────────────
    found_candidates = []

    # ── LONG ─────────────────────────────────────────────────────────────────
    trend_up, close_4h, ema50_4h = check_4h_uptrend(candles_4h)
    if trend_up:
        slope_ok, ema_now, ema_prev = check_ema_slope(candles_4h, "long")
        if not slope_ok:
            print(f"  [{symbol}] LONG: DISCARDED — EMA50 flat/falling ({ema_prev} -> {ema_now})")
        else:
            ign_ok, conf_c, level, vol_ratio, strength, body_pct, vol_bld, reason = \
                check_momentum_ignition_long(candles_15m)
            if not ign_ok:
                print(f"  [{symbol}] LONG: DISCARDED — ignition fail: {reason}")
            else:
                htf_ok, htf_r, htf_p, htf_conf = check_1h_bullish_confirmation(candles_1h)
                if not htf_ok:
                    print(f"  [{symbol}] LONG: DISCARDED — 1H bullish fail "
                          f"(bullish={htf_conf}, vol {htf_p}->{htf_r})")
                else:
                    print(f"  [{symbol}] LONG: IGNITION PASS  confirm={conf_c}  level={level}  "
                          f"vol={vol_ratio}x  strength={strength}%  body={body_pct}  vol_building={vol_bld}")

                    # ── VWAP filter: never long below the institutional benchmark ──
                    vwap, vwap_src = compute_session_vwap(candles_15m)
                    if VWAP_FILTER and vwap and conf_c < vwap:
                        print(f"  [{symbol}] LONG: DISCARDED — price {conf_c} below "
                              f"VWAP {vwap:.8g} ({vwap_src}) — sellers in control")
                        vwap_block = True
                    else:
                        vwap_block = False
                        if vwap:
                            print(f"  [{symbol}] LONG: VWAP PASS — price {conf_c} > "
                                  f"VWAP {vwap:.8g} ({vwap_src})")

                    if not vwap_block:
                        # ── RETEST ENTRY: limit rests at the broken level, not the
                        #    extended breakout price. The pullback fills us. ──
                        atr_val = compute_atr(candles_15m[-50:], ATR_LEN) or (conf_c * 0.01)
                        if RETEST_ENTRY:
                            entry_price = round(level + RETEST_BUFFER_ATR * atr_val, precision)
                            print(f"  [{symbol}] LONG: RETEST entry {entry_price} "
                                  f"(level {level} + {RETEST_BUFFER_ATR} ATR) "
                                  f"vs chase price {conf_c} — "
                                  f"saving {round((conf_c - entry_price) / conf_c * 100, 3)}%")
                        else:
                            entry_price = round(conf_c, precision)
                        tp_price, sl_price = compute_atr_tp_sl(entry_price, candles_15m, "long", precision, level)
                        order_qty = compute_qty(entry_price, symbol)
                        ob_ok, ob_reason, ob_imb, ob_spread = check_orderbook_gate(
                            symbol, "long", entry_price, order_qty)
                        if not ob_ok:
                            print(f"  [{symbol}] LONG: DISCARDED — order book: {ob_reason}")
                        else:
                            ob_bonus     = orderbook_score_bonus(ob_imb, "long")
                            move_pct     = strength
                            ema_prox_pct = ((conf_c / ema50_4h - 1) * 100) if ema50_4h > 0 else 0.0
                            score  = score_candidate(vol_ratio, move_pct, ema_prox_pct,
                                                     vol_24h_usd, "long", btc_bull,
                                                     ob_bonus, body_pct, compressed)
                            tp_pct = round(abs((tp_price - entry_price) / entry_price * 100), 2)
                            sl_pct = round(abs((sl_price - entry_price) / entry_price * 100), 2)
                            regime = "" if btc_bull else " [COUNTER-TREND]"
                            print(f"  [{symbol}] ✅ LONG CANDIDATE  score={score}{regime}  "
                                  f"body={body_pct}  ob={ob_imb:.3f}(+{ob_bonus})  "
                                  f"entry={entry_price}  tp={tp_price}(+{tp_pct}%)  sl={sl_price}(-{sl_pct}%)")
                            found_candidates.append({
                                "symbol": symbol, "row": row, "direction": "long",
                                "score": score, "entry_price": entry_price,
                                "tp_price": tp_price, "sl_price": sl_price,
                                "precision": precision, "curr_ts": curr_ts,
                                "level": level,
                                "vol_ratio": vol_ratio, "move_pct": round(move_pct, 4),
                                "body_pct": body_pct,
                                "ema_prox_pct": round(ema_prox_pct, 4),
                                "vol_24h_usd": round(vol_24h_usd, 0),
                                "ob_imbalance": round(ob_imb, 4), "ob_bonus": ob_bonus,
                            })
    else:
        print(f"  [{symbol}] LONG: 4H FAIL — close={close_4h} < ema50={ema50_4h}")

    # ── SHORT ────────────────────────────────────────────────────────────────
    trend_dn, close_4h, ema50_4h = check_4h_downtrend(candles_4h)
    if trend_dn:
        slope_ok, ema_now, ema_prev = check_ema_slope(candles_4h, "short")
        if not slope_ok:
            print(f"  [{symbol}] SHORT: DISCARDED — EMA50 flat/rising ({ema_prev} -> {ema_now})")
        else:
            ign_ok, conf_c, level, vol_ratio, strength, body_pct, vol_bld, reason = \
                check_momentum_ignition_short(candles_15m)
            if not ign_ok:
                print(f"  [{symbol}] SHORT: DISCARDED — ignition fail: {reason}")
            else:
                htf_ok, htf_r, htf_p, htf_conf = check_1h_bearish_confirmation(candles_1h)
                if not htf_ok:
                    print(f"  [{symbol}] SHORT: DISCARDED — 1H bearish fail "
                          f"(bearish={htf_conf}, vol {htf_p}->{htf_r})")
                else:
                    print(f"  [{symbol}] SHORT: IGNITION PASS  confirm={conf_c}  level={level}  "
                          f"vol={vol_ratio}x  strength={strength}%  body={body_pct}  vol_building={vol_bld}")

                    # ── VWAP filter: never short above the institutional benchmark ──
                    vwap, vwap_src = compute_session_vwap(candles_15m)
                    if VWAP_FILTER and vwap and conf_c > vwap:
                        print(f"  [{symbol}] SHORT: DISCARDED — price {conf_c} above "
                              f"VWAP {vwap:.8g} ({vwap_src}) — buyers in control")
                        vwap_block = True
                    else:
                        vwap_block = False
                        if vwap:
                            print(f"  [{symbol}] SHORT: VWAP PASS — price {conf_c} < "
                                  f"VWAP {vwap:.8g} ({vwap_src})")

                    if not vwap_block:
                        # ── RETEST ENTRY: limit rests at the broken level ──
                        atr_val = compute_atr(candles_15m[-50:], ATR_LEN) or (conf_c * 0.01)
                        if RETEST_ENTRY:
                            entry_price = round(level - RETEST_BUFFER_ATR * atr_val, precision)
                            print(f"  [{symbol}] SHORT: RETEST entry {entry_price} "
                                  f"(level {level} - {RETEST_BUFFER_ATR} ATR) "
                                  f"vs chase price {conf_c} — "
                                  f"saving {round((entry_price - conf_c) / conf_c * 100, 3)}%")
                        else:
                            entry_price = round(conf_c, precision)
                        tp_price, sl_price = compute_atr_tp_sl(entry_price, candles_15m, "short", precision, level)
                        order_qty = compute_qty(entry_price, symbol)
                        ob_ok, ob_reason, ob_imb, ob_spread = check_orderbook_gate(
                            symbol, "short", entry_price, order_qty)
                        if not ob_ok:
                            print(f"  [{symbol}] SHORT: DISCARDED — order book: {ob_reason}")
                        else:
                            ob_bonus     = orderbook_score_bonus(ob_imb, "short")
                            move_pct     = strength
                            ema_prox_pct = ((1 - conf_c / ema50_4h) * 100) if ema50_4h > 0 else 0.0
                            score  = score_candidate(vol_ratio, move_pct, ema_prox_pct,
                                                     vol_24h_usd, "short", btc_bull,
                                                     ob_bonus, body_pct, compressed)
                            tp_pct = round(abs((tp_price - entry_price) / entry_price * 100), 2)
                            sl_pct = round(abs((sl_price - entry_price) / entry_price * 100), 2)
                            regime = "" if not btc_bull else " [COUNTER-TREND]"
                            print(f"  [{symbol}] ✅ SHORT CANDIDATE  score={score}{regime}  "
                                  f"body={body_pct}  ob={ob_imb:.3f}(+{ob_bonus})  "
                                  f"entry={entry_price}  tp={tp_price}(-{tp_pct}%)  sl={sl_price}(+{sl_pct}%)")
                            found_candidates.append({
                                "symbol": symbol, "row": row, "direction": "short",
                                "score": score, "entry_price": entry_price,
                                "tp_price": tp_price, "sl_price": sl_price,
                                "precision": precision, "curr_ts": curr_ts,
                                "level": level,
                                "vol_ratio": vol_ratio, "move_pct": round(move_pct, 4),
                                "body_pct": body_pct,
                                "ema_prox_pct": round(ema_prox_pct, 4),
                                "vol_24h_usd": round(vol_24h_usd, 0),
                                "ob_imbalance": round(ob_imb, 4), "ob_bonus": ob_bonus,
                            })
    else:
        print(f"  [{symbol}] SHORT: 4H FAIL — close={close_4h} > ema50={ema50_4h}")

    st["last_candle_ts"] = curr_ts

    if not found_candidates:
        save_state(all_state)
        return None

    best = max(found_candidates, key=lambda x: x["score"])
    return best


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>MOMENTUM IGNITION v3 — Institutional Defense</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🏦 <b>Anti-institutional-slap:</b>\n"
    f"  <code>• RETEST ENTRY — limit rests at broken level; the pullback</code>\n"
    f"  <code>  that used to stop us out now FILLS us</code>\n"
    f"  <code>• VWAP filter — long only above / short only below session VWAP</code>\n"
    f"  <code>• Pending expiry — unfilled retest cancelled after "
    f"{PENDING_EXPIRY_CANDLES}x15m</code>\n"
    f"\n"
    f"🛡 <b>9-Filter Anti-Fakeout Stack:</b>\n"
    f"  <code>① 2-candle confirm    ② {VOL_SPIKE_MULT}x vol spike</code>\n"
    f"  <code>③ Volume BUILDING     ④ Body &gt;= {int(BODY_DOMINANCE*100)}% (no wick traps)</code>\n"
    f"  <code>⑤ {MIN_BREAKOUT_PCT}% min strength  ⑥ 4H EMA slope</code>\n"
    f"  <code>⑦ 1H directional      ⑧ Order book gate</code>\n"
    f"  <code>⑨ Live price re-check at execution</code>\n"
    f"\n"
    f"📐 Both long+short per coin | compression = +{COMPRESS_BONUS} pts bonus (not gate)\n"
    f"🌍 BTC regime = +{REGIME_BONUS_PTS} pts | book lean = up to +{OB_BONUS_MAX_PTS} pts\n"
    f"💹 SL: <code>{SL_LEVEL_BUFFER} ATR beyond broken level "
    f"(clamp {SL_MIN_ATR}-{SL_MAX_ATR} ATR)</code>\n"
    f"💹 TP: <code>{RR_TARGET} x risk — scales per coin, always reachable</code>\n"
    f"🐌 Stale alert: <code>&lt; +{STALE_MIN_MOVE_PCT}% after {STALE_CANDLES}x15m</code>\n"
    f"🔁 Scan: <code>Every {SCAN_INTERVAL}s</code>  |  "
    f"💰 <code>{CAPITAL_USDT} USDT x {LEVERAGE}x</code>"
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

        print(f"\n===== CYCLE {cycle} | "
              f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} | "
              f"positions={len(global_positions)} orders={len(global_orders)} =====")

        all_symbols_rows = []
        row_index        = {}
        for row in range(len(df)):
            symbol = normalize_symbol(df.iloc[row, 0])
            if symbol:
                all_symbols_rows.append((symbol, row))
                row_index[symbol] = row

        btc_bull, btc_close, btc_ema200 = get_btc_regime()
        eligible = build_eligible_universe(all_symbols_rows)

        eligible_set = {s for s, _ in eligible}
        for sym, sym_st in state.items():
            if (sym_st.get("in_position") or sym_st.get("tp_level")) and sym not in eligible_set:
                r = row_index.get(sym)
                if r is not None:
                    eligible.append((sym, r))
                    eligible_set.add(sym)
                    print(f"[FORCE-INCLUDE] {sym} — active position/TP, monitoring only")

        active_count    = len(global_positions)
        slots_available = max(0, MAX_OPEN_TRADES - active_count)
        btc_label       = "BULL" if btc_bull else "BEAR"
        print(f"[SLOTS] {active_count} open / {MAX_OPEN_TRADES} max -> {slots_available} slot(s)")
        print(f"[BTC]   {btc_label} — aligned trades score +{REGIME_BONUS_PTS} pts\n")

        candidates = []
        for symbol, row in eligible:
            print(f"--- {symbol} ---")
            try:
                cand = check_and_trade(
                    symbol, row, df, state,
                    global_positions, global_orders,
                    btc_bull,
                )
                if cand:
                    candidates.append(cand)
            except Exception as e:
                print(f"  [{symbol}] ERROR: {e}")
                continue

        candidates.sort(key=lambda x: x["score"], reverse=True)
        n_long  = sum(1 for c in candidates if c["direction"] == "long")
        n_short = sum(1 for c in candidates if c["direction"] == "short")

        print(f"\n[RANKING] {len(candidates)} candidate(s) "
              f"({n_long}L / {n_short}S) | {slots_available} slot(s)")
        for i, c in enumerate(candidates):
            tag    = f"EXECUTE #{i + 1}" if i < slots_available else "SKIP (no slot)"
            emoji  = "🟢" if c["direction"] == "long" else "🔴"
            regime = "✅ aligned" if (c["direction"] == "long") == btc_bull else "⚡ counter-trend"
            print(f"  [{tag}] {emoji} {c['symbol']} ({c['direction'].upper()})  "
                  f"score={c['score']}  {regime}  vol={c['vol_ratio']}x  "
                  f"body={c['body_pct']}  ob={c['ob_imbalance']}  "
                  f"move={c['move_pct']}%  24h=${c['vol_24h_usd']:,.0f}")

        for cand in candidates[:slots_available]:
            try:
                execute_entry(cand, state)
            except Exception as e:
                print(f"  [{cand['symbol']}] ENTRY ERROR: {e}")

        if candidates:
            executed = candidates[:slots_available]
            skipped  = candidates[slots_available:]
            msg = (
                f"📊 <b>Cycle {cycle} — BTC {btc_label}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔍 Scanned  : <code>{len(eligible)}</code>\n"
                f"✅ Qualified: <code>{len(candidates)} ({n_long}L / {n_short}S)</code>\n"
                f"🎯 Executed : <code>{len(executed)}</code>\n"
            )
            for c in executed:
                emoji  = "🟢" if c["direction"] == "long" else "🔴"
                regime = "✅" if (c["direction"] == "long") == btc_bull else "⚡"
                msg += (f"  {emoji}{regime} {c['symbol']}  score={c['score']}  "
                        f"body={c['body_pct']}  ob={c['ob_imbalance']}  "
                        f"entry={c['entry_price']}\n")
            if skipped:
                msg += f"⏭ Skipped : <code>{', '.join(c['symbol'] for c in skipped)}</code>"
            send_telegram(msg)

        print(f"\n===== CYCLE {cycle} DONE =====")
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