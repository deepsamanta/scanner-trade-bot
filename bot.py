import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"


# =====================================================
# GOOGLE SHEETS CONNECTION
# =====================================================

scope = [
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
"service_account.json",
scopes=scope
)

client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1


# =====================================================
# READ SHEET
# =====================================================

def get_sheet_data():

    try:
        data = sheet.get_all_values()
        df = pd.DataFrame(data)

        if df.shape[1] < 2:
            df[1] = ""

        return df

    except:
        return pd.DataFrame()


# =====================================================
# UPDATE TP COLUMN
# =====================================================

def update_sheet_tp(row,value):

    try:

        sheet.update(f"B{row+1}", [[str(value)]])
        print(f"[SHEET] Row {row+1} -> {value}")

    except Exception as e:

        print("Sheet update error:",e)


# =====================================================
# SYMBOL HELPERS
# =====================================================

def normalize_symbol(symbol):

    symbol = str(symbol).upper().strip()

    if "USDT" in symbol:
        return symbol.split("USDT")[0] + "USDT"

    return symbol


def fut_pair(symbol):

    return f"B-{symbol.replace('USDT','')}_USDT"


# =====================================================
# SIGN REQUEST
# =====================================================

def sign_request(body):

    payload = json.dumps(body,separators=(",",":"))

    signature = hmac.new(
        COINDCX_SECRET.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type":"application/json",
        "X-AUTH-APIKEY":COINDCX_KEY,
        "X-AUTH-SIGNATURE":signature
    }

    return payload,headers


# =====================================================
# BTC TREND FILTER
# =====================================================

def btc_is_bearish():

    try:

        pair_api="B-BTC_USDT"

        url="https://public.coindcx.com/market_data/candlesticks"

        now=int(time.time())

        params={
            "pair":pair_api,
            "from":now-(360000),
            "to":now,
            "resolution":"15",
            "pcode":"f"
        }

        response=requests.get(url,params=params)

        data=response.json()["data"]

        candles=sorted(data,key=lambda x:x["time"])

        closes=[float(c["close"]) for c in candles]

        period=200
        multiplier=2/(period+1)

        ema=sum(closes[:period])/period

        for price in closes[period:]:
            ema=(price-ema)*multiplier+ema

        btc_price=closes[-1]

        if btc_price<ema:
            print("[BTC] Bearish")
            return True
        else:
            print("[BTC] Bullish")
            return False

    except:
        return False


# =====================================================
# OPEN POSITIONS
# =====================================================

def get_open_positions():

    try:

        body={
            "timestamp":int(time.time()*1000),
            "page":"1",
            "size":"50",
            "margin_currency_short_name":["USDT"]
        }

        payload,headers=sign_request(body)

        url=BASE_URL+"/exchange/v1/derivatives/futures/positions"

        response=requests.post(url,data=payload,headers=headers)

        positions=response.json()

        return [pos for pos in positions if float(pos.get("active_pos",0))!=0]

    except:
        return []


# =====================================================
# POSITION TP
# =====================================================

def get_position_tp(symbol):

    try:

        positions=get_open_positions()

        pair=fut_pair(symbol)

        for pos in positions:

            if pos.get("pair")==pair:

                tp=pos.get("take_profit_trigger")

                if tp:
                    return float(tp)

        return None

    except:
        return None


# =====================================================
# WICK TP DETECTION
# =====================================================

def get_recent_low(symbol):

    try:

        pair_api=fut_pair(symbol)

        url="https://public.coindcx.com/market_data/candlesticks"

        now=int(time.time())

        params={
            "pair":pair_api,
            "from":now-180,
            "to":now,
            "resolution":"1",
            "pcode":"f"
        }

        response=requests.get(url,params=params)

        result=response.json()

        candles=result["data"]

        lows=[float(c["low"]) for c in candles]

        return min(lows)

    except:
        return None


# =====================================================
# QUANTITY
# =====================================================

def get_quantity_step(symbol):

    try:

        pair=fut_pair(symbol)

        url=f"https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument?pair={pair}&margin_currency_short_name=USDT"

        response=requests.get(url)

        data=response.json()

        instrument=data["instrument"]

        quantity_increment=Decimal(str(instrument["quantity_increment"]))
        min_quantity=Decimal(str(instrument["min_quantity"]))

        return max(quantity_increment,min_quantity)

    except:
        return Decimal("1")


def compute_qty(entry_price,symbol):

    step=get_quantity_step(symbol)

    capital=Decimal(str(CAPITAL_USDT))
    leverage=Decimal(str(LEVERAGE))

    exposure=capital*leverage

    raw_qty=exposure/Decimal(str(entry_price))

    qty=(raw_qty/step).quantize(Decimal("1"))*step

    if qty<=0:
        qty=step

    qty=qty.quantize(step)

    return float(qty)


# =====================================================
# PLACE ORDER
# =====================================================

def place_order(side,symbol,entry_price,ema,candles):

    qty=compute_qty(entry_price,symbol)

    precision=len(str(entry_price).split(".")[1]) if "." in str(entry_price) else 0

    entry=round(entry_price,precision)

    tp=entry*0.95

    previous_high=float(candles[-2]["high"])
    sl=round(previous_high*1.001,precision)

    tp=round(tp,precision)

    print(f"[TRADE] {symbol} SELL | Entry {entry} | TP {tp} | SL {sl}")

    body={
        "timestamp":int(time.time()*1000),
        "order":{
            "side":side,
            "pair":fut_pair(symbol),
            "order_type":"limit_order",
            "price":entry,
            "total_quantity":qty,
            "leverage":LEVERAGE,
            "take_profit_price":tp,
            "stop_loss_price":sl,
            "position_margin_type":"crossed"
        }
    }

    payload,headers=sign_request(body)

    response=requests.post(BASE_URL+"/exchange/v1/derivatives/futures/orders/create",data=payload,headers=headers)

    result=response.json()

    try:
        tp=result["order"]["take_profit_trigger"]
    except:
        tp=None

    return tp


# =====================================================
# MAIN LOGIC
# =====================================================

def check_ema_and_trade(symbol,row,df,allow_trade):

    pair_api=fut_pair(symbol)

    url="https://public.coindcx.com/market_data/candlesticks"

    now=int(time.time())

    params={
        "pair":pair_api,
        "from":now-(360000),
        "to":now,
        "resolution":"15",
        "pcode":"f"
    }

    response=requests.get(url,params=params)

    candles=sorted(response.json()["data"],key=lambda x:x["time"])

    closes=[float(c["close"]) for c in candles]

    period=200
    multiplier=2/(period+1)

    ema=sum(closes[:period])/period

    for price in closes[period:]:
        ema=(price-ema)*multiplier+ema

    last_close=float(candles[-1]["close"])
    prev_close=float(candles[-2]["close"])

    recent_high=max([float(c["high"]) for c in candles[-5:]])

    ema_upper=ema*0.995
    ema_lower=ema*0.99

    print(f"[CHECK] {symbol} | Close {last_close} | EMA {ema}")

    if not btc_is_bearish():
        print("[SKIP] BTC bullish")
        return


    # ==========================================
    # ENTRY TYPE DETECTION
    # ==========================================

    sweep_entry = recent_high > ema and last_close < ema
    breakdown_entry = last_close < ema*0.995 and prev_close < ema

    if not (sweep_entry or breakdown_entry):
        print(f"[SKIP] {symbol} no sweep or breakdown")
        return


    tp_raw=df.iloc[row,1]

    if str(tp_raw).upper()=="TP COMPLETED":
        return


    # ==========================================
    # ACTIVE POSITION CHECK
    # ==========================================

    positions=get_open_positions()
    pair=fut_pair(symbol)

    for pos in positions:

        if pos.get("pair")==pair:

            print(f"[ACTIVE] {symbol}")

            tp=get_position_tp(symbol)

            if tp:
                update_sheet_tp(row,tp)

            return


    # ==========================================
    # TP CHECK
    # ==========================================

    try:

        tp=float(tp_raw)

        if last_close<=tp:

            update_sheet_tp(row,"TP COMPLETED")
            return

        recent_low=get_recent_low(symbol)

        if recent_low and recent_low<=tp:

            update_sheet_tp(row,"TP COMPLETED")
            return

    except:
        pass


    # ==========================================
    # ENTRY
    # ==========================================

    if allow_trade and ema_lower<=last_close<=ema_upper:

        tp=place_order("sell",symbol,last_close,ema,candles)

        if tp:
            update_sheet_tp(row,tp)


# =====================================================
# MAIN LOOP
# =====================================================

cycle=0

while True:

    try:

        df=get_sheet_data()

        allow_trade=(cycle%10==0)

        for row in range(len(df)):

            pair=df.iloc[row,0]

            if not pair:
                continue

            symbol=normalize_symbol(pair)

            check_ema_and_trade(symbol,row,df,allow_trade)

        cycle+=1

        time.sleep(30)

    except Exception as e:

        print("BOT ERROR:",e)

        time.sleep(60)