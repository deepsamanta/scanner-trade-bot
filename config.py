import os

COINDCX_KEY = os.getenv("COINDCX_KEY")
COINDCX_SECRET = os.getenv("COINDCX_SECRET")

SHEET_ID = os.getenv("SHEET_ID")

CAPITAL_USDT = os.getenv("CAPITAL_USDT", "5")
LEVERAGE = os.getenv("LEVERAGE", "6")


# Safety check
if not COINDCX_KEY or not COINDCX_SECRET:
    raise ValueError("API keys missing. Please provide COINDCX_KEY and COINDCX_SECRET.")