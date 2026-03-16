import os

COINDCX_KEY = os.getenv("COINDCX_KEY")
COINDCX_SECRET = os.getenv("COINDCX_SECRET")

SHEET_ID = os.getenv("SHEET_ID")

CAPITAL_USDT = os.getenv("CAPITAL_USDT", "5")
LEVERAGE = os.getenv("LEVERAGE", "6")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# Safety check
if not COINDCX_KEY or not COINDCX_SECRET:
    raise ValueError("API keys missing. Please provide COINDCX_KEY and COINDCX_SECRET. lol")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Telegram config missing. Please provide TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")