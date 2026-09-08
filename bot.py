import os
import asyncio
import logging
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from google import genai
import ccxt
from aiohttp import web

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ai_client = genai.Client(api_key=GEMINI_API_KEY)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
exchange = ccxt.kucoin()

logging.basicConfig(level=logging.INFO)

# کیبورد دکمه‌ای برای دسترسی سریع
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="BTC"), KeyboardButton(text="ETH")],
        [KeyboardButton(text="SOL"), KeyboardButton(text="BNB")]
    ],
    resize_keyboard=True
)

def get_crypto_data(symbol: str) -> dict:
    formatted_symbol = f"{symbol.upper()}/USDT"
    try:
        ticker = exchange.fetch_ticker(formatted_symbol)
        return {
            "symbol": symbol.upper(),
            "price": ticker['last'],
            "high": ticker['high'],
            "low": ticker['low'],
            "volume": ticker['baseVolume']
        }
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

async def analyze_with_ai(market_data: dict) -> str:
    prompt = f"""
    تو یک تحلیل‌گر حرفه‌ای کریپتو هستی. بر اساس داده‌های زیر یک تحلیل و سیگنال کوتاه بساز:
    
    ارز: {market_data['symbol']}/USDT
    قیمت فعلی: {market_data['price']}
    بالاترین قیمت ۲۴ ساعت: {market_data['high']}
    پایین‌ترین قیمت ۲۴ ساعت: {market_data['low']}
    حجم: {market_data['volume']}
    
    فرمت خروجی:
    🎯 **سیگنال:** (خرید / فروش / صبر)
    📥 **نقطه ورود:**
    🏁 **تارگت‌ها:**
    🛡️ **حد ضرر:**
    💡 **تحلیل کوتاه:** (حداکثر ۲ جمله)
    """
    try:
        response = ai_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        return response.text
    except Exception as e:
        logging.error(f"AI Error: {e}")
        return f"خطا در هوش مصنوعی: {e}"

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer(
        "سلام! به ربات تحلیل کریپتو خوش آمدید.\n\n"
        "کافیست **نام ارز** (مثلاً BTC یا ETH) را بفرستید یا از دکمه‌های زیر استفاده کنید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

# پردازش متون معمولی (ارسال مستقیم اسم ارز بدون دستور)
@dp.message(F.text)
async def process_crypto_name(message: types.Message):
    symbol = message.text.strip().replace("/", "")
    
    # صرف‌نظر از دستورات ناشناخته
    if symbol.startswith("start"):
        return

    wait_msg = await message.answer(f"⏳ در حال دریافت داده‌ها و تحلیل {symbol.upper()}...")

    market_data = get_crypto_data(symbol)
    if not market_data:
        await wait_msg.edit_text("❌ ارز مورد نظر پیدا نشد. لطفاً نماد انگلیسی درست را بفرستید (مانند BTC).")
        return

    ai_analysis = await analyze_with_ai(market_data)
    await wait_msg.edit_text(f"📊 **تحلیل هوشمند {symbol.upper()}**\n\n{ai_analysis}", parse_mode="Markdown")

async def handle_ping(request):
    return web.Response(text="Bot is running!")

async def main():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())