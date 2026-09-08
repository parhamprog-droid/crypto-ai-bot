import os
import asyncio
import logging
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
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
        return "خطا در پردازش تحلیل با AI."

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("سلام! به ربات تحلیل کریپتو خوش آمدید.\nبرای دریافت سیگنال دستور زیر را بفرستید:\n`/signal BTC`", parse_mode="Markdown")

@dp.message(Command("signal"))
async def signal_handler(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("لطفاً نام ارز را وارد کنید. مثال:\n`/signal BTC`", parse_mode="Markdown")
        return

    symbol = args[1]
    wait_msg = await message.answer(f"⏳ در حال دریافت داده‌ها و تحلیل {symbol.upper()}...")

    market_data = get_crypto_data(symbol)
    if not market_data:
        await wait_msg.edit_text("❌ ارز مورد نظر پیدا نشد.")
        return

    ai_analysis = await analyze_with_ai(market_data)
    await wait_msg.edit_text(f"📊 **تحلیل هوشمند {symbol.upper()}**\n\n{ai_analysis}", parse_mode="Markdown")

# ساخت یک سرور وب کوچک برای پاسخ به Health Check سرویس Render
async def handle_ping(request):
    return web.Response(text="Bot is running smoothly!")

async def main():
    # فعال‌سازی پورت برای Render
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print("Bot is running...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())