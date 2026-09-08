import os
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, URLInputFile
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

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="BTC"), KeyboardButton(text="ETH")],
        [KeyboardButton(text="SOL"), KeyboardButton(text="SEI")]
    ],
    resize_keyboard=True
)

def get_crypto_data(symbol: str) -> dict:
    formatted_symbol = f"{symbol.upper()}/USDT"
    try:
        ticker = exchange.fetch_ticker(formatted_symbol)
        price = ticker['last']
        change_24h = ticker.get('percentage', 0)
        return {
            "symbol": symbol.upper(),
            "price": price,
            "high": ticker['high'],
            "low": ticker['low'],
            "volume": ticker['baseVolume'],
            "change_24h": change_24h
        }
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def generate_chart_url(symbol: str) -> str:
    """تولید لینک تصویر چارت پیشرفته با اندیکاتورها"""
    formatted_symbol = f"KUCOIN:{symbol.upper()}USDT"
    # ساخت لینک تصویر اختصاصی تریدینگ‌ویو
    chart_url = f"https://s3.tradingview.com/snapshots/{symbol.lower()[0]}/{symbol.upper()}_chart.png"
    # سرویس پشتیبان هوشمند برای ساخت چارت چشمی
    fallback_url = f"https://charts2.finviz.com/chart.ashx?t={symbol.upper()}&ty=c&ta=1&p=d&s=l"
    return fallback_url

async def analyze_with_ai(market_data: dict) -> str:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    prompt = f"""
    تو یک سیستم هوشمند آنالیز تکنیکال و الگوریتمی کریپتو هستی. 
    بر اساس داده‌های بازار، یک خروجی ساختاریافته و حرفه‌ای بساز.

    مشخصات ارز:
    - نماد: #{market_data['symbol']}USDT
    - قیمت فعلی: {market_data['price']}
    - تغییرات ۲۴ ساعته: {market_data['change_24h']}%
    - زمان: {current_time}

    قالب خروجی (حداکثر ۱۰۰۰ کاراکتر جهت قرارگیری کامل در كپشن عکس):

    💎 **آلفا سیگنال | #{market_data['symbol']}USDT**
    ⏱ زمان: {current_time}
    📌 وضعیت: (صعودی 🟢 / نزولی 🔴 / رنج 🟡)

    📊 **داشبورد قیمت**
    • قیمت فعلی: `{market_data['price']}` USDT
    • تغییر ۲۴h: {market_data['change_24h']}%

    ⚡️ **ستاپ معاملاتی (ورود)**
    • پله اول: `{market_data['price']}`
    • پله دوم: (۱.۲٪ پایین‌تر)
    • پله سوم: (۲.۵٪ پایین‌تر)

    🎯 **تارگت‌ها (Take Profit)**
    ▫️ هدف ۱: (۱.۵٪ بالاتر)
    ▫️ هدف ۲: (۳.۲٪ بالاتر)
    ▫️ هدف ۳: (۵.۵٪ بالاتر)

    🛑 **حد ضرر:** (۴٪ پایین‌تر)

    ⚖️ **مدیریت ریسک & اهرم**
    • ریسک به ریوارد: (محاسبه R/R)
    • اهرم پیشنهادی: (مثلاً Cross 3x - 5x)

    🧩 **تاییده‌ها:**
    • بررسی روند کندل‌ها و RSI
    """
    
    models_to_try = [
        'gemini-3.6-flash',
        'gemini-2.5-flash',
        'gemini-2.5-pro'
    ]

    for model_name in models_to_try:
        try:
            response = ai_client.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            return response.text
        except Exception as e:
            continue

    return "❌ خطا در دریافت تحلیل هوش مصنوعی."

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer(
        "🧠 **به ربات تحلیل تکنیکال و سیگنال‌دهی خوش آمدید**\n\n"
        "برای دریافت چارت و ستاپ کامل معاملاتی، نام ارز (مانند BTC یا SOL) را ارسال کنید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text)
async def process_crypto_name(message: types.Message):
    symbol = message.text.strip().replace("/", "").replace("#", "")
    
    if symbol.lower().startswith("start"):
        return

    wait_msg = await message.answer(f"🔎 در حال ترسیم چارت و محاسبات برای {symbol.upper()}...")

    market_data = get_crypto_data(symbol)
    if not market_data:
        await wait_msg.edit_text("❌ ارز مورد نظر پیدا نشد. لطفاً نماد معتبر وارد کنید.")
        return

    ai_analysis = await analyze_with_ai(market_data)
    chart_image_url = generate_chart_url(symbol)

    try:
        # حذف پیام انتظار
        await wait_msg.delete()
        # ارسال عکس چارت همراه با متن تحلیل در کپشن
        await message.answer_photo(
            photo=URLInputFile(chart_image_url),
            caption=ai_analysis,
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Error sending photo: {e}")
        # در صورت بروز خطا در بارگذاری عکس، متن به صورت جداگانه ارسال می‌شود
        await message.answer(ai_analysis, parse_mode="Markdown")

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