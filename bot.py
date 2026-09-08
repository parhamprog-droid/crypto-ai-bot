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
        logging.error(f"Error fetching data from KuCoin: {e}")
        return None

def get_chart_image_url(symbol: str) -> str:
    return f"https://charts2.finviz.com/chart.ashx?t={symbol.upper()}USD&ty=c&ta=1&p=d&s=l"

async def analyze_with_ai(market_data: dict) -> str:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    prompt = f"""
    تو یک سیستم هوشمند آنالیز تکنیکال و الگوریتمی کریپتو به نام "آلفا سیگنال" هستی. 
    بر اساس داده‌های زیر، یک ستاپ کامل معامله‌گری بساز.
    
    دقت کن: کل متن نباید از ۹۰۰ کاراکتر بیشتر شود تا در کپشن عکس تلگرام جا بشود.

    داده‌ها:
    - ارز: #{market_data['symbol']}USDT
    - قیمت فعلی: {market_data['price']}
    - تغییرات ۲۴ ساعته: {market_data['change_24h']}%
    - زمان: {current_time}

    قالب خروجی دقیقاً به این شکل باشد:

    💎 **آلفا سیگنال | #{market_data['symbol']}USDT**
    ⏱ زمان: {current_time}
    📌 روند: (صعودی 🟢 / نزولی 🔴 / رنج 🟡)

    📊 **اطلاعات بازار**
    • قیمت: `{market_data['price']}` USDT
    • تغییر ۲۴h: {market_data['change_24h']}%

    ⚡️ **پله‌های ورود (Buy Entry)**
    • مارکت: `{market_data['price']}`
    • پله ۲: (محاسبه ۱.۲٪ پایین‌تر)
    • پله ۳: (محاسبه ۲.۵٪ پایین‌تر)

    🎯 **تارگت‌های سود**
    ▫️ هدف ۱: (۱.۵٪ بالاتر)
    ▫️ هدف ۲: (۳.۲٪ بالاتر)
    ▫️ هدف ۳: (۵.۵٪ بالاتر)

    🛑 **حد ضرر (Stop Loss):** (۴.۵٪ پایین‌تر)

    ⚖️ **مدیریت ریسک**
    • ریسک به ریوارد (R/R): (محاسبه)
    • اهرم پیشنهادی: Cross 3x - 5x

    🧩 **تحلیل فنی:** (توضیح کوتاه ۱ جمله‌ای در مورد وضعیت عمومی بازار)
    """
    
    # اولویت‌بندی جدید مدل‌ها جهت جلوگیری از قطعی
    models_to_try = [
        'gemini-3.6-flash',
        'gemini-2.5-flash',
        'gemini-2.5-pro'
    ]

    last_err = None
    for model_name in models_to_try:
        try:
            logging.info(f"Trying Gemini model: {model_name}")
            response = ai_client.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            if response and response.text:
                return response.text
        except Exception as e:
            logging.error(f"Error with model {model_name}: {e}")
            last_err = e
            await asyncio.sleep(0.5)

    return f"❌ خطا در پاسخگویی هوش مصنوعی. لطفاً لحظاتی بعد مجدداً تلاش کنید.\n(جزئیات: {last_err})"

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer(
        "🧠 **ربات پیشرفته تحلیل تکنیکال و آلفا سیگنال**\n\n"
        "نام ارز مورد نظر (مانند BTC یا SOL) را وارد کنید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text)
async def process_crypto_name(message: types.Message):
    symbol = message.text.strip().replace("/", "").replace("#", "")
    
    if symbol.lower().startswith("start"):
        return

    wait_msg = await message.answer(f"🔎 در حال ترسیم چارت و محاسبات سیگنال برای {symbol.upper()}...")

    market_data = get_crypto_data(symbol)
    if not market_data:
        await wait_msg.edit_text("❌ ارز مورد نظر یافت نشد. لطفاً نماد انگلیسی معتبر وارد کنید.")
        return

    ai_analysis = await analyze_with_ai(market_data)
    chart_url = get_chart_image_url(symbol)

    try:
        await wait_msg.delete()
        await message.answer_photo(
            photo=URLInputFile(chart_url),
            caption=ai_analysis,
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Error sending photo: {e}")
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