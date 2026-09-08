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

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def get_crypto_data(symbol: str) -> dict:
    formatted_symbol = f"{symbol.upper()}/USDT"
    try:
        ticker = exchange.fetch_ticker(formatted_symbol)
        ohlcv = exchange.fetch_ohlcv(formatted_symbol, timeframe='1h', limit=20)
        
        close_prices = [candle[4] for candle in ohlcv]
        rsi_val = calculate_rsi(close_prices)
        
        return {
            "symbol": symbol.upper(),
            "price": ticker['last'],
            "high": ticker['high'],
            "low": ticker['low'],
            "volume": ticker['baseVolume'],
            "change_24h": ticker.get('percentage', 0),
            "rsi": rsi_val,
            "recent_closes": close_prices[-5:]
        }
    except Exception as e:
        logging.error(f"Error fetching data from KuCoin: {e}")
        return None

def get_chart_image_url(symbol: str) -> str:
    return f"https://charts2.finviz.com/chart.ashx?t={symbol.upper()}USD&ty=c&ta=1&p=d&s=l"

async def analyze_with_ai(market_data: dict) -> str:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    prompt = f"""
    تو یک سیستم فوق‌پیشرفته و هوشمند سیگنال‌دهی تکنیکال و الگوریتمی کریپتو به نام "آلفا آنالیتیکس" هستی.
    با توجه به داده‌های زیر، یک ستاپ معاملاتی دوطرفه (اگر روند نزولی بود ستاپ Short/Sell و اگر صعودی/رنج بود ستاپ Long/Buy) صادر کن.

    داده‌های واقعی بازار:
    - نماد: #{market_data['symbol']}USDT
    - قیمت فعلی: {market_data['price']}
    - تغییرات ۲۴ ساعته: {market_data['change_24h']}%
    - شاخص RSI (14): {market_data['rsi']}
    - آخرین بسته‌شدن کندل‌ها (1h): {market_data['recent_closes']}
    - زمان: {current_time}

    دستورالعمل مهم روند:
    - اگر RSI بالا بود یا قیمت در حال ریزش شدید و تغییرات ۲۴h منفی بود، روند را "نزولی 🔴" اعلام کن و ستاپ **فروش / شورت (Short/Sell)** بده.
    - اگر RSI پایین بود یا روند مثبت بود، روند را "صعودی 🟢" اعلام کن و ستاپ **خرید / لاین (Long/Buy)** بده.
    - در غیر این صورت روند را "رنج 🟡" اعلام کن.

    قالب الزامی خروجی (حداکثر ۹۰۰ کاراکتر):

    ⚡️ **آلفا آنالیتیکس | #{market_data['symbol']}USDT**
    ⏱ زمان: {current_time}
    📌 تحلیل روند: (صعودی 🟢 / نزولی 🔴 / رنج 🟡)
    📊 شاخص RSI: {market_data['rsi']} | ۲۴h: {market_data['change_24h']}%

    💵 **قیمت مارکت:** `{market_data['price']}` USDT

    🎯 **ستاپ معاملاتی (بر اساس تشخیص روند)**
    • موقعیت: (خرید Long 🟢 یا فروش Short 🔴)
    • پله اول: `{market_data['price']}`
    • پله دوم: (بر اساس Long یا Short بودن، پله دوم ورود)
    • پله سوم: (پله سوم ورود)

    🚀 **اهداف سودآوری (Take Profit)**
    ▫️ هدف ۱: (محاسبه هدف ۱)
    ▫️ هدف ۲: (محاسبه هدف ۲)
    ▫️ هدف ۳: (محاسبه هدف ۳)

    🛑 **حد ضرر (Stop Loss):** (محاسبه استاپ دقیق)

    ⚖️ **مدیریت ریسک & اهرم**
    • ریسک به ریوارد (R/R): (محاسبه)
    • اهرم پیشنهادی: Cross 3x - 5x

    🧩 **تحلیل اکشن قیمت:** (یک جمله تحلیل بر اساس کندل‌ها و RSI)
    """
    
    # مدل‌های پایدار با سهمیه مناسب
    models_to_try = [
        'gemini-3.6-flash',
        'gemini-2.5-flash'
    ]

    last_err = None
    for model_name in models_to_try:
        for attempt in range(3):
            try:
                logging.info(f"Trying Gemini model: {model_name} (Attempt {attempt + 1})")
                response = ai_client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                if response and response.text:
                    return response.text
            except Exception as e:
                logging.error(f"Error with model {model_name}: {e}")
                last_err = e
                if "429" in str(e) or "RESOURCEEXHAUSTED" in str(e):
                    await asyncio.sleep(2 * (attempt + 1))
                else:
                    break

    return "❌ ترافیک سرورهای هوش مصنوعی بالا است. لطفاً چند ثانیه دیگر دوباره امتحان کنید."

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer(
        "⚡️ **ترمینال تحلیل پیشرفته و سیگنال‌دهی دوطرفه آلفا**\n\n"
        "برای دریافت چارت و ستاپ معاملاتی (Long/Short)، نام ارز را فرستاده یا از دکمه‌ها استفاده کنید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text)
async def process_crypto_name(message: types.Message):
    symbol = message.text.strip().replace("/", "").replace("#", "")
    
    if symbol.lower().startswith("start"):
        return

    wait_msg = await message.answer(f"🔎 در حال آنالیز کندل‌ها، RSI و ساخت ستاپ برای {symbol.upper()}...")

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