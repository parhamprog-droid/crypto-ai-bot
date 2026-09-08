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
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ai_client = genai.Client(api_key=GEMINI_API_KEY)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
exchange = ccxt.kucoin()

logging.basicConfig(level=logging.INFO)

USERS_FILE = "users.txt"

# مدیریت ذخیره‌سازی آی‌دی کاربران
def save_user(user_id: int):
    users = get_users()
    if user_id not in users:
        with open(USERS_FILE, "a") as f:
            f.write(f"{user_id}\n")

def get_users() -> set:
    if not os.path.exists(USERS_FILE):
        return set()
    with open(USERS_FILE, "r") as f:
        return {int(line.strip()) for line in f if line.strip().isdigit()}

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="BTC"), KeyboardButton(text="ETH")],
        [KeyboardButton(text="SOL"), KeyboardButton(text="SEI")]
    ],
    resize_keyboard=True
)

def calculate_rsi_wilder(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def get_crypto_data(symbol: str, timeframe: str = '1h') -> dict:
    formatted_symbol = f"{symbol.upper()}/USDT"
    try:
        ticker = exchange.fetch_ticker(formatted_symbol)
        ohlcv = exchange.fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=50)
        
        close_prices = [candle[4] for candle in ohlcv]
        rsi_val = calculate_rsi_wilder(close_prices)
        current_price = ticker['last']
        
        is_bearish = rsi_val > 65 or ticker.get('percentage', 0) < -2.5
        position_type = "Short 🔴" if is_bearish else "Long 🟢"
        
        if is_bearish:
            entry_1 = current_price
            entry_2 = round(current_price * 1.012, 4)
            entry_3 = round(current_price * 1.025, 4)
            tp1 = round(current_price * 0.985, 4)
            tp2 = round(current_price * 0.968, 4)
            tp3 = round(current_price * 0.945, 4)
            sl = round(current_price * 1.035, 4)
        else:
            entry_1 = current_price
            entry_2 = round(current_price * 0.988, 4)
            entry_3 = round(current_price * 0.975, 4)
            tp1 = round(current_price * 1.015, 4)
            tp2 = round(current_price * 1.032, 4)
            tp3 = round(current_price * 1.055, 4)
            sl = round(current_price * 0.965, 4)

        return {
            "symbol": symbol.upper(),
            "price": current_price,
            "change_24h": ticker.get('percentage', 0),
            "rsi": rsi_val,
            "timeframe": timeframe,
            "position_type": position_type,
            "entry_1": entry_1,
            "entry_2": entry_2,
            "entry_3": entry_3,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": sl
        }
    except Exception as e:
        logging.error(f"Error fetching data from KuCoin for {symbol}: {e}")
        return None

def get_chart_image_url(symbol: str) -> str:
    return f"https://charts2.finviz.com/chart.ashx?t={symbol.upper()}USD&ty=c&ta=1&p=d&s=l"

async def analyze_with_ai(market_data: dict) -> str:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    prompt = f"""
    تو یک ترمینال تحلیل حرفه‌ای کریپتو به نام "آلفا آنالیتیکس" هستی.
    بر اساس محاسبات دقیق فنی زیر، یک گزارش و تحلیل اکشن‌قیمت کوتاه (حداکثر ۱ جمله) بنویس و خروجی را دقیقاً در قالب زیر تنظیم کن.

    داده‌ها:
    - نماد: #{market_data['symbol']}USDT
    - تایم‌فریم: {market_data['timeframe']}
    - وضعیت پوزیشن: {market_data['position_type']}
    - قیمت: {market_data['price']}
    - تغییر ۲۴h: {market_data['change_24h']}%
    - شاخص RSI (Wilder): {market_data['rsi']}
    - پله ۱: {market_data['entry_1']} | پله ۲: {market_data['entry_2']} | پله ۳: {market_data['entry_3']}
    - تارگت ۱: {market_data['tp1']} | تارگت ۲: {market_data['tp2']} | تارگت ۳: {market_data['tp3']}
    - حد ضرر: {market_data['sl']}

    قالب الزامی (زیر ۹۰۰ کاراکتر):

    ⚡️ **آلفا آنالیتیکس | #{market_data['symbol']}USDT**
    ⏱ زمان: {current_time} | تایم‌فریم: {market_data['timeframe']}
    📌 تحلیل روند: {market_data['position_type']}
    📊 شاخص RSI: {market_data['rsi']} | ۲۴h: {market_data['change_24h']}%

    💵 **قیمت مارکت:** `{market_data['price']}` USDT

    🎯 **ستاپ معاملاتی**
    • موقعیت: {market_data['position_type']}
    • پله اول: `{market_data['entry_1']}`
    • پله دوم: `{market_data['entry_2']}`
    • پله سوم: `{market_data['entry_3']}`

    🚀 **اهداف سودآوری (Take Profit)**
    ▫️ هدف ۱: `{market_data['tp1']}`
    ▫️ هدف ۲: `{market_data['tp2']}`
    ▫️ هدف ۳: `{market_data['tp3']}`

    🛑 **حد ضرر (Stop Loss):** `{market_data['sl']}`

    ⚖️ **مدیریت ریسک & اهرم**
    • ریسک به ریوارد (R/R): 1:2.2
    • اهرم پیشنهادی: Cross 3x - 5x

    🧩 **تحلیل اکشن قیمت:** (یک جمله تحلیل مختصر بر اساس RSI و کندل‌ها)
    """
    
    models_to_try = ['gemini-3.6-flash', 'gemini-2.5-flash']

    for model_name in models_to_try:
        for attempt in range(3):
            try:
                response = ai_client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                if response and response.text:
                    return response.text
            except Exception as e:
                logging.error(f"Error with model {model_name}: {e}")
                if "429" in str(e) or "RESOURCEEXHAUSTED" in str(e):
                    await asyncio.sleep(2 * (attempt + 1))
                else:
                    break

    return "❌ ترافیک سرورهای هوش مصنوعی بالا است. لطفاً لحظاتی بعد مجدداً تلاش کنید."

# موتور سیگنال‌دهی اتوماتیک به تمام کاربران
async def auto_signal_job():
    users = get_users()
    if not users:
        logging.info("No users registered for auto-signals yet.")
        return
    
    watch_list = ["BTC", "ETH", "SOL", "SEI"]
    for symbol in watch_list:
        data = get_crypto_data(symbol)
        if data and (data['rsi'] >= 65 or data['rsi'] <= 35):
            analysis = await analyze_with_ai(data)
            chart_url = get_chart_image_url(symbol)
            
            for user_id in users:
                try:
                    await bot.send_photo(
                        chat_id=user_id,
                        photo=URLInputFile(chart_url),
                        caption=f"🔔 **سیگنال اتوماتیک آلفا**\n\n{analysis}",
                        parse_mode="Markdown"
                    )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logging.error(f"Failed to send signal to {user_id}: {e}")

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    save_user(message.from_user.id)
    await message.answer(
        "⚡️ **ترمینال تحلیل پیشرفته و سیگنال‌دهی دوطرفه آلفا**\n\n"
        "آیدی شما در سیستم ثبت شد و سیگنال‌های اتوماتیک برای شما ارسال خواهد شد.\n"
        "برای تحلیل آنی، نام هر ارزی را بفرستید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text)
async def process_crypto_name(message: types.Message):
    save_user(message.from_user.id)
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

    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_signal_job, 'interval', minutes=60)
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())