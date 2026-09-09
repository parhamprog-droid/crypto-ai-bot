import os
import asyncio
import logging
import ccxt.async_support as ccxt
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from google import genai
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

logging.basicConfig(level=logging.INFO)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = os.getenv("ADMIN_ID")

if ADMIN_ID:
    try:
        ADMIN_ID = int(ADMIN_ID.strip())
    except ValueError:
        logging.error("ADMIN_ID must be a numeric integer!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

ai_client = genai.Client(api_key=GEMINI_API_KEY)

user_ids = set()

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")]
    ],
    resize_keyboard=True
)

async def get_crypto_data(symbol="ETH/USDT", timeframe="1h", limit=100):
    exchange = ccxt.kucoin()
    try:
        formatted_symbol = symbol.upper().strip()
        if not formatted_symbol.endswith("/USDT") and not formatted_symbol.endswith("USDT"):
            formatted_symbol = f"{formatted_symbol}/USDT"
        elif formatted_symbol.endswith("USDT") and "/" not in formatted_symbol:
            formatted_symbol = formatted_symbol.replace("USDT", "/USDT")

        ohlcv = await exchange.fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=limit)
        await exchange.close()
        
        closes = [c[4] for c in ohlcv]
        current_price = closes[-1]
        
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
            
        avg_gain = sum(gains[-14:]) / 14 if len(gains) >= 14 else 1
        avg_loss = sum(losses[-14:]) / 14 if len(losses) >= 14 else 1
        rs = avg_gain / (avg_loss if avg_loss != 0 else 1)
        rsi = 100 - (100 / (1 + rs))
        
        change_24h = ((current_price - closes[0]) / closes[0]) * 100
        return formatted_symbol, current_price, rsi, change_24h
    except Exception as e:
        await exchange.close()
        logging.error(f"Error fetching CCXT data: {e}")
        return None, None, None, None

async def generate_signal(user_input_symbol: str, timeframe="1h"):
    symbol, price, rsi, change_24h = await get_crypto_data(user_input_symbol, timeframe)
    if not price:
        return f"⚠️ ارز **{user_input_symbol}** پیدا نشد. لطفاً نماد را درست وارد کنید (مثال: BTC یا ETH)."

    prompt = f"""
    تو یک تحلیل‌گر تکنیکال ارشد کریپتو هستی. برای ارز {symbol} در تایم‌فریم {timeframe} تحلیل بنویس.
    اطلاعات بازار:
    - قیمت کنونی: {price} USDT
    - شاخص RSI: {rsi:.2f}
    - تغییرات اخیر: {change_24h:.2f}%

    خروجی را دقیقا با همین فرمت فاقد متن اضافی بفرست:
    ⚡️ AlphaEngine Pro | #{symbol.replace('/', '')}
    ⏱ زمان: 2026-09-09 | تایم‌فریم: {timeframe}
    📌 تحلیل روند: [Long 🟢 یا Short 🔴]
    📊 شاخص RSI: {rsi:.2f} | تغییرات: {change_24h:.2f}%
    💵 قیمت مارکت: {price} USDT

    🎯 ستاپ معاملاتی
    • موقعیت: [Long 🟢 یا Short 🔴]
    • نقطه ورود: {price}
    • پله پشتیبان: [عدد منطقی]

    🚀 اهداف سودآوری (Take Profit)
    ▫️ TP1: [عدد]
    ▫️ TP2: [عدد]
    ▫️ TP3: [عدد]

    🛑 حد ضرر (Stop Loss): [عدد]
    ⚖️ ریسک به ریوارد: 1:2.2 | اهرم: Cross 3x-5x

    🧩 تحلیل اکشن قیمت: [توضیح تحلیلی ۲ جمله‌ای بر اساس RSI و اکشن قیمت]
    """

    try:
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model="gemini-3.6-flash",
            contents=prompt
        )
        return response.text
    except Exception as e:
        logging.error(f"Gemini Error: {e}")
        return f"⚠️ خطا در سرویس هوش مصنوعی: {e}"

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_ids.add(message.from_user.id)
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "برای دریافت تحلیل، فقط **نام ارز** را بفرستید (مثلاً: `BTC` یا `ETH` یا `SOL`).",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text == "📊 شاخص ترس و طمع")
async def fear_and_greed_btn(message: types.Message):
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.alternative.me/fng/") as resp:
            if resp.status == 200:
                data = await resp.json()
                item = data["data"][0]
                val = item["value"]
                classification = item["value_classification"]
                await message.answer(f"📊 **شاخص ترس و طمع بازار:**\n\n🎯 عدد: **{val}/100**\n📌 وضعیت: **{classification}**")
            else:
                await message.answer("⚠️ خطا در دریافت شاخص.")

@dp.message(F.text == "🧮 محاسبه ریسک")
async def risk_calculator_cmd(message: types.Message):
    await message.answer(
        "📐 **راهنمای محاسبه حجم پوزیشن (/risk):**\n\n"
        "فرمت ارسال دستور:\n"
        "`/risk [موجودی] [درصد ریسک] [قیمت ورود] [حدضرر]`\n\n"
        "مثال:\n`/risk 1000 2 2500 2450`"
    )

@dp.message(Command("risk"))
async def process_risk(message: types.Message):
    try:
        parts = message.text.split()
        capital = float(parts[1])
        risk_pct = float(parts[2])
        entry = float(parts[3])
        sl = float(parts[4])

        risk_amount = capital * (risk_pct / 100)
        price_diff = abs(entry - sl)
        position_size = risk_amount / (price_diff / entry)

        await message.answer(
            f"🧮 **نتیجه مدیریت ریسک:**\n\n"
            f"💵 مبلغ مجاز برای ریسک: **{risk_amount:.2f} USDT**\n"
            f"📊 حجم کل پوزیشن: **{position_size:.2f} USDT**\n"
            f"⚠️ در صورت لمس SL دقیقاً {risk_amount:.2f} دلار متضرر می‌شوید."
        )
    except Exception:
        await message.answer("⚠️ فرمت اشتباه است. مثال صحیح:\n`/risk 1000 2 2500 2450`")

@dp.message(Command("broadcast"))
async def broadcast_msg(message: types.Message):
    if ADMIN_ID and message.from_user.id == ADMIN_ID:
        text = message.text.replace("/broadcast", "").strip()
        if not text:
            await message.answer("⚠️ متن پیام را وارد کنید.")
            return
        count = 0
        for uid in user_ids:
            try:
                await bot.send_message(uid, f"📢 **پیام ادمین:**\n\n{text}")
                count += 1
            except Exception:
                pass
        await message.answer(f"✅ پیام به {count} کاربر ارسال شد.")

@dp.message(F.text)
async def handle_symbol_input(message: types.Message):
    user_ids.add(message.from_user.id)
    symbol_text = message.text.strip()
    
    msg = await message.answer(f"🔄 در حال دریافت اطلاعات و آنالیز **{symbol_text.upper()}**...")
    signal_text = await generate_signal(symbol_text, "1h")
    await msg.delete()
    await message.answer(signal_text)

async def auto_signal_job():
    if user_ids:
        signal = await generate_signal("ETH", "1h")
        for uid in user_ids:
            try:
                await bot.send_message(uid, signal)
            except Exception:
                pass

async def handle_web(request):
    return web.Response(text="AlphaEngine Pro is Active!")

app = web.Application()
app.router.add_get('/', handle_web)

async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_signal_job, 'interval', hours=1)
    scheduler.start()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
