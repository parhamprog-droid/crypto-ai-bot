import os
import asyncio
import logging
import ccxt.async_support as ccxt
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from groq import AsyncGroq
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Environment Variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
ADMIN_ID = os.getenv("ADMIN_ID")

if ADMIN_ID:
    try:
        ADMIN_ID = int(ADMIN_ID.strip())
    except ValueError:
        logging.error("ADMIN_ID must be a numeric integer!")

# Initialize Bot & Groq AI
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
groq_client = AsyncGroq(api_key=GROQ_API_KEY)

# Active users storage
user_ids = set()

# Main Reply Keyboard
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⚡️ دریافت سیگنال دستی"), KeyboardButton(text="📊 شاخص ترس و طمع")],
        [KeyboardButton(text="🧮 محاسبه ریسک")]
    ],
    resize_keyboard=True
)

# Function to generate Inline Keyboard for Timeframes & Ranges
def get_timeframe_keyboard(symbol: str):
    clean_symbol = symbol.replace("/", "").replace("USDT", "")
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1m", callback_data=f"tf_1m_{clean_symbol}"),
            InlineKeyboardButton(text="30m", callback_data=f"tf_30m_{clean_symbol}"),
            InlineKeyboardButton(text="1h", callback_data=f"tf_1h_{clean_symbol}"),
            InlineKeyboardButton(text="1day", callback_data=f"tf_1d_{clean_symbol}"),
            InlineKeyboardButton(text="1week", callback_data=f"tf_1w_{clean_symbol}"),
        ],
        [
            InlineKeyboardButton(text="1months", callback_data=f"tf_1M_{clean_symbol}"),
            InlineKeyboardButton(text="6months", callback_data=f"tf_6M_{clean_symbol}"),
            InlineKeyboardButton(text="1range", callback_data=f"rng_1_{clean_symbol}"),
            InlineKeyboardButton(text="10ranges", callback_data=f"rng_10_{clean_symbol}"),
            InlineKeyboardButton(text="100ranges", callback_data=f"rng_100_{clean_symbol}"),
        ]
    ])

# Fetch Candle Data from KuCoin
async def get_crypto_data(symbol="ETH/USDT", timeframe="1h", limit=100):
    exchange = ccxt.kucoin()
    try:
        tf_map = {
            "1m": "1m", "30m": "30m", "1h": "1h", 
            "1d": "1day", "1w": "1week", "1M": "1month", "6M": "1month"
        }
        actual_tf = tf_map.get(timeframe, "1h")
        if timeframe == "6M":
            limit = 180
            
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=actual_tf, limit=limit)
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
        return current_price, rsi, change_24h, closes
    except Exception as e:
        await exchange.close()
        logging.error(f"Error fetching CCXT data: {e}")
        return None, None, None, None

# AI Signal Generation with Groq
async def generate_signal(symbol="ETH/USDT", timeframe="1h"):
    price, rsi, change_24h, closes = await get_crypto_data(symbol, timeframe)
    if not price:
        return "⚠️ خطا در دریافت اطلاعات صرافی. لطفا مجددا تلاش کنید."

    prompt = f"""
    تو یک تحلیل‌گر تکنیکال ارشد کریپتو هستی. برای ارز {symbol} در تایم‌فریم/محدوده {timeframe} تحلیل بنویس.
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

    models_to_try = ["llama-3.3-70b-versatile", "llama3-70b-8192"]
    last_error = ""

    for model_name in models_to_try:
        try:
            chat_completion = await groq_client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model_name,
            )
            if chat_completion.choices[0].message.content:
                return chat_completion.choices[0].message.content
        except Exception as e:
            logging.error(f"Error on {model_name}: {e}")
            last_error = str(e)

    return f"⚠️ خطا در سرویس هوش مصنوعی: {last_error}"

# Telegram Commands & Handlers
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_ids.add(message.from_user.id)
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n"
        "سیستم هوشمند آنالیز، مدیریت ریسک و سیگنال‌دهی کریپتو.",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text == "⚡️ دریافت سیگنال دستی")
async def manual_signal_btn(message: types.Message):
    user_ids.add(message.from_user.id)
    msg = await message.answer("🔄 در حال آنالیز مارکت و صدور سیگنال...")
    signal_text = await generate_signal("ETH/USDT", "1h")
    keyboard = get_timeframe_keyboard("ETH/USDT")
    await msg.delete()
    await message.answer(signal_text, reply_markup=keyboard)

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

# Callback Query Handler for Timeframe Buttons
@dp.callback_query(lambda c: c.data.startswith(('tf_', 'rng_')))
async def handle_timeframe_click(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split('_')
    tf_type = data_parts[0]
    selected_tf = data_parts[1]
    raw_symbol = data_parts[2] if len(data_parts) > 2 else "ETH"
    symbol = f"{raw_symbol}/USDT"

    display_label = f"{selected_tf} range" if tf_type == "rng" else selected_tf
    await callback_query.answer(f"در حال تولید سیگنال برای {display_label}...")

    new_signal = await generate_signal(symbol, display_label)
    keyboard = get_timeframe_keyboard(symbol)

    try:
        await callback_query.message.edit_text(new_signal, reply_markup=keyboard)
    except Exception:
        await callback_query.message.answer(new_signal, reply_markup=keyboard)

# Background Jobs (Scheduler)
async def auto_signal_job():
    if user_ids:
        signal = await generate_signal("ETH/USDT", "1h")
        keyboard = get_timeframe_keyboard("ETH/USDT")
        for uid in user_ids:
            try:
                await bot.send_message(uid, signal, reply_markup=keyboard)
            except Exception:
                pass

# Aiohttp Web Server for Render
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
