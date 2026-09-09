import os
import asyncio
import logging
import ccxt.async_support as ccxt
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from google import genai
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

logging.basicConfig(level=logging.INFO)

# Config & Envs
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = os.getenv("ADMIN_ID")

if ADMIN_ID:
    try:
        ADMIN_ID = int(ADMIN_ID.strip())
    except ValueError:
        ADMIN_ID = None

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
ai_client = genai.Client(api_key=GEMINI_API_KEY)

user_data = {}

def get_user(user_id: int):
    if user_id not in user_data:
        user_data[user_id] = {"usage_count": 0, "is_vip": False}
    return user_data[user_id]

# Keyboards
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
        [KeyboardButton(text="📸 آنالیز عکس چارت (VIP)"), KeyboardButton(text="👤 حساب کاربری")]
    ],
    resize_keyboard=True
)

def timeframe_keyboard(symbol: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="15m", callback_data=f"tf:{symbol}:15m"),
            InlineKeyboardButton(text="1h", callback_data=f"tf:{symbol}:1h"),
            InlineKeyboardButton(text="4h", callback_data=f"tf:{symbol}:4h"),
            InlineKeyboardButton(text="1d", callback_data=f"tf:{symbol}:1d")
        ]
    ])

# Fetch Candle Data (OHLCV)
async def get_crypto_data(symbol="ETH/USDT", timeframe="1h", limit=30):
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
        
        # Format candles for chart: [o, h, l, c]
        candles = [{"o": c[1], "h": c[2], "l": c[3], "c": c[4]} for c in ohlcv[-15:]]
        
        return formatted_symbol, current_price, rsi, change_24h, candles
    except Exception as e:
        await exchange.close()
        logging.error(f"CCXT Error: {e}")
        return None, None, None, None, []

# Fetch Reliable Candlestick Chart via QuickChart API
async def fetch_chart_image(symbol: str, timeframe: str, candles: list):
    clean_symbol = symbol.replace("/", "").upper()
    if not candles:
        return None

    labels = [f"C{i+1}" for i in range(len(candles))]
    
    # Financial Candlestick configuration for QuickChart
    chart_config = {
        "type": "candlestick",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": f"{clean_symbol} ({timeframe})",
                "data": candles
            }]
        },
        "options": {
            "plugins": {
                "legend": {"display": True}
            }
        }
    }

    # Alternative: High-precision Line Chart with Dark Theme if Candlestick rendering varies
    import json
    chart_json = json.dumps({
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": f"{clean_symbol} ({timeframe}) - USDT",
                "data": [c["c"] for c in candles],
                "borderColor": "#00ff7f",
                "backgroundColor": "rgba(0, 255, 127, 0.1)",
                "fill": True,
                "tension": 0.2
            }]
        },
        "options": {
            "plugins": {
                "title": {"display": True, "text": f"{clean_symbol} Price Chart ({timeframe})", "color": "#ffffff"}
            },
            "scales": {
                "x": {"ticks": {"color": "#aaaaaa"}, "grid": {"color": "#333333"}},
                "y": {"ticks": {"color": "#aaaaaa"}, "grid": {"color": "#333333"}}
            }
        }
    })

    chart_url = f"https://quickchart.io/chart?bkg=%231e1e2f&c={chart_json}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(chart_url, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception as e:
            logging.error(f"Chart fetch error: {e}")
    return None

# AI Signal Generation
async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, price, rsi, change_24h, candles = await get_crypto_data(symbol, timeframe)
    if not price:
        return f"⚠️ ارز **{symbol}** پیدا نشد.", None

    prompt = f"""
    تو یک تحلیل‌گر تکنیکال ارشد کریپتو هستی. برای ارز {formatted_symbol} در تایم‌فریم {timeframe} تحلیل بنویس.
    اطلاعات بازار:
    - قیمت کنونی: {price} USDT
    - شاخص RSI: {rsi:.2f}
    - تغییرات: {change_24h:.2f}%

    خروجی را دقیقا با همین فرمت فاقد متن اضافی بفرست:
    ⚡️ AlphaEngine Pro | #{formatted_symbol.replace('/', '')}
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

    🧩 تحلیل اکشن قیمت: [توضیح تحلیلی ۲ جمله‌ای]
    """

    try:
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model="gemini-3.6-flash",
            contents=prompt
        )
        chart_bytes = await fetch_chart_image(formatted_symbol, timeframe, candles)
        return response.text, chart_bytes
    except Exception as e:
        return f"⚠️ خطا در سرویس هوش مصنوعی: {e}", None

# Handlers
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    get_user(message.from_user.id)
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "برای دریافت تحلیل و عکس نمودار، نام ارز را بفرستید (مثلاً `BTC` یا `ETH`).",
        reply_markup=main_keyboard
    )

@dp.message(F.text == "📊 شاخص ترس و طمع")
async def fear_and_greed(message: types.Message):
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.alternative.me/fng/") as resp:
            if resp.status == 200:
                data = await resp.json()
                item = data["data"][0]
                await message.answer(
                    f"📊 **شاخص ترس و طمع:**\n\n🎯 عدد: **{item['value']}/100**\n📌 وضعیت: **{item['value_classification']}**"
                )

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال دریافت عکس چارت و تحلیل **{symbol}** در تایم‌فریم **{tf}**...")
    
    signal_text, chart_bytes = await generate_signal(symbol, tf)
    await callback.message.delete()

    if chart_bytes:
        photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
        await callback.message.answer_photo(photo=photo_file, caption=signal_text)
    else:
        await callback.message.answer(signal_text)
        
    await callback.answer()

@dp.message(F.text)
async def handle_symbol_input(message: types.Message):
    symbol_text = message.text.strip().upper()
    if symbol_text.startswith("/") or symbol_text in ["📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "📸 آنالیز عکس چارت (VIP)", "👤 حساب کاربری"]:
        return

    await message.answer(
        f"⏱ لطفاً تایم‌فریم تحلیل **{symbol_text}** را انتخاب کنید:",
        reply_markup=timeframe_keyboard(symbol_text)
    )

# Web Server Setup
async def handle_web(request):
    return web.Response(text="AlphaEngine Pro v3 Active!")

app = web.Application()
app.router.add_get('/', handle_web)

async def main():
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
