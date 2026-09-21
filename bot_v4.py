import os
import io
import logging
import asyncio
import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
import google.generativeai as genai
from pydub import AudioSegment
from aiohttp import web

# ---------------------------------------------------------
# تنظیمات پایه
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PORT = int(os.getenv("PORT", 10000))

if not BOT_TOKEN or not GEMINI_API_KEY:
    raise ValueError("❌ متغیرهای محیطی یافت نشدند.")

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-1.5-flash")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

subscribers = set()
user_portfolio = {}

# ---------------------------------------------------------
# وب‌سرور داخلی Render
# ---------------------------------------------------------
async def handle_health_check(request):
    return web.Response(text="AlphaEngine Pro Elite Suite is Active!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Healthcheck web server running on port {PORT}")

async def send_long_message(event: types.Message | types.CallbackQuery, text: str, parse_mode: str = "Markdown", reply_markup=None):
    target = event.message if isinstance(event, CallbackQuery) else event
    MAX_LEN = 3900
    if len(text) <= MAX_LEN:
        try:
            await target.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception:
            await target.answer(text, reply_markup=reply_markup)
        return
    chunks = [text[i:i + MAX_LEN] for i in range(0, len(text), MAX_LEN)]
    for chunk in chunks:
        try:
            await target.answer(chunk, parse_mode=parse_mode)
        except Exception:
            await target.answer(chunk)
        await asyncio.sleep(0.3)

# ---------------------------------------------------------
# موتور محاسبات بازار با تایم‌فریم‌های پویا
# ---------------------------------------------------------
async def fetch_market_data(symbol: str, timeframe: str = "1h", limit: int = 100):
    exchange = ccxt.binance({'enableRateLimit': True, 'timeout': 10000})
    try:
        formatted_symbol = symbol.upper().strip()
        if not formatted_symbol.endswith("/USDT") and "/" not in formatted_symbol:
            formatted_symbol = f"{formatted_symbol}/USDT"

        ohlcv = await exchange.fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=limit)
        await exchange.close()

        if not ohlcv or len(ohlcv) < 30:
            return None, "دیتای کافی یافت نشد."

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + (gain / loss)))

        # EMA & MACD
        df['EMA_20'] = df['close'].ewm(span=20).mean()
        df['EMA_50'] = df['close'].ewm(span=50).mean()
        exp1 = df['close'].ewm(span=12).mean()
        exp2 = df['close'].ewm(span=26).mean()
        df['MACD'] = exp1 - exp2
        df['Signal'] = df['MACD'].ewm(span=9).mean()

        # Bollinger Bands
        sma = df['close'].rolling(20).mean()
        std = df['close'].rolling(20).std()
        df['BB_Upper'] = sma + (std * 2)
        df['BB_Lower'] = sma - (std * 2)

        last = df.iloc[-1]
        price = float(last['close'])

        stats = {
            "symbol": formatted_symbol,
            "timeframe": timeframe,
            "current_price": price,
            "change_24h": round(((price - float(df.iloc[0]['close'])) / float(df.iloc[0]['close'])) * 100, 2),
            "rsi": round(float(last['RSI']), 2) if not np.isnan(last['RSI']) else 50,
            "ema_20": round(float(last['EMA_20']), 4),
            "ema_50": round(float(last['EMA_50']), 4),
            "macd": round(float(last['MACD']), 4),
            "bb_upper": round(float(last['BB_Upper']), 4),
            "bb_lower": round(float(last['BB_Lower']), 4),
        }
        return stats, None
    except Exception as e:
        await exchange.close()
        return None, str(e)

# اسکنر پامپ
async def scan_market_pumps():
    exchange = ccxt.binance({'enableRateLimit': True, 'timeout': 15000})
    try:
        tickers = await exchange.fetch_tickers()
        await exchange.close()
        pairs = [
            {"symbol": k, "change": round(v.get('percentage', 0), 2), "price": v.get('last', 0)}
            for k, v in tickers.items() if k.endswith('/USDT') and v.get('quoteVolume', 0) > 1000000
        ]
        pairs.sort(key=lambda x: x['change'], reverse=True)
        return pairs[:10], None
    except Exception as e:
        await exchange.close()
        return [], str(e)

# ---------------------------------------------------------
# منوها و دستورات
# ---------------------------------------------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    subscribers.add(message.from_user.id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={message.from_user.id}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 اسکنر پامپ", callback_data="menu_pump"), InlineKeyboardButton(text="📋 تحلیل روزانه بازار", callback_data="menu_daily")],
        [InlineKeyboardButton(text="📊 پورتفوی من", callback_data="menu_portfolio"), InlineKeyboardButton(text="💎 وضعیت VIP", callback_data="menu_vip")]
    ])

    welcome_text = (
        "⚡ **AlphaEngine Pro | Ultra Trading Suite** ⚡\n\n"
        "ترمینال پیشرفته هوش مصنوعی و الگوریتم‌های ترید.\n"
        f"🔗 لینک دعوت شما:\n`{ref_link}`"
    )
    await message.answer(welcome_text, parse_mode="Markdown", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("menu_"))
async def handle_menu(callback: CallbackQuery):
    action = callback.data.split("_")[1]
    if action == "pump":
        await callback.message.edit_text("🔍 در حال اسکن پامپ‌های بازار...")
        pumps, _ = await scan_market_pumps()
        text = "🚀 **لیست برترین ارزهای پرنوسان بازار:**\n\n"
        for p in pumps:
            text += f"🟢 **{p['symbol']}** ➔ رشد: `+{p['change']}%` | قیمت: `{p['price']}`\n"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu_back")]])
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    elif action == "daily":
        await callback.message.edit_text("📈 در حال آماده‌سازی گزارش جامع و تحلیل روزانه بازار...")
        pumps, _ = await scan_market_pumps()
        prompt = (
            f"دیتای برترین نوسانات بازار کریپتو:\n{pumps[:5]}\n\n"
            "یک گزارش جامع و تحلیل روزانه بازار (Market Daily Brief) شامل وضعیت بیت‌کوین، آلت‌کوین‌ها، سنتیمنت کلی و فرصت‌های ترید امروز بنویس."
        )
        resp = gemini_model.generate_content(prompt)
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu_back")]])
        await callback.message.edit_text(resp.text, parse_mode="Markdown", reply_markup=kb)
    elif action == "portfolio":
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu_back")]])
        await callback.message.edit_text("📊 پورتفوی مجازی شما خالی است. با دکمه‌های زیر هر سیگنال را ذخیره کنید.", parse_mode="Markdown", reply_markup=kb)
    elif action == "vip":
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu_back")]])
        await callback.message.edit_text("💎 سطح کاربری: VIP رایگان با دعوت ۳ دوست.", parse_mode="Markdown", reply_markup=kb)
    elif action == "back":
        await cmd_start(callback.message)
    await callback.answer()

@dp.message(Command("pump"))
async def cmd_pump(message: Message):
    pumps, _ = await scan_market_pumps()
    text = "🚀 **اسکنر زنده پامپ بازار:**\n\n"
    for p in pumps:
        text += f"🟢 **{p['symbol']}** ➔ `+{p['change']}%` | قیمت: `{p['price']}`\n"
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    status = await message.answer("📈 در حال تهیه گزارش تحلیل روزانه بازار...", parse_mode="Markdown")
    pumps, _ = await scan_market_pumps()
    prompt = f"برترین نوسانات بازار:\n{pumps[:5]}\n\nیک گزارش تحلیلی روزانه جامع (Market Daily Brief) با ساختار حرفه‌ای بنویس."
    resp = gemini_model.generate_content(prompt)
    await status.delete()
    await send_long_message(message, resp.text)

@dp.message(Command("scan"))
async def cmd_scan(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("⚠️ لطفاً نام نماد را وارد کنید. مثال: `/scan SOL`", parse_mode="Markdown")
        return
    await process_symbol(message, args[1], timeframe="1h")

async def process_symbol(message: Message, symbol: str, timeframe: str):
    status = await message.answer(f"🔄 آنالیز `{symbol}` در تایم‌فریم `{timeframe}`...", parse_mode="Markdown")
    stats, err = await fetch_market_data(symbol, timeframe=timeframe)

    if stats:
        prompt = (
            f"دیتای صرافی برای {stats['symbol']} در تایم‌فریم {timeframe}:\n"
            f"قیمت: {stats['current_price']} | تغییر: {stats['change_24h']}%\n"
            f"RSI: {stats['rsi']} | MACD: {stats['macd']} | EMA20: {stats['ema_20']}\n\n"
            "تحلیل پرایس اکشن حرفه‌ای با تعیین محدوده ورود، حد سود و ضرر بده."
        )
    else:
        prompt = f"تحلیل تکنیکال کامل برای {symbol} در تایم‌فریم {timeframe}"

    try:
        response = gemini_model.generate_content(prompt)
        await status.delete()

        # دکمه‌های انتخاب تایم‌فریم‌های گسترده برای این تحلیل
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="5m", callback_data=f"tf_{symbol}_5m"),
                InlineKeyboardButton(text="15m", callback_data=f"tf_{symbol}_15m"),
                InlineKeyboardButton(text="1h", callback_data=f"tf_{symbol}_1h"),
            ],
            [
                InlineKeyboardButton(text="4h", callback_data=f"tf_{symbol}_4h"),
                InlineKeyboardButton(text="1d", callback_data=f"tf_{symbol}_1d"),
                InlineKeyboardButton(text="1w", callback_data=f"tf_{symbol}_1w"),
            ],
            [
                InlineKeyboardButton(text="⭐ ذخیره در پورتفوی", callback_data=f"save_{symbol.upper()}")
            ]
        ])
        await send_long_message(message, response.text, reply_markup=keyboard)
    except Exception as e:
        await status.edit_text(f"⚠️ خطا: `{e}`", parse_mode="Markdown")

@dp.callback_query(F.data.startswith("tf_"))
async def handle_dynamic_tf(callback: CallbackQuery):
    parts = callback.data.split("_")
    symbol = parts[1]
    tf = parts[2]
    await callback.answer(f"تغییر به تایم‌فریم {tf}")
    await process_symbol(callback.message, symbol, timeframe=tf)

@dp.callback_query(F.data.startswith("save_"))
async def save_signal(callback: CallbackQuery):
    sym = callback.data.split("_")[1]
    await callback.answer(f"✅ سیگنال {sym} ذخیره شد!")

# پردازش متون، چارت‌ها و ویس‌ها
@dp.message(F.text & ~F.command)
async def handle_text(message: Message):
    text = message.text.strip()
    if len(text) <= 10 and text.isalnum():
        await process_symbol(message, text, timeframe="1h")
    else:
        status = await message.answer("🧠 در حال پردازش...", parse_mode="Markdown")
        try:
            resp = gemini_model.generate_content(f"به این سوال کریپتو پاسخ بده: {text}")
            await status.delete()
            await send_long_message(message, resp.text)
        except Exception as e:
            await status.edit_text(f"⚠️ خطا: `{e}`")

@dp.message(F.photo)
async def handle_photo(message: Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5m", callback_data="chart_5m"), InlineKeyboardButton(text="15m", callback_data="chart_15m"), InlineKeyboardButton(text="1h", callback_data="chart_1h")],
        [InlineKeyboardButton(text="4h", callback_data="chart_4h"), InlineKeyboardButton(text="1d", callback_data="chart_1d"), InlineKeyboardButton(text="1w", callback_data="chart_1w")]
    ])
    await message.answer("📊 تایم‌فریم چارت را انتخاب کنید:", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("chart_"))
async def handle_chart_tf(callback: CallbackQuery):
    tf = callback.data.split("_")[1]
    status = await callback.message.answer(f"🧠 آنالیز تصویر چارت در تایم‌فریم {tf}...")
    try:
        photo_msg = callback.message.reply_to_message or callback.message
        photo = photo_msg.photo[-1]
        file_info = await bot.get_file(photo.file_id)
        img_bytes = await bot.download_file(file_info.file_path)

        resp = gemini_model.generate_content([
            f"این یک چارت کریپتو در تایم‌فریم {tf} است. الگوها و سناریوی ورود را تحلیل کن.",
            {"mime_type": "image/jpeg", "data": img_bytes.read()}
        ])
        await status.delete()
        await send_long_message(callback.message, resp.text)
    except Exception as e:
        await status.edit_text(f"⚠️ خطا: `{e}`")

@dp.message(F.voice)
async def handle_voice(message: Message):
    status = await message.answer("🎙 در حال آنالیز ویس...")
    try:
        file_info = await bot.get_file(message.voice.file_id)
        v_bytes = await bot.download_file(file_info.file_path)
        audio = AudioSegment.from_file(io.BytesIO(v_bytes.read()), format="ogg")
        out = io.BytesIO()
        audio.export(out, format="mp3")
        out.seek(0)

        resp = gemini_model.generate_content([
            "به این سوال صوتی معاملاتی پاسخ حرفه‌ای بده:",
            {"mime_type": "audio/mp3", "data": out.read()}
        ])
        await status.delete()
        await send_long_message(message, resp.text)
    except Exception as e:
        await status.edit_text(f"⚠️ خطا: `{e}`")

async def main():
    logging.info("🚀 Starting AlphaEngine Pro Elite Suite...")
    asyncio.create_task(start_web_server())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
