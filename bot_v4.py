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
# تنظیمات لاگ‌گیری و متغیرهای محیطی
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PORT = int(os.getenv("PORT", 10000))

if not BOT_TOKEN or not GEMINI_API_KEY:
    raise ValueError("❌ متغیرهای BOT_TOKEN یا GEMINI_API_KEY یافت نشدند.")

# تنظیم هوش مصنوعی Gemini
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-1.5-flash")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ذخیره کاربران برای ارسال هشدارهای پس‌زمینه
subscribers = set()

# ---------------------------------------------------------
# وب‌سرور داخلی جهت رفع خطای Port Binding در Render
# ---------------------------------------------------------
async def handle_health_check(request):
    return web.Response(text="AlphaEngine Pro Bot & Pump Scanner is Active!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Healthcheck web server listening on port {PORT}")

# ---------------------------------------------------------
# مدیریت محدودیت طول پیام در تلگرام (۴۰۹۶ کاراکتر)
# ---------------------------------------------------------
async def send_long_message(event: types.Message | types.CallbackQuery, text: str, parse_mode: str = "Markdown"):
    target = event.message if isinstance(event, CallbackQuery) else event
    MAX_LEN = 3900

    if len(text) <= MAX_LEN:
        try:
            await target.answer(text, parse_mode=parse_mode)
        except Exception:
            await target.answer(text)
        return

    chunks = [text[i:i + MAX_LEN] for i in range(0, len(text), MAX_LEN)]
    for chunk in chunks:
        try:
            await target.answer(chunk, parse_mode=parse_mode)
        except Exception:
            await target.answer(chunk)
        await asyncio.sleep(0.3)

# ---------------------------------------------------------
# موتور تحلیل بازار و اندیکاتورهای تکنیکال (CCXT + Pandas)
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
            return None, "دیتای کافی برای محاسبات تکنیکال یافت نشد."

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # ۱. محاسبه RSI (14)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # ۲. محاسبه EMAs
        df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()

        # ۳. محاسبه MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # ۴. محاسبه باندهای بولینگر (Bollinger Bands)
        df['SMA_20'] = df['close'].rolling(window=20).mean()
        df['STD_20'] = df['close'].rolling(window=20).std()
        df['BB_Upper'] = df['SMA_20'] + (df['STD_20'] * 2)
        df['BB_Lower'] = df['SMA_20'] - (df['STD_20'] * 2)

        # ۵. محاسبه ATR
        df['TR'] = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift(1)),
                abs(df['low'] - df['close'].shift(1))
            )
        )
        df['ATR'] = df['TR'].rolling(window=14).mean()

        # ۶. محاسبه ضریب جهش حجم (Volume Surge)
        df['Vol_SMA_20'] = df['volume'].rolling(window=20).mean()
        df['Vol_Surge'] = df['volume'] / df['Vol_SMA_20']

        last = df.iloc[-1]
        prev = df.iloc[-2]

        price = float(last['close'])
        atr = float(last['ATR']) if not np.isnan(last['ATR']) else price * 0.02

        stats = {
            "symbol": formatted_symbol,
            "timeframe": timeframe,
            "current_price": price,
            "price_change_24h": round(((price - float(df.iloc[0]['close'])) / float(df.iloc[0]['close'])) * 100, 2),
            "high": float(last['high']),
            "low": float(last['low']),
            "volume": float(last['volume']),
            "vol_surge_ratio": round(float(last['Vol_Surge']), 2) if not np.isnan(last['Vol_Surge']) else 1.0,
            "rsi": round(float(last['RSI']), 2) if not np.isnan(last['RSI']) else 50.0,
            "rsi_prev": round(float(prev['RSI']), 2) if not np.isnan(prev['RSI']) else 50.0,
            "ema_20": round(float(last['EMA_20']), 4),
            "ema_50": round(float(last['EMA_50']), 4),
            "ema_200": round(float(last['EMA_200']), 4),
            "macd": round(float(last['MACD']), 4),
            "macd_signal": round(float(last['Signal_Line']), 4),
            "bb_upper": round(float(last['BB_Upper']), 4),
            "bb_lower": round(float(last['BB_Lower']), 4),
            "atr": round(atr, 4),
        }
        return stats, None

    except Exception as e:
        await exchange.close()
        logging.error(f"Fetch Error: {e}")
        return None, str(e)

# ---------------------------------------------------------
# اسکنر پامپ و دامپ بازار
# ---------------------------------------------------------
async def scan_market_pumps(top_n: int = 15):
    exchange = ccxt.binance({'enableRateLimit': True, 'timeout': 15000})
    try:
        tickers = await exchange.fetch_tickers()
        await exchange.close()

        usdt_pairs = {k: v for k, v in tickers.items() if k.endswith('/USDT') and v.get('quoteVolume', 0) > 1000000}

        pumps = []
        for symbol, ticker in usdt_pairs.items():
            change_24h = ticker.get('percentage', 0) or 0
            volume = ticker.get('quoteVolume', 0) or 0
            last_price = ticker.get('last', 0) or 0

            if change_24h >= 3.0 or change_24h <= -3.0:
                pumps.append({
                    "symbol": symbol,
                    "change": round(change_24h, 2),
                    "price": last_price,
                    "volume_m": round(volume / 1000000, 2)
                })

        pumps.sort(key=lambda x: x['change'], reverse=True)
        return pumps[:top_n], None
    except Exception as e:
        await exchange.close()
        logging.error(f"Pump Scanner Error: {e}")
        return [], str(e)

async def background_pump_alert_task():
    while True:
        try:
            await asyncio.sleep(300)
            pumps, err = scan_market_pumps(top_n=5)
            if pumps and subscribers:
                alert_text = "🚀 **[Hedge Alert] هشدار زنده ارزهای در حال پامپ شدید:**\n\n"
                has_big_pump = False
                for item in pumps:
                    if item['change'] >= 5.0:
                        has_big_pump = True
                        alert_text += f"🔥 **{item['symbol']}** | رشد: `+{item['change']}%` | قیمت: `{item['price']}` | حجم: `${item['volume_m']}M`\n"

                if has_big_pump:
                    for uid in list(subscribers):
                        try:
                            await bot.send_message(uid, alert_text, parse_mode="Markdown")
                        except Exception:
                            pass
        except Exception as e:
            logging.error(f"Background alert task error: {e}")
            await asyncio.sleep(60)

# ---------------------------------------------------------
# دستورات ربات (Commands)
# ---------------------------------------------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    subscribers.add(user_id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    welcome_text = (
        "⚡ **AlphaEngine Pro v4.0 | Ultra Trading & Pump Suite** ⚡\n\n"
        "به قدرتمندترین ربات معامله‌گری، اسکنر هوشمند پامپ/دامپ و تحلیلگر AI خوش آمدید!\n\n"
        "🔥 **امکانات فعال در این نسخه:**\n"
        "1️⃣ **اسکنر پامپ و دامپ (`/pump`):** شناسایی زنده ارزهای در حال رشد شدید و جهش حجم.\n"
        "2️⃣ **سیگنال‌ساز اتوماتیک (CCXT + Indicators):** محاسبه زنده RSI, MACD, EMAs, Bollinger Bands, ATR.\n"
        "3️⃣ **چارت‌خوان چندزمانی (Vision AI):** تحلیل چارت با هوش مصنوعی در ۴ تایم‌فریم.\n"
        "4️⃣ **پردازش صوتی استراتژی (Voice AI):** آنالیز مستقیم ویس‌های معاملاتی.\n\n"
        "📌 **دستورات کاربردی:**\n"
        "▫️ `/pump` - اسکن زنده ارزهای پامپی بازار\n"
        "▫️ `/scan BTC` - تحلیل زنده تکنیکال و سیگنال نماد\n"
        "▫️ `/vip` - وضعیت اشتراک و لینک رفرال\n"
        "▫️ `/help` - راهنمای کامل دستورات\n\n"
        f"🔗 **لینک دعوت اختصاصی شما:**\n`{ref_link}`"
    )
    await message.answer(welcome_text, parse_mode="Markdown")

@dp.message(Command("pump"))
async def cmd_pump_scanner(message: Message):
    status = await message.answer("🔍 **در حال اسکن زنده بازار برای یافتن ارزهای در حال پامپ/دامپ...**", parse_mode="Markdown")
    pumps, err = scan_market_pumps(top_n=10)

    if err:
        await status.edit_text(f"❌ خطایی در اسکن بازار رخ داد: `{err}`", parse_mode="Markdown")
        return

    if not pumps:
        await status.edit_text("ℹ️ در حال حاضر نوسان شدیدی در بازار ثبت نشده است.")
        return

    msg = "🚀 **[AlphaEngine] لیست اسکن زنده ارزهای پامپی و پرنوسان بازار:**\n\n"
    for idx, item in enumerate(pumps, 1):
        icon = "🟢" if item['change'] > 0 else "🔴"
        msg += f"{idx}. {icon} **{item['symbol']}**\n"
        msg += f"   ├ تغییر ۲۴ ساعته: `{item['change']}%`\n"
        msg += f"   ├ قیمت لحظه‌ای: `{item['price']}`\n"
        msg += f"   └ حجم معاملات: `${item['volume_m']}M`\n\n"

    msg += "💡 *برای دریافت تحلیل و سیگنال هر نماد، کافیست نام آن را ارسال کنید (مثلاً SOL)*"
    await status.delete()
    await send_long_message(message, msg)

@dp.message(Command("scan"))
async def cmd_scan_symbol(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("⚠️ لطفاً نام نماد را وارد کنید. مثال:\n`/scan BTC` یا `/scan SOL`", parse_mode="Markdown")
        return
    await process_symbol_analysis(message, args[1])

@dp.message(Command("vip"))
async def cmd_vip(message: Message):
    user_id = message.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    vip_msg = (
        "💎 **سیستم کاربری VIP و ارتقا اکانت**\n\n"
        "با دعوت ۳ معامله‌گر دیگر، اشتراک VIP شما فعال شده و هشدارهای لحظه‌ای پامپ مستقیماً برای شما ارسال می‌گردد.\n\n"
        "📊 **وضعیت اکانت شما:** کاربر عادی (Free)\n"
        "👥 **تعداد دعوت‌های فعال:** 0 / 3\n\n"
        f"🔗 **لینک اختصاصی شما:**\n`{ref_link}`"
    )
    await message.answer(vip_msg, parse_mode="Markdown")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "📚 **راهنمای کامل ربات AlphaEngine Pro:**\n\n"
        "1️⃣ **ارسال نام نماد:** ارسال `SOL`, `ETH`, `BTC` جهت استخراج اندیکاتورها و سیگنال ورود/خروج.\n"
        "2️⃣ **دستور `/pump`:** اسکنر هوشمند ارزهای دارای رشد شدید و جهش حجم.\n"
        "3️⃣ **ارسال عکس چارت:** تحلیل تکنیکال با هوش مصنوعی در تایم‌فریم‌های 15m, 1h, 4h, 1d.\n"
        "4️⃣ **ارسال ویس:** پردازش و آنالیز صوتی استراتژی معاملاتی شما."
    )
    await message.answer(help_text, parse_mode="Markdown")

# ---------------------------------------------------------
# آنالیزور مرکزی نمادها و ساخت سیگنال
# ---------------------------------------------------------
async def process_symbol_analysis(message: Message, symbol: str):
    status_msg = await message.answer(f"🔄 در حال اتصال به صرافی، استخراج دیتای کندل‌ها و محاسبه اندیکاتورهای `{symbol}`...", parse_mode="Markdown")

    stats, err = await fetch_market_data(symbol, timeframe="1h")

    if stats:
        prompt = (
            f"شما سیستم تحلیلی الگوریتمی AlphaEngine Pro هستید. دیتای زنده و اندیکاتورهای زیر از بازار استخراج شده‌اند:\n\n"
            f"📌 نماد: {stats['symbol']} | تایم‌فریم: {stats['timeframe']}\n"
            f"💵 قیمت لحظه‌ای: {stats['current_price']} | تغییر ۲۴ساعته: {stats['price_change_24h']}%\n"
            f"📊 High/Low: {stats['high']} / {stats['low']}\n"
            f"📈 شاخص RSI: {stats['rsi']} (قبلی: {stats['rsi_prev']})\n"
            f"📉 EMAs: EMA20={stats['ema_20']} | EMA50={stats['ema_50']} | EMA200={stats['ema_200']}\n"
            f"⚡ MACD: {stats['macd']} | Signal: {stats['macd_signal']}\n"
            f"🌐 باند بولینگر: Upper={stats['bb_upper']} | Lower={stats['bb_lower']}\n"
            f"🌊 ضریب جهش حجم: {stats['vol_surge_ratio']}x | ATR={stats['atr']}\n\n"
            "با توجه به این مشخصات ریاضی زنده:\n"
            "۱. وضعیت روند کلی و قدرت خریداران/فروشندگان را تحلیل کنید.\n"
            "۲. یک **سیگنال کامل معاملاتی** ارائه دهید شامل جهت معامله (Long/Short)، محدوده ورود، حد سودها (TP1, TP2, TP3) و حد زیان (Stop Loss).\n"
            "پاسخ را بسیار خوانا، شکیل و با تیتربندی کامل ارائه دهید."
        )
    else:
        prompt = f"کاربر عبارت '{symbol}' را ارسال کرده است. تحلیل کامل تکنیکال و فاندامنتال برای این نماد ارائه بده."

    try:
        response = gemini_model.generate_content(prompt)
        await status_msg.delete()
        await send_long_message(message, response.text)
    except Exception as e:
        logging.error(f"Analysis error: {e}")
        await status_msg.edit_text(f"⚠️ خطایی در پردازش رخ داد:\n`{e}`", parse_mode="Markdown")

# ---------------------------------------------------------
# پردازش پیام‌های متنی، تصاویر و ویس‌ها
# ---------------------------------------------------------
@dp.message(F.text & ~F.command)
async def handle_text(message: Message):
    text = message.text.strip()
    subscribers.add(message.from_user.id)

    if len(text) <= 10 and text.isalnum():
        await process_symbol_analysis(message, text)
    else:
        status_msg = await message.answer("🧠 در حال پردازش سوال معاملاتی شما با Gemini AI...", parse_mode="Markdown")
        try:
            prompt = f"شما دستیار معامله‌گری AlphaEngine Pro هستید. به سوال معاملاتی زیر پاسخ دهید:\n\"{text}\""
            response = gemini_model.generate_content(prompt)
            await status_msg.delete()
            await send_long_message(message, response.text)
        except Exception as e:
            await status_msg.edit_text(f"⚠️ خطایی رخ داد:\n`{e}`", parse_mode="Markdown")

@dp.message(F.photo)
async def handle_photo(message: Message):
    subscribers.add(message.from_user.id)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="15m", callback_data="tf_15m"),
            InlineKeyboardButton(text="1h", callback_data="tf_1h"),
            InlineKeyboardButton(text="4h", callback_data="tf_4h"),
            InlineKeyboardButton(text="1d", callback_data="tf_1d"),
        ]
    ])
    await message.answer("📊 **تایم‌فریم چارت ارسالی را برای اسکن هوشمند انتخاب کنید:**", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("tf_"))
async def handle_timeframe_click(callback: CallbackQuery):
    tf = callback.data.split("_")[1]
    await callback.answer(f"تایم‌فریم {tf} انتخاب شد.")
    status_msg = await callback.message.answer(f"🧠 در حال تحلیل تصویر چارت در تایم‌فریم {tf}...")

    try:
        photo_msg = callback.message.reply_to_message or callback.message
        if not photo_msg.photo:
            await status_msg.edit_text("❌ تصویر چارت یافت نشد.")
            return

        photo = photo_msg.photo[-1]
        file_info = await bot.get_file(photo.file_id)
        img_bytes = await bot.download_file(file_info.file_path)

        prompt = (
            f"شما تحلیل‌گر ارشد پرایس‌اکشن هستید. این تصویر یک چارت کریپتو در تایم‌فریم {tf} است.\n"
            "تحلیل کاملی شامل الگوها، حمایت/مقاومت، و پیشنهاد خرید/فروش ارائه دهید."
        )

        response = gemini_model.generate_content([
            prompt,
            {"mime_type": "image/jpeg", "data": img_bytes.read()}
        ])

        await status_msg.delete()
        await send_long_message(callback.message, response.text)

    except Exception as e:
        logging.error(f"Image analysis error: {e}")
        await status_msg.edit_text(f"⚠️ خطایی در تحلیل چارت رخ داد:\n`{e}`", parse_mode="Markdown")

@dp.message(F.voice)
async def handle_voice(message: Message):
    subscribers.add(message.from_user.id)
    status_msg = await message.answer("🎙 در حال تبدیل فایل صوتی و تحلیل استراتژی...")

    try:
        file_info = await bot.get_file(message.voice.file_id)
        voice_bytes = await bot.download_file(file_info.file_path)

        audio = AudioSegment.from_file(io.BytesIO(voice_bytes.read()), format="ogg")
        out_io = io.BytesIO()
        audio.export(out_io, format="mp3")
        out_io.seek(0)

        response = gemini_model.generate_content([
            "این یک پیام صوتی درباره استراتژی یا سوال کریپتو است. متن را متوجه شده و پاسخ معامله‌گری ارائه دهید.",
            {"mime_type": "audio/mp3", "data": out_io.read()}
        ])

        await status_msg.delete()
        await send_long_message(message, response.text)

    except Exception as e:
        logging.error(f"Voice analysis error: {e}")
        await status_msg.edit_text(f"⚠️ خطایی در تحلیل ویس رخ داد:\n`{e}`", parse_mode="Markdown")

# ---------------------------------------------------------
# اجرای اصلی برنامه (Main)
# ---------------------------------------------------------
async def main():
    logging.info("🚀 Starting AlphaEngine Pro Suite & Pump Scanner...")

    # ۱. راه اندازی وب‌سرور داخلی جهت رفع خطای پورت Render
    asyncio.create_task(start_web_server())

    # ۲. فعال‌سازی اسکنر پس‌زمینه هشدارهای پامپ
    asyncio.create_task(background_pump_alert_task())

    # ۳. آغاز دریافت پیام‌ها از تلگرام
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
