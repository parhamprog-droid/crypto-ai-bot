import logging
import ccxt
import pandas as pd
import pandas_ta as ta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)
from google import genai
from google.genai import types

# ==========================================
# تنظیمات کلیدها و تنظیمات مدل جمینای
# ==========================================
TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"

# بهترین و دقیق‌ترین مدل جمینای برای تحلیل عمیق و استدلال
GEMINI_MODEL = "gemini-1.5-pro"

# راه اندازی کلاینت جمینای با کتابخانه جدید google-genai
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# تنظیمات لاگینگ
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ==========================================
# دریافت و پردازش داده‌های بازار (CCXT & TA)
# ==========================================
def get_binance_data(symbol: str = 'BTC/USDT', timeframe: str = '1h', limit: int = 100):
    try:
        exchange = ccxt.binance({'enableRateLimit': True})
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        logging.error(f"خطا در دریافت داده‌ها از بایننس: {e}")
        return None

def calculate_indicators(df: pd.DataFrame):
    if df is None or df.empty:
        return None
    
    # اندیکاتور RSI
    df['RSI'] = ta.rsi(df['close'], length=14)
    
    # اندیکاتور MACD
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    if macd is not None:
        df['MACD'] = macd['MACD_12_26_9']
        df['MACD_SIGNAL'] = macd['MACDs_12_26_9']
        df['MACD_HIST'] = macd['MACDh_12_26_9']
        
    # باند بولینگر
    bb = ta.bbands(df['close'], length=20, std=2)
    if bb is not None:
        df['BBL'] = bb['BBL_20_2.0']
        df['BBM'] = bb['BBM_20_2.0']
        df['BBU'] = bb['BBU_20_2.0']

    # میانگین‌های متحرک EMA
    df['EMA_20'] = ta.ema(df['close'], length=20)
    df['EMA_50'] = ta.ema(df['close'], length=50)
    df['EMA_200'] = ta.ema(df['close'], length=200)

    # اندیکاتور ATR برای حد ضرر
    df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    return df

# ==========================================
# تحلیل عمیق با قدرتمندترین مدل Gemini Pro
# ==========================================
def generate_signal(symbol: str = 'BTC/USDT', timeframe: str = '1h'):
    df = get_binance_data(symbol, timeframe=timeframe)
    if df is None:
        return "⚠️ خطا در دریافت اطلاعات از صرافی بایننس."

    df = calculate_indicators(df)
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # ساخت پرامپت حرفه‌ای و ساختاریافته برای Gemini Pro
    prompt = f"""
    شما یک تریدر ارشد و تحلیل‌گر حرفه‌ای بازارهای مالی هستید.
    لطفاً بر اساس داده‌های تکنیکال زیر برای جفت ارز {symbol} در تایم‌فریم {timeframe}، یک تحلیل دقیق و سیگنال معاملاتی صادر کنید:

    📊 داده‌های قیمتی و اندیکاتورها:
    - قیمت فعلی: {latest['close']} USDT
    - RSI (14): {latest['RSI']:.2f}
    - MACD Line: {latest['MACD']:.4f} | Signal Line: {latest['MACD_SIGNAL']:.4f}
    - EMA 20: {latest['EMA_20']:.2f} | EMA 50: {latest['EMA_50']:.2f} | EMA 200: {latest['EMA_200']:.2f}
    - Bollinger Bands: بالا = {latest['BBU']:.2f} | پایین = {latest['BBL']:.2f}
    - میزان نوسانات (ATR): {latest['ATR']:.2f}

    🎯 الزامات خروجی:
    1. **جهت سیگنال**: (BUY یا SELL یا WAIT)
    2. **نقطه ورود پیشنهادی (Entry Range)**
    3. **حد ضرر (Stop Loss)** - ترجیحاً بر اساس ATR و سطوح حمایتی/مقاومتی
    4. **هدف‌های سود (Take Profit 1, TP 2, TP 3)**
    5. **ریسک به ریوارد (Risk/Reward Ratio)**
    6. **تحلیل و دلیل سناریو**: دلایل تکنیکال برای ورود یا عدم ورود را کاملاً روان و دقیق توضیح دهید.

    پاسخ را با فرمتی کاملاً شکیل، خوانا و با استفاده از ایموجی‌های مناسب به زبان فارسی ارسال کنید.
    """

    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.2, # دمای پایین برای پاسخ‌های منطقی‌تر و دقیق‌تر
            )
        )
        return response.text
    except Exception as e:
        logging.error(f"خطا در ارتباط با Gemini API: {e}")
        return "⚠️ خطا در پردازش تحلیل توسط هوش مصنوعی."

# ==========================================
# هندلرهای تلگرام (Telegram Handlers)
# ==========================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("📈 BTC/USDT (15M)", callback_data='BTC/USDT_15m'),
            InlineKeyboardButton("📈 BTC/USDT (1H)", callback_data='BTC/USDT_1h'),
            InlineKeyboardButton("📈 BTC/USDT (4H)", callback_data='BTC/USDT_4h')
        ],
        [
            InlineKeyboardButton("💎 ETH/USDT (1H)", callback_data='ETH/USDT_1h'),
            InlineKeyboardButton("💎 ETH/USDT (4H)", callback_data='ETH/USDT_4h')
        ],
        [
            InlineKeyboardButton("🚀 SOL/USDT (1H)", callback_data='SOL/USDT_1h')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_msg = (
        "👋 **به ربات دستیار معاملاتی پیشرفته خوش آمدید!**\n\n"
        f"🤖 **مدل فعلی هوش مصنوعی:** `{GEMINI_MODEL}` (دقت بالا)\n\n"
        "لطفاً نماد و تایم‌فریم مورد نظر خود را جهت تحلیل انتخاب کنید:"
    )
    await update.message.reply_text(welcome_msg, reply_markup=reply_markup, parse_mode='Markdown')

async def timeframe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data.split('_')
    symbol = data[0]
    timeframe = data[1]

    await query.edit_message_text(
        text=f"⏳ در حال استخراج داده‌ها و تحلیل {symbol} ({timeframe}) با مدل **{GEMINI_MODEL}**...\nلطفاً چند ثانیه شکیبا باشید."
    )

    analysis_result = generate_signal(symbol, timeframe)
    
    # منوی مجدد برای درخواست تحلیل‌های دیگر
    keyboard = [
        [
            InlineKeyboardButton("🔄 تحلیل مجدد همین ارز", callback_data=query.data),
            InlineKeyboardButton("🏠 منوی اصلی", callback_data='main_menu')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(analysis_result, reply_markup=reply_markup)

async def crypto_news_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """بررسی و خلاصه‌سازی اخبار بازار کریپتو توسط جمینای"""
    await update.message.reply_text("🔎 در حال پردازش و تحلیل اخبار بازار...")
    
    prompt = "آخرین اخبار و روندهای کلیدی بازار کریپتوکارنسی را خلاصه کرده و ۳ نکته مهمی که تریدرها باید بدانند را بنویس."
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        await update.message.reply_text(response.text)
    except Exception as e:
        await update.message.reply_text("⚠️ خطا در دریافت اخبار.")

# ==========================================
# اجرای اصلی برنامه (Main)
# ==========================================
if __name__ == '__main__':
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # ثبت دستورات
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("news", crypto_news_handler))
    app.add_handler(CallbackQueryHandler(timeframe_callback))

    print("🤖 ربات تلگرام با قدرتمندترین مدل Gemini فعال شد...")
    app.run_polling()
