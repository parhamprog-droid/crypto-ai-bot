import os
import asyncio
import logging
import io
import ccxt.async_support as ccxt
import aiohttp
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from google import genai
from aiohttp import web

logging.basicConfig(level=logging.INFO)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
ai_client = genai.Client(api_key=GEMINI_API_KEY)

user_data = {}

def get_user(user_id: int):
    if user_id not in user_data:
        user_data[user_id] = {"usage_count": 0, "is_vip": False}
    return user_data[user_id]

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

# Fetch Candle Data from Binance (Extremely Reliable)
async def get_crypto_dataframe(symbol="ETH/USDT", timeframe="1h", limit=80):
    exchange = ccxt.binance()
    try:
        formatted_symbol = symbol.upper().strip()
        if not formatted_symbol.endswith("/USDT") and not formatted_symbol.endswith("USDT"):
            formatted_symbol = f"{formatted_symbol}/USDT"
        elif formatted_symbol.endswith("USDT") and "/" not in formatted_symbol:
            formatted_symbol = formatted_symbol.replace("USDT", "/USDT")

        ohlcv = await exchange.fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=limit)
        await exchange.close()
        
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
        return formatted_symbol, df
    except Exception as e:
        await exchange.close()
        logging.error(f"CCXT Error: {e}")
        return None, None

def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    
    fig, (ax_main, ax_rsi) = plt.subplots(2, 1, figsize=(11, 6.5), gridspec_kw={'height_ratios': [3, 1]}, facecolor='#f8f9fa')
    ax_main.set_facecolor('#ffffff')
    ax_rsi.set_facecolor('#ffffff')

    n = len(df)
    
    # Custom Candlestick rendering
    for i in range(n):
        open_p = df['Open'].iloc[i]
        close_p = df['Close'].iloc[i]
        high_p = df['High'].iloc[i]
        low_p = df['Low'].iloc[i]
        
        color = '#089981' if close_p >= open_p else '#f23645'
        
        ax_main.plot([i, i], [low_p, high_p], color=color, linewidth=1)
        ax_main.bar(i, abs(close_p - open_p), bottom=min(open_p, close_p), color=color, width=0.6)

    # Key Levels & Lines
    recent_high = df['High'].iloc[-30:].max()
    recent_low = df['Low'].iloc[-30:].min()
    last_price = df['Close'].iloc[-1]

    # Upper/Lower Channel Lines
    ax_main.plot([0, n-1], [df['High'].iloc[0], recent_high], color='#8b0000', linestyle='-', linewidth=1, alpha=0.7)
    ax_main.plot([0, n-1], [df['Low'].iloc[0], recent_low], color='#006400', linestyle='-', linewidth=1, alpha=0.7)
    
    # Support Box (Hatch Area)
    support_box_bottom = recent_low * 0.995
    ax_main.axhspan(support_box_bottom, recent_low, facecolor='#ffcccc', edgecolor='red', hatch='//', alpha=0.4)
    
    # Current Price Horizontal
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=8, fontweight='bold', va='center')

    # Grid & Titles
    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - Start using Turbo Trade Bot today : @tbsignalbot", fontsize=12, fontweight='bold', pad=10, color='#222222')
    ax_main.yaxis.tick_right()

    # RSI Plot
    ax_rsi.plot(range(n), df['RSI'], color='#8a2be2', linewidth=1.2)
    ax_rsi.axhline(70, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.axhline(30, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.fill_between(range(n), 30, 70, color='#e6e6fa', alpha=0.4)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_rsi.set_title("RSI @tbsignalbot", fontsize=10, fontweight='bold', pad=5, color='#333333')
    ax_rsi.yaxis.tick_right()

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()

async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد. لطفاً نماد معتبر وارد کنید.", None

    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    change_24h = ((price - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100

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

    response_text = None
    for attempt in range(3):
        try:
            response = await asyncio.to_thread(
                ai_client.models.generate_content,
                model="gemini-3.6-flash",
                contents=prompt
            )
            response_text = response.text
            break
        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                return "⚠️ سرور هوش مصنوعی در حال حاضر شلوغ است. لطفاً چند ثانیه دیگر مجدداً سعی کنید.", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    get_user(message.from_user.id)
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "برای دریافت تحلیل و چارت اختصاصی، نام ارز را بفرستید (مثلاً `BTC` یا `ETH`).",
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
    await callback.answer()  # پاسخ آنی به تلگرام جهت جلوگیری از اکسپایر شدن دکمه
    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال دریافت چارت و سیگنال **{symbol}**...")
    
    signal_text, chart_bytes = await generate_signal(symbol, tf)
    await callback.message.delete()

    if chart_bytes:
        photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
        await callback.message.answer_photo(photo=photo_file, caption=signal_text)
    else:
        await callback.message.answer(signal_text)

@dp.message(F.text)
async def handle_symbol_input(message: types.Message):
    symbol_text = message.text.strip().upper()
    if symbol_text.startswith("/") or symbol_text in ["📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "📸 آنالیز عکس چارت (VIP)", "👤 حساب کاربری"]:
        return

    await message.answer(
        f"⏱ لطفاً تایم‌فریم تحلیل **{symbol_text}** را انتخاب کنید:",
        reply_markup=timeframe_keyboard(symbol_text)
    )

async def handle_web(request):
    return web.Response(text="AlphaEngine Pro Active!")

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
