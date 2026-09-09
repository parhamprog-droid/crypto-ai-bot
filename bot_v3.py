import os
import asyncio
import logging
import io
import ccxt.async_support as ccxt
import aiohttp
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server
import matplotlib.pyplot as plt
import mplfinance as mpf

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

# Config & Envs
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = os.getenv("ADMIN_ID")

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

# Fetch Candle Data from KuCoin
async def get_crypto_dataframe(symbol="ETH/USDT", timeframe="1h", limit=80):
    exchange = ccxt.kucoin()
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
        df.set_index('timestamp', inplace=True)
        
        # Calculate RSI
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

# Generate Professional Custom Chart Image (Exact match to target image)
def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    
    # Custom light style
    mc = mpf.make_marketcolors(
        up='#089981', down='#f23645',
        edge='inherit',
        wick='inherit',
        volume='in'
    )
    style = mpf.make_mpf_style(
        marketcolors=mc,
        gridcolor='#e0e0e0',
        gridstyle='--',
        y_on_right=True,
        figcolor='#f8f9fa',
        facecolor='#ffffff'
    )

    # Plot setup with 2 panels (Main + RSI)
    fig, axes = mpf.plot(
        df,
        type='candle',
        style=style,
        volume=False,
        panel_ratios=(3, 1),
        figsize=(12, 7),
        returnfig=True
    )
    
    ax_main = axes[0]
    ax_rsi = axes[2] if len(axes) > 2 else axes[1]

    # Calculate Key Levels for Drawing
    closes = df['Close'].values
    highs = df['High'].values
    lows = df['Low'].values
    n = len(df)
    
    recent_high = max(highs[-30:])
    recent_low = min(lows[-30:])
    last_price = closes[-1]

    # Draw Channel / Trend lines
    ax_main.plot([0, n-1], [highs[0], recent_high], color='#8b0000', linestyle='-', linewidth=1.2, alpha=0.7)
    ax_main.plot([0, n-1], [lows[0], recent_low], color='#006400', linestyle='-', linewidth=1.2, alpha=0.7)
    
    # Draw Support / Resistance Hatch Area (Support Zone)
    support_box_bottom = recent_low * 0.995
    ax_main.axhspan(support_box_bottom, recent_low, facecolor='#ffcccc', edgecolor='red', hatch='//', alpha=0.5)
    
    # Current Price Line
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=9, fontweight='bold', va='center')

    # Watermark Header
    ax_main.set_title(f"{clean_symbol} {timeframe} - AlphaEngine Pro : @AlphaEngineBot", fontsize=13, fontweight='bold', pad=12, color='#222222')

    # Plot RSI Indicator
    ax_rsi.plot(df.index, df['RSI'], color='#8a2be2', linewidth=1.5)
    ax_rsi.axhline(70, color='gray', linestyle='--', linewidth=1)
    ax_rsi.axhline(30, color='gray', linestyle='--', linewidth=1)
    ax_rsi.fill_between(df.index, 30, 70, color='#e6e6fa', alpha=0.5)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.set_title("RSI @AlphaEngineBot", fontsize=11, fontweight='bold', pad=6, color='#333333')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()

# AI Signal Generation with Retry Logic
async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد.", None

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

# Handlers
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
    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال تولید چارت تحلیلی و دریافت سیگنال برای **{symbol}**...")
    
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
