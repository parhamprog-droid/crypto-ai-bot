import os
import asyncio
import logging
import io
import datetime
import ccxt.async_support as ccxt
import aiohttp
import pandas as pd
import numpy as np
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
import google.generativeai as genai
from aiohttp import web

# تنظیمات لاگینگ
logging.basicConfig(level=logging.INFO)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8800494482"))

genai.configure(api_key=GEMINI_API_KEY)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

user_data = {}
price_alerts = []

def get_user(user_id: int):
    if user_id not in user_data:
        is_admin = (user_id == ADMIN_ID)
        user_data[user_id] = {
            "usage_count": 0,
            "is_vip": is_admin,
            "state": None,
            "risk_calc_data": {},
            "alert_temp": {},
            "referred_by": None,
            "referrals": []
        }
    return user_data[user_id]

# --- فراخوانی جمینای با پشتیبان هوشمند ---
async def query_gemini(prompt: str) -> str:
    preferred_models = [
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite",
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
        "gemini-pro"
    ]

    dynamic_models = []
    try:
        models_list = await asyncio.to_thread(genai.list_models)
        for m in models_list:
            if 'generateContent' in m.supported_generation_methods:
                clean_name = m.name.replace("models/", "")
                dynamic_models.append(clean_name)
    except Exception as e:
        logging.warning(f"Dynamic models fetch failed: {e}")

    candidate_models = list(dict.fromkeys(preferred_models + dynamic_models))

    last_error = None
    for model_name in candidate_models:
        try:
            model = genai.GenerativeModel(model_name)
            response = await asyncio.to_thread(model.generate_content, prompt)
            if response and response.text:
                return response.text
        except Exception as e:
            last_error = e
            continue

    raise last_error or Exception("هیچ‌کدام از مدل‌های جمینای پاسخ ندادند.")

# --- کیبوردهای تلگرام ---
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
        [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
        [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
        [KeyboardButton(text="👥 سیستم دعوت و هدیه"), KeyboardButton(text="👤 حساب کاربری")]
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

def alert_success_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 هشدارهای فعال من", callback_data="my_alerts"),
            InlineKeyboardButton(text="➕ ثبت هشدار جدید", callback_data="new_alert")
        ]
    ])

def buy_vip_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 دریافت VIP رایگان با دعوت دوستان", callback_data="buy_vip")]
    ])

# --- توابع دریافت و پردازش داده‌های بازار ---
async def get_crypto_dataframe(symbol="BTC/USDT", timeframe="1h", limit=100):
    exchange = ccxt.coinex()
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
        
        # اندیکاتورهای فنی
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
        df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # باندهای بولینگر و سنجش انقباض نوسان (Squeeze)
        df['BB_Middle'] = df['Close'].rolling(window=20).mean()
        df['BB_Std'] = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['BB_Middle'] + (df['BB_Std'] * 2)
        df['BB_Lower'] = df['BB_Middle'] - (df['BB_Std'] * 2)
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']

        # مفاهیم پرایس‌اکشن سبک ICT
        df['FVG_Bullish'] = (df['Low'] > df['High'].shift(2))
        df['OrderBlock_Bullish'] = (df['Close'] > df['Open']) & (df['Close'].shift(1) < df['Open'].shift(1)) & (df['Volume'] > df['Volume'].rolling(10).mean() * 1.5)

        return formatted_symbol, df
    except Exception as e:
        await exchange.close()
        logging.error(f"CCXT Error ({symbol}): {e}")
        return None, None

async def fetch_orderbook_and_futures(symbol="BTC/USDT"):
    exchange = ccxt.coinex()
    try:
        formatted_symbol = symbol.upper()
        orderbook = await exchange.fetch_order_book(formatted_symbol, limit=20)
        
        bids_volume = sum([b[1] for b in orderbook['bids']])
        asks_volume = sum([a[1] for a in orderbook['asks']])
        orderbook_ratio = bids_volume / asks_volume if asks_volume > 0 else 1.0

        await exchange.close()
        return {
            "bids_vol": bids_volume,
            "asks_vol": asks_volume,
            "ratio": orderbook_ratio,
            "open_interest": "افزایشی 📈" if bids_volume > asks_volume else "کاهشی 📉"
        }
    except Exception as e:
        await exchange.close()
        return {"bids_vol": 0, "asks_vol": 0, "ratio": 1.0, "open_interest": "نامشخص ⚪️"}

# --- دریافت اخبار با بای‌پاس SSL و چندین منبع پشتیبان ---
async def fetch_crypto_news():
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    connector = aiohttp.TCPConnector(ssl=False)
    
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        try:
            url1 = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
            async with session.get(url1, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    articles = data.get("Data", [])[:5]
                    if articles:
                        return "".join([f"- Title: {a.get('title')}\n  Body: {a.get('body')[:150]}...\n\n" for a in articles])
        except Exception as e:
            logging.error(f"News Source 1 Error: {e}")

        try:
            url2 = "https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fwww.coindesk.com%2Farc%2Foutboundfeeds%2Frss%2F"
            async with session.get(url2, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    articles = data.get("items", [])[:5]
                    if articles:
                        return "".join([f"- Title: {a.get('title')}\n  Body: {a.get('description')[:150]}...\n\n" for a in articles])
        except Exception as e:
            logging.error(f"News Source 2 Error: {e}")

    return "- Title: Crypto Market Volatility\n  Body: High market volatility continues across major crypto assets.\n\n"

async def scan_pump_candidates():
    exchange = ccxt.coinex()
    try:
        tickers = await exchange.fetch_tickers()
        await exchange.close()
        candidates = []
        for symbol, data in tickers.items():
            if symbol.endswith("/USDT"):
                volume = data.get('quoteVolume') or 0
                change = data.get('percentage') or 0
                if volume >= 30000 and change >= 2.0:
                    candidates.append({'symbol': symbol, 'change': float(change), 'volume': float(volume)})
        return sorted(candidates, key=lambda x: x['change'], reverse=True)[:5]
    except Exception:
        await exchange.close()
        return []

async def fetch_dex_tokens():
    headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    url = "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1"
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pools = data.get("data", [])
                    filtered = []
                    for pool in pools:
                        attr = pool.get("attributes", {})
                        raw_name = attr.get("name", "N/A")
                        symbol = raw_name.split("/")[0].strip() if "/" in raw_name else raw_name
                        raw_price = float(attr.get("base_token_price_usd") or 0)
                        formatted_price = f"{raw_price:.6f}".rstrip('0').rstrip('.') if raw_price > 0 else "0"
                        filtered.append({"symbol": symbol, "price": formatted_price})
                        if len(filtered) >= 5: break
                    return filtered
        except Exception as e:
            logging.error(f"Gecko Error: {e}")
    return [{"symbol": "BONK", "price": "0.000021"}, {"symbol": "WIF", "price": "1.84"}]

def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    fig, (ax_main, ax_rsi) = plt.subplots(2, 1, figsize=(11, 6.5), gridspec_kw={'height_ratios': [3, 1]}, facecolor='#f8f9fa')
    ax_main.set_facecolor('#ffffff')
    ax_rsi.set_facecolor('#ffffff')

    n = len(df)
    for i in range(n):
        open_p, close_p, high_p, low_p = df['Open'].iloc[i], df['Close'].iloc[i], df['High'].iloc[i], df['Low'].iloc[i]
        color = '#089981' if close_p >= open_p else '#f23645'
        ax_main.plot([i, i], [low_p, high_p], color=color, linewidth=1)
        ax_main.bar(i, abs(close_p - open_p), bottom=min(open_p, close_p), color=color, width=0.6)

    ax_main.plot(range(n), df['EMA_50'], color='#2196F3', linewidth=1.2, label='EMA 50')
    ax_main.plot(range(n), df['EMA_200'], color='#FF9800', linewidth=1.2, label='EMA 200')

    last_price = df['Close'].iloc[-1]
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=8, fontweight='bold', va='center')

    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - Multi-Timeframe AlphaEngine Pro", fontsize=12, fontweight='bold', pad=10)
    ax_main.legend(loc='upper left', fontsize=8)
    ax_main.yaxis.tick_right()

    ax_rsi.plot(range(n), df['RSI'], color='#8a2be2', linewidth=1.2)
    ax_rsi.axhline(70, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.axhline(30, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.fill_between(range(n), 30, 70, color='#e6e6fa', alpha=0.4)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_rsi.yaxis.tick_right()

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()

# --- موتور تحلیل فوق‌پیشرفته چند تایم‌فریمی ---
async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد.", None

    # دریافت داده‌های چند تایم‌فریمی
    _, df_1d = await get_crypto_dataframe(symbol, "1d", 30)
    _, df_4h = await get_crypto_dataframe(symbol, "4h", 30)
    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1h", 30)

    trend_1d = "صعودی 🟢" if (df_1d is not None and df_1d['Close'].iloc[-1] > df_1d['EMA_50'].iloc[-1]) else "نزولی 🔴"
    trend_4h = "صعودی 🟢" if (df_4h is not None and df_4h['Close'].iloc[-1] > df_4h['EMA_50'].iloc[-1]) else "نزولی 🔴"
    btc_bullish = (df_btc is not None and df_btc['Close'].iloc[-1] > df_btc['EMA_50'].iloc[-1])

    df['TR'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))))
    df['ATR'] = df['TR'].rolling(window=14).mean()
    atr_val = round(float(df['ATR'].iloc[-1]), 4)

    is_squeeze = df['BB_Width'].iloc[-1] < df['BB_Width'].rolling(30).mean().iloc[-1] * 0.7
    squeeze_status = "⚠️ فشرده‌سازی نوسان (آماده‌باش انفجار قیمت 🔥)" if is_squeeze else "عادی 🟢"

    ob_data = await fetch_orderbook_and_futures(formatted_symbol)
    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    has_fvg = df['FVG_Bullish'].iloc[-3:].any()
    has_ob = df['OrderBlock_Bullish'].iloc[-5:].any()

    prompt = f"""
    تو مدیر ارشد ریسک یک هج‌فاند کریپتو هستی. یک ستاپ فوق‌پیشرفته موسسه‌ای برای {formatted_symbol} در تایم‌فریم {timeframe} صادر کن.

    همگرایی روندهای تایم‌فریم بالاتر (Multi-TF Confluence):
    - روند دیلی (1D): {trend_1d}
    - روند چهارساعته (4H): {trend_4h}
    - وضعیت کلان بیت‌کوین: {"صعودی 🟢" if btc_bullish else "نزولی 🔴"}
    
    داده‌های فنی و ICT:
    - قیمت فعلی: {price} | ATR (نویز بازار): {atr_val}
    - وضعیت نوسان (Bollinger Squeeze): {squeeze_status}
    - FVG خریداران: {has_fvg} | Order Block: {has_ob}
    - نسبت سفارشات خرید/فروش: {ob_data['ratio']:.2f} | RSI: {rsi:.2f}

    فرمت خروجی دقیقاً طبق ساختار زیر باشد:

    ⚡️ AlphaEngine Institutional Multi-TF Setup
    📊 نماد: #{formatted_symbol.replace('/', '')} | تایم‌فریم: {timeframe}
    🌐 همگرایی روندها: 1D: {trend_1d} | 4H: {trend_4h}
    🌀 وضعیت نوسان: {squeeze_status}

    🎯 ستاپ معاملاتی:
    • جهت معامله: [Long 🟢 / Short 🔴 / Wait 🟡]
    • محدوده دقیق ورود (Entry Zone): [بازه قیمت]
    • اهرم پیشنهادی: [Cross 1x-3x]

    🚀 اهداف سودآوری:
    ▫️ TP1: [عدد] 👈 (۵۰٪ خروج + فری‌ریسک)
    ▫️ TP2: [عدد] 👈 (۳۰٪ خروج)
    ▫️ TP3: [عدد] 👈 (پوزیشن نهایی)

    🛑 حد ضرر (ATR Based): [عدد]
    ❌ شرط ابطال ستاپ: [توضیح کوتاه]

    🏛 تحلیل پرایس‌اکشن و نقدینگی (SMC & Wyckoff):
    • تحلیل FVG و اوردربلاک: [۱ خط]
    • رادار تله نهنگ: [۱ خط]
    """

    try:
        response_text = await query_gemini(prompt)
    except Exception as e:
        return f"⚠️ خطا در تحلیل جمینای:\n`{e}`", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

# --- هاندلرهای تلگرام ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    user["state"] = None

    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
        if referrer_id != user_id and user["referred_by"] is None:
            referrer = get_user(referrer_id)
            if user_id not in referrer["referrals"]:
                user["referred_by"] = referrer_id
                referrer["referrals"].append(user_id)
                if len(referrer["referrals"]) >= 3 and not referrer["is_vip"]:
                    referrer["is_vip"] = True
                    try:
                        await bot.send_message(referrer_id, "🎉 **تبریک!** ۳ کاربر جدید دعوت کردید و حساب شما **VIP** شد!")
                    except Exception:
                        pass

    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    await message.answer(
        f"👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        f"🎁 **سیستم دعوت و VIP رایگان:**\n"
        f"با دعوت ۳ نفر به ربات، اشتراک **VIP رایگان** دریافت کنید!\n\n"
        f"🔗 لینک دعوت اختصاصی شما:\n`{ref_link}`\n"
        f"👥 تعداد دعوت‌های شما: **{len(user['referrals'])} نفر**\n\n"
        f"نام ارز مورد نظر خود را ارسال کنید یا از منوی زیر استفاده کنید:",
        reply_markup=main_keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.text == "👥 سیستم دعوت و هدیه")
async def referral_info_handler(message: types.Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    await message.answer(
        f"🎁 **برنامه دعوت دوستان**\n\n"
        f"لینک اختصاصی شما:\n`{ref_link}`\n\n"
        f"📊 وضعیت شما:\n"
        f"• تعداد دعوت‌شده‌ها: **{len(user['referrals'])} از ۳ نفر**\n"
        f"• وضعیت VIP: {'🟢 فعال' if user['is_vip'] else '🔴 غیرفعال (با ۳ دعوت فعال می‌شود)'}",
        parse_mode="Markdown"
    )

@dp.message(F.text == "📰 اخبار و تحلیل احساسات")
async def crypto_news_handler(message: types.Message):
    msg = await message.answer("🔄 در حال دریافت آخرین اخبار...")
    raw_news = await fetch_crypto_news()

    prompt = f"این اخبار کریپتو را تحلیلی و ساختاریافته خلاصه کن:\n{raw_news}"
    try:
        response_text = await query_gemini(prompt)
        await msg.edit_text(f"📰 **خلاصه اخبار و احساسات بازار:**\n\n{response_text}", parse_mode="Markdown")
    except Exception:
        await msg.edit_text(f"📰 خلاصه اخبار:\n\n{raw_news[:1000]}")

@dp.message(F.text == "🔔 هشدار قیمت")
async def start_price_alert(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = "awaiting_alert_symbol"
    await message.answer("🔔 **تنظیم هشدار قیمت**\n\nلطفاً **نام ارز** را وارد کنید (مثال: BTC یا ETH):")

@dp.callback_query(F.data == "new_alert")
async def callback_new_alert(callback: types.CallbackQuery):
    await callback.answer()
    user = get_user(callback.from_user.id)
    user["state"] = "awaiting_alert_symbol"
    await callback.message.answer("🔔 نام ارز را وارد کنید:")

@dp.callback_query(F.data == "my_alerts")
async def callback_my_alerts(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    user_alerts = [a for a in price_alerts if a['user_id'] == user_id]
    
    if not user_alerts:
        await callback.message.answer("📋 هیچ هشدار فعالی ندارید.")
        return
        
    text = "📋 **هشدارهای فعال شما:**\n\n"
    for i, a in enumerate(user_alerts, 1):
        text += f"{i}. ارز: **{a['symbol']}** | قیمت هدف: `${a['target_price']}`\n"
    await callback.message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer("🔒 مخصوص کاربران VIP (برای فعال‌سازی ۳ نفر را دعوت کنید).", reply_markup=buy_vip_keyboard())
        return

    msg = await message.answer("🔍 در حال اسکن بازار...")
    candidates = await scan_pump_candidates()
    if not candidates:
        await msg.edit_text("⚠️ ارزی یافت نشد.")
        return
    
    text = "🔥 **ارزهای مستعد پامپ:**\n\n"
    for c in candidates:
        text += f"📌 **#{c['symbol'].replace('/', '')}** | رشد: `+{c['change']:.2f}%`\n"
    await msg.edit_text(text, parse_mode="Markdown")

@dp.message(F.text == "🐳 رادار توکن‌های جدید (DEX)")
async def dex_radar_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer("🔒 مخصوص کاربران VIP (برای فعال‌سازی ۳ نفر را دعوت کنید).", reply_markup=buy_vip_keyboard())
        return

    msg = await message.answer("🔎 در حال رصد توکن‌های DEX...")
    tokens = await fetch_dex_tokens()
    text = "🐳 **توکن‌های ترند DEX:**\n\n"
    for t in tokens:
        text += f"🪙 **{t['symbol']}** | قیمت: `${t['price']}`\n"
    await msg.edit_text(text, parse_mode="Markdown")

@dp.message(F.text == "📊 شاخص ترس و طمع")
async def fear_and_greed(message: types.Message):
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.alternative.me/fng/") as resp:
            if resp.status == 200:
                data = await resp.json()
                item = data["data"][0]
                await message.answer(f"📊 **شاخص ترس و طمع:**\n\n🎯 عدد: **{item['value']}/100**\n📌 وضعیت: **{item['value_classification']}**")

@dp.message(F.text == "🧮 محاسبه ریسک")
async def start_risk_calc(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = "awaiting_capital"
    user["risk_calc_data"] = {}
    await message.answer("🧮 **محاسبه مدیریت ریسک**\n\nموجودی کل حساب (به دلار) را وارد کنید:")

@dp.message(F.text == "👤 حساب کاربری")
async def user_profile(message: types.Message):
    user = get_user(message.from_user.id)
    status_text = "💎 VIP" if user["is_vip"] else "👤 رایگان"
    await message.answer(f"👤 **پروفایل کاربری:**\n\n🆔 آیدی: `{message.from_user.id}`\n👑 وضعیت: {status_text}\n👥 تعداد دعوت‌ها: **{len(user['referrals'])}**", parse_mode="Markdown")

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    await callback.answer()
    _, symbol, tf = callback.data.split(":")
    
    loading_msg = await callback.message.answer(f"🔄 در حال پردازش موتور تحلیل **{symbol}**...")
    
    try:
        signal_text, chart_bytes = await generate_signal(symbol, tf)
        try:
            await loading_msg.delete()
        except Exception:
            pass

        if chart_bytes:
            photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}.png")
            try:
                await callback.message.answer_photo(photo=photo_file, caption=f"📊 **چارت {symbol} ({tf})**", parse_mode="Markdown")
            except Exception:
                await callback.message.answer_photo(photo=photo_file, caption=f"📊 چارت {symbol} ({tf})")

        if signal_text:
            try:
                await callback.message.answer(signal_text, parse_mode="Markdown")
            except Exception:
                await callback.message.answer(signal_text)
            
    except Exception as e:
        logging.error(f"Callback Error: {e}")
        await callback.message.answer(f"⚠️ خطایی در اجرای تحلیل رخ داد:\n{e}")

@dp.message(F.text)
async def handle_text_input(message: types.Message):
    text = message.text.strip()
    user = get_user(message.from_user.id)
    state = user.get("state")

    if state == "awaiting_alert_symbol":
        formatted = text.upper()
        if not formatted.endswith("/USDT"):
            formatted += "/USDT"
        user["alert_temp"]["symbol"] = formatted
        user["state"] = "awaiting_alert_price"
        await message.answer(f"قیمت مد نظر برای **{formatted}** (به دلار) را وارد کنید:")
        return

    elif state == "awaiting_alert_price":
        try:
            target_p = float(text)
            symbol = user["alert_temp"]["symbol"]
            
            exchange = ccxt.coinex()
            ticker = await exchange.fetch_ticker(symbol)
            await exchange.close()
            current_p = ticker['close']
            
            price_alerts.append({
                "user_id": message.from_user.id,
                "symbol": symbol,
                "target_price": target_p,
                "condition": "above" if target_p > current_p else "below"
            })
            
            user["state"] = None
            await message.answer(f"✅ **هشدار قیمت ثبت شد!**\nارز: {symbol} | هدف: `${target_p}`", reply_markup=alert_success_keyboard(), parse_mode="Markdown")
        except Exception:
            await message.answer("⚠️ قیمت نامعتبر است.")
        return

    elif state == "awaiting_capital":
        try:
            user["risk_calc_data"]["capital"] = float(text)
            user["state"] = "awaiting_risk_pct"
            await message.answer("درصد ریسک در معامله (مثلاً 1 یا 2) را وارد کنید:")
        except ValueError:
            await message.answer("لطفاً عدد وارد کنید.")
        return

    elif state == "awaiting_risk_pct":
        try:
            user["risk_calc_data"]["risk_pct"] = float(text)
            user["state"] = "awaiting_entry"
            await message.answer("قیمت ورود (Entry):")
        except ValueError:
            await message.answer("لطفاً عدد وارد کنید.")
        return

    elif state == "awaiting_entry":
        try:
            user["risk_calc_data"]["entry"] = float(text)
            user["state"] = "awaiting_sl"
            await message.answer("قیمت حد ضرر (Stop Loss):")
        except ValueError:
            await message.answer("لطفاً عدد وارد کنید.")
        return

    elif state == "awaiting_sl":
        try:
            sl = float(text)
            data = user["risk_calc_data"]
            capital, risk_pct, entry = data["capital"], data["risk_pct"], data["entry"]
            user["state"] = None

            risk_amount = capital * (risk_pct / 100)
            sl_distance_pct = abs(entry - sl) / entry
            position_size = risk_amount / sl_distance_pct

            result = (
                f"🧮 **نتیجه مدیریت ریسک:**\n\n"
                f"💵 کل سرمایه: `${capital:,.2f}`\n"
                f"🎯 میزان ریسک: `${risk_amount:,.2f}` ({risk_pct}%)\n"
                f"✅ **حجم پیشنهادی برای ورود:** `${position_size:,.2f}`"
            )
            await message.answer(result, parse_mode="Markdown")
        except ValueError:
            await message.answer("لطفاً عدد وارد کنید.")
        return

    symbol_text = text.upper()
    if symbol_text.startswith("/") or symbol_text in ["🚀 اسکنر ارزهای پامپی", "🐳 رادار توکن‌های جدید (DEX)", "📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "👤 حساب کاربری", "🔔 هشدار قیمت", "📰 اخبار و تحلیل احساسات", "👥 سیستم دعوت و هدیه"]:
        return

    await message.answer(f"⏱ تایم‌فریم تحلیل **{symbol_text}** را انتخاب کنید:", reply_markup=timeframe_keyboard(symbol_text))

# --- سرویس ارسال بولتن روزانه خودکار ---
async def generate_daily_digest():
    fng_val, fng_class = "N/A", "N/A"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.alternative.me/fng/") as resp:
                if resp.status == 200:
                    d = await resp.json()
                    fng_val = d["data"][0]["value"]
                    fng_class = d["data"][0]["value_classification"]
    except Exception:
        pass

    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1d", 30)
    btc_price = df_btc['Close'].iloc[-1] if df_btc is not None else 0
    btc_change = df_btc['Close'].pct_change().iloc[-1] * 100 if df_btc is not None else 0

    raw_news = await fetch_crypto_news()
    prompt = f"یک خلاصه بسیار کوتاه و جذاب (حداکثر ۳ سطر) از مهم‌ترین اخبار کریپتو ارائه بده:\n{raw_news}"
    try:
        news_summary = await query_gemini(prompt)
    except Exception:
        news_summary = "تغییرات شدید نوسانی در بازار مشاهده می‌شود."

    return (
        f"☀️ **بولتن تحلیلی روزانه AlphaEngine**\n\n"
        f"🪙 **بیت‌کوین (BTC):** `${btc_price:,.2f}` (`{btc_change:+.2f}%`)\n"
        f"📊 **شاخص ترس و طمع:** {fng_val}/100 ({fng_class})\n\n"
        f"📰 **خلاصه اخبار:**\n{news_summary}\n\n"
        f"💡 برای تحلیل کامل هر ارز، نام آن را در ربات بفرستید."
    )

async def daily_digest_scheduler():
    while True:
        await asyncio.sleep(60)
        now = datetime.datetime.now()
        # ارسال خودکار رأس ساعت ۰۸:۰۰ صبح
        if now.hour == 8 and now.minute == 0:
            digest_msg = await generate_daily_digest()
            for u_id in list(user_data.keys()):
                try:
                    await bot.send_message(u_id, digest_msg, parse_mode="Markdown")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logging.warning(f"Failed to send digest to {u_id}: {e}")
            await asyncio.sleep(300)

async def background_alert_checker():
    while True:
        try:
            await asyncio.sleep(30)
            if not price_alerts:
                continue

            exchange = ccxt.coinex()
            for alert in price_alerts[:]:
                symbol = alert['symbol']
                target = alert['target_price']
                condition = alert['condition']

                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker['close']

                    if (condition == "above" and current_price >= target) or (condition == "below" and current_price <= target):
                        await bot.send_message(
                            chat_id=alert['user_id'],
                            text=f"🚨 **هشدار قیمت رسید!**\n\n🪙 ارز: **{symbol}**\n🎯 قیمت هدف: `${target}`\n💵 قیمت فعلی: `${current_price}`",
                            parse_mode="Markdown"
                        )
                        price_alerts.remove(alert)
                except Exception as e:
                    logging.error(f"Alert Check Error: {e}")

            await exchange.close()
        except Exception as e:
            logging.error(f"Alert Loop Error: {e}")

async def handle_web(request):
    return web.Response(text="AlphaEngine Pro Active!")

app = web.Application()
app.router.add_get('/', handle_web)

async def main():
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    asyncio.create_task(background_alert_checker())
    asyncio.create_task(daily_digest_scheduler())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
