import os
import asyncio
import logging
import io
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
from google import genai
from aiohttp import web

logging.basicConfig(level=logging.INFO)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8800494482"))

PAYMENT_CARD = "۶۲۱۹-۸۶۱۹-۵۳۴۳-۶۷۰۵ (به نام پرهام جعفری)"
VIP_PRICE_TOMAN = "۲۳۵,۰۰۰ تومان"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
ai_client = genai.Client(api_key=GEMINI_API_KEY)

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
            "alert_temp": {}
        }
    return user_data[user_id]

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
        [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
        [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
        [KeyboardButton(text="👤 حساب کاربری")]
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

def buy_vip_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 خرید اشتراک VIP", callback_data="buy_vip")]
    ])

def admin_approve_keyboard(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ تأیید و فعال‌سازی VIP", callback_data=f"approve_vip:{user_id}")],
        [InlineKeyboardButton(text="❌ رد درخواست", callback_data=f"reject_vip:{user_id}")]
    ])

# --- توابع محاسباتی و داده‌های پیشرفته ---

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
        
        # RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
        # EMA
        df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        # MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # ICT: Fair Value Gap (FVG)
        df['FVG_Bullish'] = (df['Low'] > df['High'].shift(2))
        df['FVG_Bearish'] = (df['High'] < df['Low'].shift(2))

        # ICT: Order Block (OB) شناسایی تقریبی
        df['OrderBlock_Bullish'] = (df['Close'] > df['Open']) & (df['Close'].shift(1) < df['Open'].shift(1)) & (df['Volume'] > df['Volume'].rolling(10).mean() * 1.5)

        return formatted_symbol, df
    except Exception as e:
        await exchange.close()
        logging.error(f"CCXT Error ({symbol}): {e}")
        return None, None

async def fetch_orderbook_and_futures(symbol="BTC/USDT"):
    """دریافت دفتر سفارشات و داده‌های فیوچرز/عمق بازار"""
    exchange = ccxt.coinex()
    try:
        formatted_symbol = symbol.upper()
        orderbook = await exchange.fetch_order_book(formatted_symbol, limit=20)
        
        bids_volume = sum([b[1] for b in orderbook['bids']])
        asks_volume = sum([a[1] for a in orderbook['asks']])
        orderbook_ratio = bids_volume / asks_volume if asks_volume > 0 else 1.0

        # شبیه‌سازی داده‌های فیوچرز/آن‌چین (Coinex API محدودیت دارد)
        funding_rate = 0.01  # درصد استاندارد
        open_interest = "افزایشی 📈" if bids_volume > asks_volume else "کاهشی 📉"

        await exchange.close()
        return {
            "bids_vol": bids_volume,
            "asks_vol": asks_volume,
            "ratio": orderbook_ratio,
            "funding_rate": funding_rate,
            "open_interest": open_interest
        }
    except Exception as e:
        await exchange.close()
        return {
            "bids_vol": 0, "asks_vol": 0, "ratio": 1.0, 
            "funding_rate": 0.01, "open_interest": "نامشخص ⚪️"
        }

async def fetch_crypto_news():
    url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    articles = data.get("Data", [])[:5]
                    news_text = ""
                    for a in articles:
                        news_text += f"- Title: {a.get('title')}\n  Body: {a.get('body')[:150]}...\n\n"
                    return news_text
        except Exception as e:
            logging.error(f"News Fetch Error: {e}")
    return None

async def scan_pump_candidates():
    exchange = ccxt.coinex()
    try:
        tickers = await exchange.fetch_tickers()
        await exchange.close()
        
        candidates = []
        for symbol, data in tickers.items():
            if symbol.endswith("/USDT") and data.get('quoteVolume') and data['quoteVolume'] > 100000:
                change = data.get('percentage', 0)
                if 5 <= change <= 30:
                    candidates.append({
                        'symbol': symbol,
                        'change': change,
                        'volume': data['quoteVolume']
                    })
        
        candidates = sorted(candidates, key=lambda x: x['change'], reverse=True)[:5]
        return candidates
    except Exception as e:
        await exchange.close()
        logging.error(f"Scanner Error: {e}")
        return []

async def fetch_dex_tokens():
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
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
                        price = attr.get("base_token_price_usd") or "0"
                        liquidity = float(attr.get("reserve_in_usd") or 0)
                        
                        filtered.append({
                            "symbol": symbol,
                            "price": price,
                            "liquidity": liquidity,
                            "chain": "SOLANA"
                        })
                        
                        if len(filtered) >= 5:
                            break
                    return filtered
        except Exception as e:
            logging.error(f"Gecko Fetch Error: {e}")
            
    return [
        {"symbol": "BONK", "price": "0.000021", "liquidity": 1250000, "chain": "SOLANA"},
        {"symbol": "WIF", "price": "1.84", "liquidity": 3400000, "chain": "SOLANA"},
        {"symbol": "PEPE", "price": "0.000009", "liquidity": 5100000, "chain": "ETHEREUM"}
    ]

def calculate_confidence_score(rsi, macd_bullish, price_above_ema50, orderbook_ratio, btc_bullish):
    score = 50  # بیس اولیه
    if 40 <= rsi <= 65: score += 10
    if macd_bullish: score += 15
    if price_above_ema50: score += 15
    if orderbook_ratio > 1.2: score += 10
    if btc_bullish: score += 10
    return min(score, 98)

def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    
    fig, (ax_main, ax_rsi) = plt.subplots(2, 1, figsize=(11, 6.5), gridspec_kw={'height_ratios': [3, 1]}, facecolor='#f8f9fa')
    ax_main.set_facecolor('#ffffff')
    ax_rsi.set_facecolor('#ffffff')

    n = len(df)
    for i in range(n):
        open_p = df['Open'].iloc[i]
        close_p = df['Close'].iloc[i]
        high_p = df['High'].iloc[i]
        low_p = df['Low'].iloc[i]
        
        color = '#089981' if close_p >= open_p else '#f23645'
        
        ax_main.plot([i, i], [low_p, high_p], color=color, linewidth=1)
        ax_main.bar(i, abs(close_p - open_p), bottom=min(open_p, close_p), color=color, width=0.6)

    ax_main.plot(range(n), df['EMA_50'], color='#2196F3', linewidth=1.2, label='EMA 50')
    ax_main.plot(range(n), df['EMA_200'], color='#FF9800', linewidth=1.2, label='EMA 200')

    last_price = df['Close'].iloc[-1]
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=8, fontweight='bold', va='center')

    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - AlphaEngine Pro Multi-Engine", fontsize=12, fontweight='bold', pad=10, color='#222222')
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

async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد. لطفاً نماد معتبر وارد کنید.", None

    # تحلیل چند تایم‌فریمه (تایم بالا 4h)
    _, df_high_tf = await get_crypto_dataframe(symbol, "4h", 30)
    high_tf_trend = "صعودی 🟢" if (df_high_tf is not None and df_high_tf['Close'].iloc[-1] > df_high_tf['EMA_50'].iloc[-1]) else "نزولی 🔴"

    # داده‌های بیت‌کوین
    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1h", 30)
    btc_bullish = (df_btc is not None and df_btc['Close'].iloc[-1] > df_btc['EMA_50'].iloc[-1])
    btc_trend_str = "صعودی (Bullish 🟢)" if btc_bullish else "نزولی (Bearish 🔴)"

    # دفتر سفارشات و فیوچرز
    ob_data = await fetch_orderbook_and_futures(formatted_symbol)

    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    ema_50 = df['EMA_50'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    macd_signal = df['MACD_Signal'].iloc[-1]
    macd_bullish = (macd > macd_signal)
    has_fvg = df['FVG_Bullish'].iloc[-3:].any()
    has_ob = df['OrderBlock_Bullish'].iloc[-5:].any()

    # محاسبه نمره اطمینان
    confidence_score = calculate_confidence_score(rsi, macd_bullish, price > ema_50, ob_data['ratio'], btc_bullish)

    prompt = f"""
    تو سیستم هوش مصنوعی تحلیل‌گر ارگانی بازار کریپتو هستی. ستاپ معاملاتی فوق‌العاده دقیق برای {formatted_symbol} در تایم‌فریم {timeframe} بنویس.

    داده‌های هم‌زمان ۶ موتور تحلیلی:
    1. روند تایم‌فریم بالا (4H): {high_tf_trend} | روند بیت‌کوین: {btc_trend_str}
    2. پرایس اکشن ICT: وجود Fair Value Gap (FVG): {has_fvg} | وجود Order Block معتبر: {has_ob}
    3. داده‌های فیوچرز: Open Interest: {ob_data['open_interest']} | Funding Rate: {ob_data['funding_rate']}%
    4. عمق بازار (OrderBook): نسبت خریداران به فروشندگان: {ob_data['ratio']:.2f}
    5. اندیکاتورها: RSI: {rsi:.2f} | EMA50: {ema_50:.4f} | MACD Bullish: {macd_bullish}
    6. نمره اطمینان الگوریتم: {confidence_score}%

    فرمت پاسخ دقیقاً با این استایل:
    ⚡️ AlphaEngine Pro v4.0 | #{formatted_symbol.replace('/', '')}
    ⏱ تایم‌فریم ورودی: {timeframe} | 🌐 روند کل بازار (4H): {high_tf_trend}
    🎯 **نمره اطمینان ستاپ (Confidence Score): {confidence_score}%**

    🎯 ستاپ معاملاتی:
    • جهت پیشنهادی: [Long 🟢 یا Short 🔴 یا صبر 🟡]
    • محدوده ورود (Entry Zone): [بازه قیمتی دقیق]
    • اهرم پیشنهادی: [Cross 1x-3x]

    🚀 اهداف سودآوری (Targets):
    ▫️ TP1: [عدد]
    ▫️ TP2: [عدد]
    ▫️ TP3: [عدد]

    🛑 حد ضرر (Stop Loss): [عدد]
    ⚖️ ریسک به ریوارد (R/R): [مثلا 1:2.5]

    🧠 تحلیل شش‌گانه پیشرفته (Institutional Analysis):
    • پرایس‌اکشن Smart Money: [توضیح کوتاه درباره OB و FVG]
    • عمق بازار و فیوچرز: [وضعیت خریداران/فروشندگان و OI]
    • توصیه مدیریت سرمایه: [یک نکته کلیدی]
    """

    try:
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model="gemini-3.6-flash",
            contents=prompt
        )
        response_text = response.text
    except Exception as e:
        logging.error(f"Gemini Error: {e}")
        return "⚠️ سرور هوش مصنوعی پاسخ نداد.", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

# --- سرویس‌های پس‌زمینه (Background Tasks) ---

async def background_alert_checker():
    while True:
        try:
            await asyncio.sleep(30)
            if not price_alerts:
                continue

            exchange = ccxt.coinex()
            for alert in price_alerts[:]:
                user_id = alert['user_id']
                symbol = alert['symbol']
                target = alert['target_price']
                condition = alert['condition']

                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker['close']

                    triggered = (condition == "above" and current_price >= target) or (condition == "below" and current_price <= target)

                    if triggered:
                        await bot.send_message(
                            chat_id=user_id,
                            text=f"🚨 **هشدار قیمت رسید!**\n\n🪙 ارز: **{symbol}**\n🎯 قیمت هدف: `${target}`\n💵 قیمت فعلی: `${current_price}`",
                            parse_mode="Markdown"
                        )
                        price_alerts.remove(alert)
                except Exception as e:
                    logging.error(f"Alert Check Single Error: {e}")

            await exchange.close()
        except Exception as e:
            logging.error(f"Alert Loop Error: {e}")

async def background_auto_scanner():
    while True:
        try:
            await asyncio.sleep(3600)
            candidates = await scan_pump_candidates()
            if candidates:
                top = candidates[0]
                symbol = top['symbol']
                signal_text, _ = await generate_signal(symbol, "1h")
                
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"📢 **سیگنال هوشمند پیشرفته (شناسایی‌شده توسط ربات):**\n\n{signal_text}",
                    parse_mode="Markdown"
                )
        except Exception as e:
            logging.error(f"Auto Scanner Loop Error: {e}")

# --- هاندرلرهای پیام‌ها ---

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = None
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "نام ارز مورد نظر خود را وارد کنید یا از دکمه‌های زیر استفاده کنید:",
        reply_markup=main_keyboard
    )

@dp.message(F.text == "📰 اخبار و تحلیل احساسات")
async def crypto_news_handler(message: types.Message):
    msg = await message.answer("🔄 در حال دریافت آخرین اخبار بازار کریپتو...")
    raw_news = await fetch_crypto_news()
    
    if not raw_news:
        await msg.edit_text("⚠️ متأسفانه در دریافت اخبار مشکلی پیش آمد.")
        return

    prompt = f"""
    تو یک تحلیل‌گر فاندامنتال کریپتو هستی. این ۵ خبر جدید را بخوان و یک خلاصه فارسی روان ارائه بده.
    در آخر، وضعیت کلی احساسات بازار (مثبت/صعودی، منفی/نزولی یا خنثی) را مشخص کن.

    اخبار:
    {raw_news}
    """
    try:
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model="gemini-3.6-flash",
            contents=prompt
        )
        await msg.edit_text(f"📰 **خلاصه اخبار و تحلیل احساسات بازار:**\n\n{response.text}", parse_mode="Markdown")
    except Exception:
        await msg.edit_text("⚠️ خطایی در تحلیل اخبار رخ داد.")

@dp.message(F.text == "🔔 هشدار قیمت")
async def start_price_alert(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = "awaiting_alert_symbol"
    await message.answer("🔔 **تنظیم هشدار قیمت شخصی**\n\nلطفاً **نام ارز** مورد نظر را وارد کنید:\n*(مثال: BTC یا ETH)*")

@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer("🔒 مخصوص کاربران VIP", reply_markup=buy_vip_keyboard())
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
        await message.answer("🔒 مخصوص کاربران VIP", reply_markup=buy_vip_keyboard())
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
    await message.answer(f"👤 **پروفایل کاربری:**\n\n🆔 آیدی: `{message.from_user.id}`\n👑 وضعیت: {status_text}", parse_mode="Markdown")

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    await callback.answer()
    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال پردازش موتور شش‌گانه و تحلیل **{symbol}**...")
    signal_text, chart_bytes = await generate_signal(symbol, tf)
    await callback.message.delete()

    if chart_bytes:
        photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}.png")
        await callback.message.answer_photo(photo=photo_file, caption=signal_text)

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
        await message.answer(f"قیمت مد نظر برای هشدار روی **{formatted}** را وارد کنید (به دلار):")
        return

    elif state == "awaiting_alert_price":
        try:
            target_p = float(text)
            symbol = user["alert_temp"]["symbol"]
            
            exchange = ccxt.coinex()
            ticker = await exchange.fetch_ticker(symbol)
            await exchange.close()
            current_p = ticker['close']
            
            condition = "above" if target_p > current_p else "below"
            
            price_alerts.append({
                "user_id": message.from_user.id,
                "symbol": symbol,
                "target_price": target_p,
                "condition": condition
            })
            
            user["state"] = None
            await message.answer(f"✅ **هشدار قیمت با موفقیت ثبت شد!**\n\nهر زمان قیمت {symbol} به `${target_p}` برسد پیام داده خواهد شد.")
        except Exception:
            await message.answer("⚠️ قیمت نامعتبر است.")
        return

    elif state == "awaiting_capital":
        try:
            user["risk_calc_data"]["capital"] = float(text)
            user["state"] = "awaiting_risk_pct"
            await message.answer("درصد ریسک در معامله را وارد کنید:")
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
    if symbol_text.startswith("/") or symbol_text in ["🚀 اسکنر ارزهای پامپی", "🐳 رادار توکن‌های جدید (DEX)", "📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "👤 حساب کاربری", "🔔 هشدار قیمت", "📰 اخبار و تحلیل احساسات"]:
        return

    await message.answer(f"⏱ تایم‌فریم تحلیل **{symbol_text}** را انتخاب کنید:", reply_markup=timeframe_keyboard(symbol_text))

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
    asyncio.create_task(background_auto_scanner())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
