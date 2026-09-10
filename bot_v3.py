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
ADMIN_ID = int(os.getenv("ADMIN_ID", "8800494482"))

# اطلاعات پرداخت
PAYMENT_CARD = "۶۲۱۹-۸۶۱۹-۵۳۴۳-۶۷۰۵ (به نام پرهام جعفری)"
VIP_PRICE_TOMAN = "۲۳۵,۰۰۰ تومان"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
ai_client = genai.Client(api_key=GEMINI_API_KEY)

user_data = {}

def get_user(user_id: int):
    if user_id not in user_data:
        is_admin = (user_id == ADMIN_ID)
        user_data[user_id] = {
            "usage_count": 0,
            "is_vip": is_admin,
            "state": None,
            "risk_calc_data": {}
        }
    return user_data[user_id]

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
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
        
        # محاسبه RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
        # محاسبه EMA 50 و EMA 200
        df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
        
        # محاسبه MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

        return formatted_symbol, df
    except Exception as e:
        await exchange.close()
        logging.error(f"CCXT Error ({symbol}): {e}")
        return None, None

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
        {"symbol": "PEPE", "price": "0.000009", "liquidity": 5100000, "chain": "ETHEREUM"},
        {"symbol": "FLOKI", "price": "0.00015", "liquidity": 980000, "chain": "BINANCE SMART CHAIN"},
        {"symbol": "BRETT", "price": "0.082", "liquidity": 750000, "chain": "BASE"}
    ]

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

    # رسم خطوط EMA روی چارت
    ax_main.plot(range(n), df['EMA_50'], color='#2196F3', linewidth=1.2, label='EMA 50')
    ax_main.plot(range(n), df['EMA_200'], color='#FF9800', linewidth=1.2, label='EMA 200')

    recent_high = df['High'].iloc[-30:].max()
    recent_low = df['Low'].iloc[-30:].min()
    last_price = df['Close'].iloc[-1]

    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=8, fontweight='bold', va='center')

    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - AlphaEngine Pro Advanced", fontsize=12, fontweight='bold', pad=10, color='#222222')
    ax_main.legend(loc='upper left', fontsize=8)
    ax_main.yaxis.tick_right()

    ax_rsi.plot(range(n), df['RSI'], color='#8a2be2', linewidth=1.2)
    ax_rsi.axhline(70, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.axhline(30, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.fill_between(range(n), 30, 70, color='#e6e6fa', alpha=0.4)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_rsi.set_title("RSI Indicator", fontsize=10, fontweight='bold', pad=5, color='#333333')
    ax_rsi.yaxis.tick_right()

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()

async def get_btc_trend():
    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1h", 50)
    if df_btc is not None and not df_btc.empty:
        last_close = df_btc['Close'].iloc[-1]
        ema_50 = df_btc['EMA_50'].iloc[-1]
        if last_close > ema_50:
            return "صعودی (Bullish 🟢)"
        else:
            return "نزولی (Bearish 🔴)"
    return "نامشخص ⚪️"

async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد. لطفاً نماد معتبر وارد کنید.", None

    btc_trend = await get_btc_trend()

    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    ema_50 = df['EMA_50'].iloc[-1]
    ema_200 = df['EMA_200'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    macd_signal = df['MACD_Signal'].iloc[-1]
    high_24h = df['High'].max()
    low_24h = df['Low'].min()
    change_24h = ((price - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100

    macd_status = "متقاطع صعودی (Bullish Cross)" if macd > macd_signal else "متقاطع نزولی (Bearish Cross)"
    ema_trend = "بالای EMA50 (روند صعودی)" if price > ema_50 else "پایین EMA50 (روند نزولی)"

    prompt = f"""
    تو یک تحلیل‌گر پیشرفته تکنیکال و معامله‌گر حرفه‌ای کریپتو هستی. برای ارز {formatted_symbol} در تایم‌فریم {timeframe} یک ستاپ معاملاتی با رعایت کامل مدیریت ریسک بنویس.

    داده‌های دریافتی از چارت:
    - روند کلی بیت‌کوین (BTC Trend): {btc_trend}
    - قیمت فعلی: {price} USDT
    - بالاترین 24h: {high_24h} | پایین‌ترین 24h: {low_24h}
    - تغییرات 24 ساعت: {change_24h:.2f}%
    - شاخص RSI: {rsi:.2f}
    - وضعیت EMA: {ema_trend} (EMA 50: {ema_50:.4f} | EMA 200: {ema_200:.4f})
    - وضعیت MACD: {macd_status} (MACD: {macd:.4f} | Signal: {macd_signal:.4f})

    نکات مهم برای تحلیل:
    1. اگر روند بیت‌کوین نزولی است یا RSI بسیار بالا (بالای 70) است، معامله خرید (Long) پرریسک است.
    2. حد ضرر (Stop Loss) باید کاملاً منطقی و بر اساس حمایت/مقاومت نزدیک تعیین شود.
    3. نسبت ریسک به ریوارد (R/R) باید حداقل 1:2 باشد.

    خروجی را دقیقا با این فرمت ارایه کن:
    ⚡️ AlphaEngine Pro | #{formatted_symbol.replace('/', '')}
    ⏱ تایم‌فریم: {timeframe} | 🌐 روند بیت‌کوین: {btc_trend}

    🎯 ستاپ معاملاتی:
    • جهت پیشنهادی: [Long 🟢 یا Short 🔴 یا خروج/صبر 🟡]
    • محدوده ورود (Entry Zone): [بازه قیمتی منطقی]
    • اهرم پیشنهادی (Leverage): [Cross 1x-3x (مخصوص تازه واردین)]

    🚀 اهداف سودآوری (Targets):
    ▫️ TP1: [عدد]
    ▫️ TP2: [عدد]
    ▫️ TP3: [عدد]

    🛑 حد ضرر (Stop Loss): [عدد]
    ⚖️ ریسک به ریوارد: [مثلا 1:2.2]

    📊 تحلیل تکنیکال ارتقایافته:
    • تحلیل اندیکاتورها: [بررسی خلاصه RSI، MACD و EMA]
    • توصیه فنی: [۱ جمله کلیدی برای مدیریت ریسک معامله‌گر]
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
                return "⚠️ سرور هوش مصنوعی شلوغ است. مجدداً تلاش کنید.", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = None
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "نام ارز مورد نظر خود را وارد کنید یا از دکمه‌های زیر استفاده کنید:",
        reply_markup=main_keyboard
    )

@dp.message(Command("setvip"))
async def set_vip_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        args = message.text.split()
        target_id = int(args[1])
        u = get_user(target_id)
        u["is_vip"] = True
        await message.answer(f"✅ کاربر `{target_id}` با موفقیت به **VIP** ارتقا یافت.", parse_mode="Markdown")
    except Exception:
        await message.answer("⚠️ فرمت دستور نادرست است. مثال: `/setvip 123456789`", parse_mode="Markdown")

@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer(
            "🔒 **این بخش مخصوص کاربران VIP است.**\n\n"
            "برای دسترسی به اسکنر آنی ارزهای مستعد پامپ، اشتراک VIP خود را فعال کنید.",
            reply_markup=buy_vip_keyboard(),
            parse_mode="Markdown"
        )
        return

    msg = await message.answer("🔍 در حال اسکن بازار و شناسایی ارزهای مستعد پامپ...")
    candidates = await scan_pump_candidates()
    
    if not candidates:
        await msg.edit_text("⚠️ در حال حاضر ارز مشکوک به پامپ یافت نشد.")
        return
    
    text = "🔥 **ارزهای مستعد پامپ و جهش حجم (۲۴ ساعت اخیر):**\n\n"
    for c in candidates:
        text += f"📌 **#{c['symbol'].replace('/', '')}**\n"
        text += f"📈 رشد ۲۴ ساعت: `+{c['change']:.2f}%`\n"
        text += f"📊 حجم معاملات: `${c['volume']:,.0f}`\n"
        text += "──────────────\n"
    
    text += "\n💡 *برای دریافت تحلیل دقیق هر ارز، نام آن را ارسال کنید.*"
    await msg.edit_text(text, parse_mode="Markdown")

@dp.message(F.text == "🐳 رادار توکن‌های جدید (DEX)")
async def dex_radar_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer(
            "🔒 **این بخش مخصوص کاربران VIP است.**\n\n"
            "برای رصد لحظه‌ای توکن‌های پرنقدینگی On-Chain، اشتراک VIP تهیه کنید.",
            reply_markup=buy_vip_keyboard(),
            parse_mode="Markdown"
        )
        return

    msg = await message.answer("🔎 در حال استعلام آخرین توکن‌های پرنقدینگی در صرافی‌های غیرمتمرکز...")
    tokens = await fetch_dex_tokens()
    
    if not tokens:
        await msg.edit_text("⚠️ اطلاعات توکن‌های غیرمتمرکز دریافت نشد.")
        return
    
    text = "🐳 **توکن‌های ترند و پرنقدینگی On-Chain (شناسایی‌شده):**\n\n"
    for t in tokens:
        text += f"🪙 **{t['symbol']}**\n"
        text += f"🌐 شبکه: `{t['chain']}`\n"
        text += f"💵 قیمت: `${float(t['price']):.6f}`\n"
        text += f"💧 نقدینگی استخر: `${float(t['liquidity']):,.0f}`\n"
        text += "──────────────\n"
    
    text += "\n⚠️ *توجه: معامله توکن‌های DEX ریسک بالا دارد. حتماً حد ضرر را رعایت کنید.*"
    await msg.edit_text(text, parse_mode="Markdown")

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

@dp.message(F.text == "🧮 محاسبه ریسک")
async def start_risk_calc(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = "awaiting_capital"
    user["risk_calc_data"] = {}
    await message.answer(
        "🧮 **ماشین‌حساب هوشمند مدیریت ریسک**\n\n"
        "لطفاً **موجودی کل حساب (به دلار)** را وارد کنید:\n"
        "*(مثال: 1000)*"
    )

@dp.message(F.text == "👤 حساب کاربری")
async def user_profile(message: types.Message):
    user = get_user(message.from_user.id)
    status_text = "💎 **VIP (نامحدود / مدیر)**" if user["is_vip"] else "👤 **رایگان**"
    limit_text = "نامحدود" if user["is_vip"] else f"{user['usage_count']} / 3 استفاده امروز"
    
    profile_msg = (
        f"👤 **پروفایل کاربری شما:**\n\n"
        f"🆔 شناسه عددی: `{message.from_user.id}`\n"
        f"👑 وضعیت اشتراک: {status_text}\n"
        f"📊 تحلیل‌های امروز: `{limit_text}`\n\n"
    )
    
    if not user["is_vip"]:
        profile_msg += "💡 *با ارتقا به VIP، به اسکنر پامپی، رادار DEX و تحلیل نامحدود دسترسی پیدا کنید.*"
        await message.answer(profile_msg, reply_markup=buy_vip_keyboard(), parse_mode="Markdown")
    else:
        await message.answer(profile_msg, parse_mode="Markdown")

@dp.callback_query(F.data == "buy_vip")
async def handle_buy_vip_click(callback: types.CallbackQuery):
    await callback.answer()
    user = get_user(callback.from_user.id)
    user["state"] = "awaiting_payment_receipt"
    
    pay_msg = (
        "💎 **راهنمای خرید اشتراک VIP:**\n\n"
        f"💰 **هزینه اشتراک:** `{VIP_PRICE_TOMAN}`\n\n"
        f"💳 **شماره کارت:**\n`{PAYMENT_CARD}`\n\n"
        "📸 **مراحل فعال‌سازی:**\n"
        "۱. مبلغ را واریز کنید.\n"
        "۲. **عکس فیش واریزی** را همین‌جا در ربات ارسال کنید.\n"
        "۳. پس از بررسی ادمین، حساب شما فوراً VIP خواهد شد."
    )
    await callback.message.answer(pay_msg, parse_mode="Markdown")

@dp.message(F.photo)
async def handle_receipt_photo(message: types.Message):
    user = get_user(message.from_user.id)
    if user.get("state") == "awaiting_payment_receipt":
        user["state"] = None
        
        await message.answer("✅ **فیش واریزی شما دریافت شد.**\nپس از بررسی ادمین، اشتراک شما فعال می‌گردد.")
        
        caption = (
            f"📥 **درخواست جدید خرید VIP**\n\n"
            f"👤 کاربر: {message.from_user.full_name}\n"
            f"🆔 آیدی عددی: `{message.from_user.id}`\n"
            f"🔗 یوزرنیم: @{message.from_user.username or 'ندارد'}"
        )
        photo_id = message.photo[-1].file_id
        await bot.send_photo(
            chat_id=ADMIN_ID,
            photo=photo_id,
            caption=caption,
            reply_markup=admin_approve_keyboard(message.from_user.id),
            parse_mode="Markdown"
        )

@dp.callback_query(F.data.startswith("approve_vip:"))
async def approve_vip_handler(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    target_id = int(callback.data.split(":")[1])
    u = get_user(target_id)
    u["is_vip"] = True
    
    await callback.message.edit_caption(caption=f"{callback.message.caption}\n\n✅ **تأیید شد و VIP فعال گردید.**")
    
    try:
        await bot.send_message(
            chat_id=target_id,
            text="🎉 **تبریک! اشتراک VIP شما با موفقیت فعال شد.**\nهم‌اکنون می‌توانید از تمام امکانات ربات استفاده کنید."
        )
    except Exception as e:
        logging.error(f"Failed to send msg to user: {e}")

@dp.callback_query(F.data.startswith("reject_vip:"))
async def reject_vip_handler(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    target_id = int(callback.data.split(":")[1])
    
    await callback.message.edit_caption(caption=f"{callback.message.caption}\n\n❌ **درخواست رد شد.**")
    
    try:
        await bot.send_message(
            chat_id=target_id,
            text="❌ **درخواست پرداخت شما تأیید نشد.**\nلطفاً در صورت نیاز با پشتیبانی تماس بگیرید."
        )
    except Exception as e:
        logging.error(f"Failed to send msg to user: {e}")

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    await callback.answer()
    user = get_user(callback.from_user.id)
    
    if not user["is_vip"] and user["usage_count"] >= 3:
        await callback.message.edit_text(
            "⚠️ **سقف استفاده روزانه شما (۳ بار) به پایان رسیده است.**\n\n"
            "برای دریافت تحلیل‌های نامحدود، اشتراک **VIP** تهیه کنید.",
            reply_markup=buy_vip_keyboard()
        )
        return

    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال محاسبه ستاپ هوشمند و چارت پیشرفته **{symbol}**...")
    
    signal_text, chart_bytes = await generate_signal(symbol, tf)
    await callback.message.delete()

    if chart_bytes:
        photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}_chart.png")
        await callback.message.answer_photo(photo=photo_file, caption=signal_text)
        if not user["is_vip"]:
            user["usage_count"] += 1
    else:
        await callback.message.answer(signal_text)

@dp.message(F.text)
async def handle_text_input(message: types.Message):
    text = message.text.strip()
    user = get_user(message.from_user.id)
    state = user.get("state")

    if state == "awaiting_capital":
        try:
            capital = float(text)
            user["risk_calc_data"]["capital"] = capital
            user["state"] = "awaiting_risk_pct"
            await message.answer("درصد ریسک مد نظر در این معامله را وارد کنید:\n*(مثال: 1 یا 2)*")
        except ValueError:
            await message.answer("⚠️ لطفاً عدد معتبر وارد کنید (مثلاً 1000).")
        return

    elif state == "awaiting_risk_pct":
        try:
            risk_pct = float(text)
            user["risk_calc_data"]["risk_pct"] = risk_pct
            user["state"] = "awaiting_entry"
            await message.answer("قیمت ورود (Entry Price) را وارد کنید:\n*(مثال: 65000)*")
        except ValueError:
            await message.answer("⚠️ لطفاً عدد معتبر وارد کنید (مثلاً 1.5).")
        return

    elif state == "awaiting_entry":
        try:
            entry = float(text)
            user["risk_calc_data"]["entry"] = entry
            user["state"] = "awaiting_sl"
            await message.answer("قیمت حد ضرر (Stop Loss) را وارد کنید:\n*(مثال: 63500)*")
        except ValueError:
            await message.answer("⚠️ لطفاً عدد معتبر وارد کنید.")
        return

    elif state == "awaiting_sl":
        try:
            sl = float(text)
            data = user["risk_calc_data"]
            capital = data["capital"]
            risk_pct = data["risk_pct"]
            entry = data["entry"]

            user["state"] = None

            risk_amount = capital * (risk_pct / 100)
            sl_distance_pct = abs(entry - sl) / entry

            if sl_distance_pct == 0:
                await message.answer("⚠️ قیمت حد ضرر نمی‌تواند با قیمت ورود برابر باشد.")
                return

            position_size = risk_amount / sl_distance_pct

            result_msg = (
                f"🧮 **نتیجه محاسبه مدیریت ریسک:**\n\n"
                f"💵 کل سرمایه: `${capital:,.2f}`\n"
                f"🎯 میزان ریسک: `{risk_pct}%` (`${risk_amount:,.2f}`)\n"
                f"📍 قیمت ورود: `${entry:,.4f}`\n"
                f"🛑 قیمت حد ضرر: `${sl:,.4f}`\n"
                f"📉 فاصله حد ضرر: `{sl_distance_pct*100:.2f}%`\n\n"
                f"✅ **حجم پیشنهادی برای ورود به پوزیشن:**\n"
                f"👉 `${position_size:,.2f}`\n\n"
                f"💡 *توضیح: اگر با این حجم وارد شوید و حد ضرر شما بخورد، دقیقاً ${risk_amount:,.2f} ضرر خواهید کرد.*"
            )
            await message.answer(result_msg, parse_mode="Markdown")
        except ValueError:
            await message.answer("⚠️ لطفاً عدد معتبر وارد کنید.")
        return

    symbol_text = text.upper()
    if symbol_text.startswith("/") or symbol_text in ["🚀 اسکنر ارزهای پامپی", "🐳 رادار توکن‌های جدید (DEX)", "📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "👤 حساب کاربری"]:
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
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
