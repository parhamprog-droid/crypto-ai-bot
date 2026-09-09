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

async def get_crypto_dataframe(symbol="BTC/USDT", timeframe="1h", limit=80):
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
        
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
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
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.dexscreener.com/token-boosts/top/v1") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    filtered = []
                    seen_symbols = set()
                    
                    for item in data:
                        token_address = item.get("tokenAddress")
                        if token_address:
                            pair_url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
                            async with session.get(pair_url) as p_resp:
                                if p_resp.status == 200:
                                    p_data = await p_resp.json()
                                    pairs = p_data.get("pairs", [])
                                    if pairs:
                                        best_pair = pairs[0]
                                        token_symbol = best_pair.get("baseToken", {}).get("symbol", "N/A")
                                        token_name = best_pair.get("baseToken", {}).get("name", "N/A")
                                        price = best_pair.get("priceUsd", "0")
                                        liquidity = best_pair.get("liquidity", {}).get("usd", 0)
                                        chain = best_pair.get("chainId", "N/A")
                                        
                                        if token_symbol not in seen_symbols and liquidity > 20000:
                                            seen_symbols.add(token_symbol)
                                            filtered.append({
                                                "symbol": token_symbol,
                                                "name": token_name,
                                                "price": price,
                                                "liquidity": liquidity,
                                                "chain": chain
                                            })
                        if len(filtered) >= 5:
                            break
                    return filtered
        except Exception as e:
            logging.error(f"DEX Fetch Error: {e}")
            return []

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

    recent_high = df['High'].iloc[-30:].max()
    recent_low = df['Low'].iloc[-30:].min()
    last_price = df['Close'].iloc[-1]

    ax_main.plot([0, n-1], [df['High'].iloc[0], recent_high], color='#8b0000', linestyle='-', linewidth=1, alpha=0.7)
    ax_main.plot([0, n-1], [df['Low'].iloc[0], recent_low], color='#006400', linestyle='-', linewidth=1, alpha=0.7)
    
    support_box_bottom = recent_low * 0.995
    ax_main.axhspan(support_box_bottom, recent_low, facecolor='#ffcccc', edgecolor='red', hatch='//', alpha=0.4)
    
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n-1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red', fontsize=8, fontweight='bold', va='center')

    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - AlphaEngine Pro", fontsize=12, fontweight='bold', pad=10, color='#222222')
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

async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد. لطفاً نماد معتبر وارد کنید.", None

    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    high_24h = df['High'].max()
    low_24h = df['Low'].min()
    change_24h = ((price - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100

    prompt = f"""
    تو یک سیستم معاملاتی هوشمند کریپتو هستی. برای ارز {formatted_symbol} در تایم‌فریم {timeframe} ستاپ دقیق بنویس.
    داده‌های مارکت:
    - قیمت فعلی: {price} USDT
    - بالاترین قیمت: {high_24h} | پایین‌ترین قیمت: {low_24h}
    - شاخص RSI: {rsi:.2f}
    - تغییرات 24 ساعت: {change_24h:.2f}%

    خروجی را دقیقا با این فرمت ارسال کن:
    ⚡️ AlphaEngine Pro | #{formatted_symbol.replace('/', '')}
    ⏱ تایم‌فریم: {timeframe}

    🎯 ستاپ معاملاتی:
    • جهت پیشنهادی: [Long 🟢 یا Short 🔴]
    • محدوده ورود (Entry Zone): [بازه قیمتی منطقی]
    • اهرم پیشنهادی (Leverage): [Cross 2x-5x یا 5x-10x]

    🚀 اهداف سودآوری (Targets):
    ▫️ TP1: [عدد]
    ▫️ TP2: [عدد]
    ▫️ TP3: [عدد]

    🛑 حد ضرر (Stop Loss): [عدد]
    ⚖️ ریسک به ریوارد: [مثلا 1:2.5]

    📊 تحلیل تکنیکال خلاصه:
    [۲ جمله کوتاه و تحلیلی از وضعیت قیمت و RSI]
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
    get_user(message.from_user.id)
    await message.answer(
        "👋 به **AlphaEngine Pro** خوش آمدید!\n\n"
        "نام ارز مورد نظر خود را وارد کنید یا از دکمه‌های زیر استفاده کنید:",
        reply_markup=main_keyboard
    )

@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
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
    msg = await message.answer("🔎 در حال استعلام آخرین توکن‌های پرنقدینگی در صرافی‌های غیرمتمرکز...")
    tokens = await fetch_dex_tokens()
    
    if not tokens:
        await msg.edit_text("⚠️ اطلاعات توکن‌های غیرمتمرکز دریافت نشد.")
        return
    
    text = "🐳 **توکن‌های ترند و پرنقدینگی On-Chain (شناسایی‌شده):**\n\n"
    for t in tokens:
        text += f"🪙 **{t['name']} ({t['symbol']})**\n"
        text += f"🌐 شبکه: `{t['chain'].upper()}`\n"
        text += f"💵 قیمت: `${float(t['price']):.6f}`\n"
        text += f"💧 نقدینگی استخر: `${t['liquidity']:,.0f}`\n"
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

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    await callback.answer()
    _, symbol, tf = callback.data.split(":")
    await callback.message.edit_text(f"🔄 در حال محاسبه ستاپ هوشمند و چارت **{symbol}**...")
    
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
