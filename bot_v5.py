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

from pydub import AudioSegment

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from google import genai
from google.genai import types as genai_types
from aiohttp import web

# --- تنظیمات لاگینگ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- خواندن امن ENV ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("❌ ENV 'TELEGRAM_BOT_TOKEN' or 'BOT_TOKEN' is not set!")
if not GEMINI_API_KEY:
    raise RuntimeError("❌ ENV 'GEMINI_API_KEY' is not set!")

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError:
    ADMIN_ID = 0
    logging.warning(f"ADMIN_ID '{ADMIN_ID_RAW}' is not a valid integer. Using 0.")

# --- کلاینت Gemini جدید ---
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# --- Bot & Dispatcher ---
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

user_data = {}
price_alerts = []
banned_users = set()
admin_logs = []
admin_state = {}
system_settings = {
    "pump_detector_enabled": True,
    "daily_digest_enabled": True,
}
ADMIN_LOG_FILE = "admin_logs.txt"

# --- دستور سیستمی ---
PERSIAN_SYSTEM_INSTRUCTION = (
    "تو یک دستیار حرفه‌ای تحلیل بازار کریپتو هستی. "
    "قانون مهم: توضیحات، تحلیل، جملات روایی و توضیح ستاپ را فقط و فقط به زبان فارسی روان و تریدری بنویس. "
    "اما اصطلاحات تخصصی مثل نام نمادها (BTC, ETH, SOL, BNB)، تایم‌فریم‌ها (15m, 1h, 4h, 1d)، "
    "شاخص‌ها (RSI, MACD, EMA, ATR, FVG, Order Block, Bollinger, Volume)، "
    "و مفاهیم ترید (Long, Short, Entry, TP, SL, Stop Loss, Take Profit, Cross, Leverage, Position) "
    "حتماً به انگلیسی باقی بمانند. هرگز کل پاسخ را به انگلیسی نده. هرگز اصطلاحات تخصصی را به فارسی ترجمه نکن. "
    "نمونه درست: «روند 4H صعودی است و RSI روی 65 قرار دارد. Entry مناسب در محدوده Long با SL زیر 63000.»\n\n"
    "⚠️ قوانین املایی و نگارشی:\n"
    "۱. املای کلمات فارسی را دقیق رعایت کن (مثلاً «قیمت» نه «قمت»).\n"
    "۲. هرگز از هشتگ (#) قبل از نام نمادها استفاده نکن.\n"
    "۳. ابتدای هر تحلیل، حتماً با ایموجی ⚡️ شروع کن.\n"
    "۴. اعداد قیمت را با دقت و فرمت استاندارد بنویس."
)

_AVAILABLE_GEMINI_MODELS = None


async def get_available_models():
    global _AVAILABLE_GEMINI_MODELS
    if _AVAILABLE_GEMINI_MODELS is not None:
        return _AVAILABLE_GEMINI_MODELS

    preferred = [
        "gemini-2.5-flash",
        "gemini-2.5-pro",
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-2.0-flash-lite",
    ]
    try:
        response = await asyncio.to_thread(gemini_client.models.list)
        dynamic = []
        for m in response:
            try:
                if hasattr(m, "supported_actions") and m.supported_actions:
                    if "generateContent" in m.supported_actions:
                        name = m.name.replace("models/", "")
                        dynamic.append(name)
                elif hasattr(m, "supported_generation_methods") and m.supported_generation_methods:
                    if "generateContent" in m.supported_generation_methods:
                        name = m.name.replace("models/", "")
                        dynamic.append(name)
            except Exception:
                continue

        combined = list(dict.fromkeys(preferred + dynamic))
        _AVAILABLE_GEMINI_MODELS = combined
        logging.info(f"✅ Available Gemini models: {combined[:5]}...")
        return combined
    except Exception as e:
        logging.warning(f"ListModels failed: {e}. Using hardcoded preferred list.")
        _AVAILABLE_GEMINI_MODELS = preferred
        return preferred


def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def log_admin_action(user_id: int, action: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] Admin {user_id}: {action}"
    admin_logs.append(log_entry)
    if len(admin_logs) > 200:
        admin_logs.pop(0)
    try:
        with open(ADMIN_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        logging.warning(f"Failed to write admin log: {e}")


def get_user(user_id: int):
    if user_id not in user_data:
        user_data[user_id] = {
            "usage_count": 0,
            "is_vip": False,
            "state": None,
            "risk_calc_data": {},
            "alert_temp": {},
            "referred_by": None,
            "referrals": [],
            "first_seen": datetime.datetime.now().isoformat(),
            "last_seen": datetime.datetime.now().isoformat(),
        }
    user_data[user_id]["last_seen"] = datetime.datetime.now().isoformat()
    return user_data[user_id]


def get_main_keyboard(user_id: int):
    rows = [
        [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
        [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
        [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
        [KeyboardButton(text="👥 سیستم دعوت و هدیه"), KeyboardButton(text="👤 حساب کاربری")],
    ]
    if is_admin(user_id):
        rows.append([KeyboardButton(text="⚙️ پنل ادمین")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def timeframe_keyboard(symbol: str):
    row1 = [
        InlineKeyboardButton(text="1m", callback_data=f"tf:{symbol}:1m"),
        InlineKeyboardButton(text="5m", callback_data=f"tf:{symbol}:5m"),
        InlineKeyboardButton(text="15m", callback_data=f"tf:{symbol}:15m"),
        InlineKeyboardButton(text="30m", callback_data=f"tf:{symbol}:30m"),
    ]
    row2 = [
        InlineKeyboardButton(text="1h", callback_data=f"tf:{symbol}:1h"),
        InlineKeyboardButton(text="2h", callback_data=f"tf:{symbol}:2h"),
        InlineKeyboardButton(text="4h", callback_data=f"tf:{symbol}:4h"),
        InlineKeyboardButton(text="6h", callback_data=f"tf:{symbol}:6h"),
    ]
    row3 = [
        InlineKeyboardButton(text="12h", callback_data=f"tf:{symbol}:12h"),
        InlineKeyboardButton(text="1d", callback_data=f"tf:{symbol}:1d"),
        InlineKeyboardButton(text="1w", callback_data=f"tf:{symbol}:1w"),
    ]
    return InlineKeyboardMarkup(inline_keyboard=[row1, row2, row3])


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


def admin_panel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin:stats"),
            InlineKeyboardButton(text="👥 کاربران", callback_data="admin:users_menu"),
        ],
        [
            InlineKeyboardButton(text="📢 پیام همگانی", callback_data="admin:broadcast_menu"),
            InlineKeyboardButton(text="🔔 هشدارها", callback_data="admin:alerts_menu"),
        ],
        [
            InlineKeyboardButton(text="📋 لاگ فعالیت‌ها", callback_data="admin:logs"),
            InlineKeyboardButton(text="⚙️ وضعیت سیستم", callback_data="admin:settings"),
        ],
    ])


def admin_users_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 لیست کاربران", callback_data="admin:users_list"),
            InlineKeyboardButton(text="🔍 جستجو", callback_data="admin:user_search"),
        ],
        [
            InlineKeyboardButton(text="💎 لیست VIP", callback_data="admin:vip_list"),
            InlineKeyboardButton(text="🚫 لیست بن", callback_data="admin:ban_list"),
        ],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back")],
    ])


def admin_broadcast_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 ارسال به همه", callback_data="admin:bcast_all")],
        [InlineKeyboardButton(text="💎 فقط VIP", callback_data="admin:bcast_vip")],
        [InlineKeyboardButton(text="👤 فقط غیر VIP", callback_data="admin:bcast_free")],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back")],
    ])


def admin_alerts_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 لیست هشدارها", callback_data="admin:alerts_list"),
            InlineKeyboardButton(text="🗑 حذف همه", callback_data="admin:alerts_clear"),
        ],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back")],
    ])


def admin_settings_keyboard():
    pump_status = "🟢 روشن" if system_settings["pump_detector_enabled"] else "🔴 خاموش"
    digest_status = "🟢 روشن" if system_settings["daily_digest_enabled"] else "🔴 خاموش"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🚀 رادار پامپ/دامپ: {pump_status}",
            callback_data="admin:toggle_pump"
        )],
        [InlineKeyboardButton(
            text=f"☀️ بولتن روزانه: {digest_status}",
            callback_data="admin:toggle_digest"
        )],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back")],
    ])


def admin_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin:back")]
    ])

# --- توابع داده بازار ---
async def get_crypto_dataframe(symbol="BTC/USDT", timeframe="1h", limit=100):
    exchange = ccxt.coinex()
    try:
        formatted_symbol = symbol.upper().strip()
        if not formatted_symbol.endswith("/USDT") and not formatted_symbol.endswith("USDT"):
            formatted_symbol = f"{formatted_symbol}/USDT"
        elif formatted_symbol.endswith("USDT") and "/" not in formatted_symbol:
            formatted_symbol = formatted_symbol.replace("USDT", "/USDT")

        ohlcv = await exchange.fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

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

        df['BB_Middle'] = df['Close'].rolling(window=20).mean()
        df['BB_Std'] = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['BB_Middle'] + (df['BB_Std'] * 2)
        df['BB_Lower'] = df['BB_Middle'] - (df['BB_Std'] * 2)
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']

        df['FVG_Bullish'] = (df['Low'] > df['High'].shift(2))
        df['OrderBlock_Bullish'] = (
            (df['Close'] > df['Open'])
            & (df['Close'].shift(1) < df['Open'].shift(1))
            & (df['Volume'] > df['Volume'].rolling(10).mean() * 1.5)
        )

        return formatted_symbol, df
    except Exception as e:
        logging.error(f"CCXT Error ({symbol}): {type(e).__name__}: {e}")
        return None, None
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def fetch_orderbook_and_futures(symbol="BTC/USDT"):
    exchange = ccxt.coinex()
    try:
        formatted_symbol = symbol.upper()
        orderbook = await exchange.fetch_order_book(formatted_symbol, limit=20)

        bids_volume = sum([b[1] for b in orderbook['bids']])
        asks_volume = sum([a[1] for a in orderbook['asks']])
        orderbook_ratio = bids_volume / asks_volume if asks_volume > 0 else 1.0

        return {
            "bids_vol": bids_volume,
            "asks_vol": asks_volume,
            "ratio": orderbook_ratio,
            "open_interest": "افزایشی 📈" if bids_volume > asks_volume else "کاهشی 📉"
        }
    except Exception:
        return {"bids_vol": 0, "asks_vol": 0, "ratio": 1.0, "open_interest": "نامشخص ⚪️"}
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


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

    return "- Title: Crypto Market Volatility\n  Body: High market volatility continues across major crypto assets.\n\n"


async def scan_pump_candidates():
    exchange = ccxt.coinex()
    try:
        tickers = await exchange.fetch_tickers()
        candidates = []
        for symbol, data in tickers.items():
            if symbol.endswith("/USDT"):
                volume = data.get('quoteVolume') or 0
                change = data.get('percentage') or 0
                if volume >= 30000 and change >= 2.0:
                    candidates.append({'symbol': symbol, 'change': float(change), 'volume': float(volume)})
        return sorted(candidates, key=lambda x: x['change'], reverse=True)[:5]
    except Exception:
        return []
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


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
                        if len(filtered) >= 5:
                            break
                    return filtered
        except Exception as e:
            logging.error(f"Gecko Error: {e}")
    return [{"symbol": "BONK", "price": "0.000021"}, {"symbol": "WIF", "price": "1.84"}]


def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    fig, (ax_main, ax_rsi) = plt.subplots(
        2, 1, figsize=(11, 6.5),
        gridspec_kw={'height_ratios': [3, 1]},
        facecolor='#f8f9fa'
    )
    ax_main.set_facecolor('#ffffff')
    ax_rsi.set_facecolor('#ffffff')

    n = len(df)
    for i in range(n):
        open_p, close_p = df['Open'].iloc[i], df['Close'].iloc[i]
        high_p, low_p = df['High'].iloc[i], df['Low'].iloc[i]
        color = '#089981' if close_p >= open_p else '#f23645'
        ax_main.plot([i, i], [low_p, high_p], color=color, linewidth=1)
        ax_main.bar(i, abs(close_p - open_p), bottom=min(open_p, close_p), color=color, width=0.6)

    ax_main.plot(range(n), df['EMA_50'], color='#2196F3', linewidth=1.2, label='EMA 50')
    ax_main.plot(range(n), df['EMA_200'], color='#FF9800', linewidth=1.2, label='EMA 200')

    last_price = df['Close'].iloc[-1]
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(
        n - 1, last_price, f" {last_price:.4f}",
        color='white', backgroundcolor='red',
        fontsize=8, fontweight='bold', va='center'
    )

    ax_main.grid(True, linestyle='--', alpha=0.5, color='#e0e0e0')
    ax_main.set_title(
        f"{clean_symbol} {timeframe} - Multi-Timeframe AlphaEngine Pro",
        fontsize=12, fontweight='bold', pad=10
    )
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


async def query_gemini(prompt: str) -> str:
    candidate_models = await get_available_models()

    last_error = None
    for model_name in candidate_models:
        try:
            response = await asyncio.to_thread(
                gemini_client.models.generate_content,
                model=model_name,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=PERSIAN_SYSTEM_INSTRUCTION,
                    temperature=0.7,
                ),
            )
            if response and response.text:
                return response.text
        except Exception as e:
            err_str = str(e)
            last_error = e
            logging.warning(f"Gemini model '{model_name}' failed: {type(e).__name__}: {err_str[:200]}")
            if "404" in err_str or "NOT_FOUND" in err_str:
                global _AVAILABLE_GEMINI_MODELS
                if _AVAILABLE_GEMINI_MODELS and model_name in _AVAILABLE_GEMINI_MODELS:
                    _AVAILABLE_GEMINI_MODELS.remove(model_name)
            continue

    raise last_error or Exception("هیچ‌کدام از مدل‌های Gemini پاسخ ندادند.")


async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{symbol}** پیدا نشد.", None

    _, df_1d = await get_crypto_dataframe(symbol, "1d", 30)
    _, df_4h = await get_crypto_dataframe(symbol, "4h", 30)
    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1h", 30)

    trend_1d = "صعودی 🟢" if (df_1d is not None and df_1d['Close'].iloc[-1] > df_1d['EMA_50'].iloc[-1]) else "نزولی 🔴"
    trend_4h = "صعودی 🟢" if (df_4h is not None and df_4h['Close'].iloc[-1] > df_4h['EMA_50'].iloc[-1]) else "نزولی 🔴"
    btc_bullish = (df_btc is not None and df_btc['Close'].iloc[-1] > df_btc['EMA_50'].iloc[-1])

    df['TR'] = np.maximum(
        df['High'] - df['Low'],
        np.maximum(
            abs(df['High'] - df['Close'].shift(1)),
            abs(df['Low'] - df['Close'].shift(1))
        )
    )
    df['ATR'] = df['TR'].rolling(window=14).mean()
    atr_val = round(float(df['ATR'].iloc[-1]), 4)

    atr_for_sl = round(atr_val * 1.5, 4)
    atr_sl_max = round(atr_val * 3, 4)

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
    - قیمت فعلی: {price} | ATR: {atr_val}
    - محدوده مجاز SL بر اساس ATR: حداقل {atr_for_sl} و حداکثر {atr_sl_max} فاصله از Entry
    - وضعیت نوسان (Bollinger Squeeze): {squeeze_status}
    - FVG خریداران: {has_fvg} | Order Block: {has_ob}
    - نسبت سفارشات خرید/فروش: {ob_data['ratio']:.2f} | RSI: {rsi:.2f}

    فرمت خروجی دقیقاً طبق ساختار زیر باشد:

    ⚡️ AlphaEngine Institutional Multi-TF Setup
    📊 نماد: {formatted_symbol.replace('/', '')} | تایم‌فریم: {timeframe}
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

    ⚠️ یادآوری نهایی:
    - توضیحات، جملات و متن تحلیل را فارسی روان و تریدری بنویس.
    - نام ارز، تایم‌فریم، اصطلاحات تکنیکال (RSI, MACD, EMA, ATR, FVG, OB, Bollinger)،
      و مفاهیم ترید (Long, Short, Entry, TP, SL, Cross, Leverage) را حتماً انگلیسی نگه دار.
    - ابتدای پاسخ را با ⚡️ شروع کن (نه با AlphaEngine).
    - هرگز از # قبل از نام نماد استفاده نکن. فقط بنویس: نماد: SOLUSDT
    - املای فارسی را دقیق رعایت کن (مثلاً «قیمت» نه «قمت»).
    - Stop Loss را منطقی و نزدیک به ATR تعیین کن.
      قاعده: SL باید حداقل 1.5×ATR و حداکثر 3×ATR از Entry فاصله داشته باشد.
    - Entry Zone باید حداکثر ۲٪ از قیمت فعلی فاصله داشته باشد.
    """

    try:
        response_text = await query_gemini(prompt)
    except Exception as e:
        return f"⚠️ خطا در تحلیل Gemini:\n`{e}`", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

# --- پردازش پیام صوتی ---
@dp.message(F.voice)
async def handle_voice_message(message: types.Message):
    user_id = message.from_user.id
    if user_id in banned_users:
        await message.answer("🚫 شما از استفاده از بات محروم شده‌اید.")
        return

    msg = await message.answer("🎙 در حال تبدیل و تحلیل ویس توسط هوش مصنوعی...")
    file_id = message.voice.file_id

    ogg_filename = f"voice_{message.message_id}_{user_id}.ogg"
    wav_filename = f"voice_{message.message_id}_{user_id}.wav"

    try:
        file = await bot.get_file(file_id)
        await bot.download_file(file.file_path, destination=ogg_filename)

        sound = AudioSegment.from_file(ogg_filename, format="ogg")
        sound.export(wav_filename, format="wav")

        def _upload_and_analyze():
            uploaded = gemini_client.files.upload(file=wav_filename)
            prompt = (
                "این یک فایل صوتی از کاربر در مورد بازار کریپتو و ارزهای دیجیتال است. "
                "متن صحبت او را متوجه شو، سوال یا درخواست او را بررسی کن و یک پاسخ جامع، تحلیلی و حرفه‌ای ارائه بده.\n\n"
                "⚠️ توضیحات را فارسی بنویس، اما نام ارزها، اصطلاحات تکنیکال و مفاهیم ترید را انگلیسی نگه دار."
            )
            models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash", "gemini-2.5-pro"]
            last_exc = None
            for m_name in models_to_try:
                try:
                    resp = gemini_client.models.generate_content(
                        model=m_name,
                        contents=[uploaded, prompt],
                        config=genai_types.GenerateContentConfig(
                            system_instruction=PERSIAN_SYSTEM_INSTRUCTION,
                        ),
                    )
                    if resp and resp.text:
                        return resp.text
                except Exception as ex:
                    last_exc = ex
                    continue
            if last_exc:
                raise last_exc
            return None

        response_text = await asyncio.to_thread(_upload_and_analyze)

        if response_text:
            await msg.edit_text(f"🗣 **پاسخ دستیار صوتی:**\n\n{response_text}", parse_mode="Markdown")
        else:
            await msg.edit_text("⚠️ متأسفانه متنی از فایل صوتی تشخیص داده نشد.")

    except Exception as e:
        logging.error(f"Voice handling error: {type(e).__name__}: {e}")
        await msg.edit_text(f"⚠️ خطا در پردازش فایل صوتی:\n`{e}`", parse_mode="Markdown")
    finally:
        for path in [ogg_filename, wav_filename]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


# --- دستور استارت ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    if user_id in banned_users:
        await message.answer("🚫 شما از استفاده از بات محروم شده‌اید.")
        return

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
                        await bot.send_message(
                            referrer_id,
                            "🎉 **تبریک!** ۳ کاربر جدید دعوت کردید و حساب شما **VIP** شد!"
                        )
                    except Exception as e:
                        logging.warning(f"Failed to notify referrer: {e}")

    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
    status_text = "✨ VIP" if user["is_vip"] else "Standard 🔑"

    start_text = (
        f"🏛 **AlphaEngine Terminal Pro**\n"
        f"────────────────────────\n\n"
        f"به ترمینال تخصصی تحلیل الگوریتمی بازار کریپتو خوش آمدید.\n\n"
        f"🔰 **وضعیت حساب:** `{status_text}`\n"
        f"📡 **وضعیت اتصال:** آنلاین 🟢\n\n"
        f"💡 **راهنمای سریع:**\n"
        f"برای دریافت ستاپ معاملاتی و چارت تحلیلی، کافی است **نام نماد** "
        f"(مانند `BTC` یا `SOL`) را ارسال کرده یا ویس بفرستید.\n\n"
        f"🎁 **ارتقا به سطح VIP:**\n"
        f"با دعوت ۳ دوست، دسترسی VIP را به صورت رایگان دریافت کنید.\n"
        f"🔗 **لینک دعوت اختصاصی:**\n`{ref_link}`\n\n"
        f"👥 دعوت‌های فعال: **{len(user['referrals'])} از ۳**"
    )

    await message.answer(start_text, reply_markup=get_main_keyboard(user_id), parse_mode="Markdown")


# --- سیستم دعوت ---
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
        f"• وضعیت VIP: {'🟢 فعال' if user['is_vip'] else '🔴 غیرفعال'}",
        parse_mode="Markdown"
    )


# --- اخبار ---
@dp.message(F.text == "📰 اخبار و تحلیل احساسات")
async def crypto_news_handler(message: types.Message):
    msg = await message.answer("🔄 در حال دریافت آخرین اخبار...")
    raw_news = await fetch_crypto_news()

    prompt = (
        f"این اخبار کریپتو را تحلیلی و ساختاریافته خلاصه کن:\n{raw_news}\n\n"
        "⚠️ توضیحات و تحلیل را فارسی بنویس، اما نام ارزها (BTC, ETH)، "
        "اصطلاحات (ETF, DeFi, Whale, Halving, Market Cap) را انگلیسی نگه دار."
    )
    try:
        response_text = await query_gemini(prompt)
        await msg.edit_text(f"📰 **خلاصه اخبار:**\n\n{response_text}", parse_mode="Markdown")
    except Exception:
        await msg.edit_text(f"📰 خلاصه اخبار:\n\n{raw_news[:1000]}")


# --- هشدار قیمت ---
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


# --- اسکنر پامپ ---
@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer("🔒 مخصوص VIP", reply_markup=buy_vip_keyboard())
        return

    msg = await message.answer("🔍 در حال اسکن...")
    candidates = await scan_pump_candidates()
    if not candidates:
        await msg.edit_text("⚠️ ارزی یافت نشد.")
        return

    text = "🔥 **ارزهای مستعد پامپ:**\n\n"
    for c in candidates:
        text += f"📌 **{c['symbol'].replace('/', '')}** | رشد: `+{c['change']:.2f}%`\n"
    await msg.edit_text(text, parse_mode="Markdown")


# --- رادار DEX ---
@dp.message(F.text == "🐳 رادار توکن‌های جدید (DEX)")
async def dex_radar_handler(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["is_vip"]:
        await message.answer("🔒 مخصوص VIP", reply_markup=buy_vip_keyboard())
        return

    msg = await message.answer("🔎 در حال رصد...")
    tokens = await fetch_dex_tokens()
    text = "🐳 **توکن‌های ترند DEX:**\n\n"
    for t in tokens:
        text += f"🪙 **{t['symbol']}** | قیمت: `${t['price']}`\n"
    await msg.edit_text(text, parse_mode="Markdown")


# --- شاخص ترس و طمع ---
@dp.message(F.text == "📊 شاخص ترس و طمع")
async def fear_and_greed(message: types.Message):
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.alternative.me/fng/") as resp:
            if resp.status == 200:
                data = await resp.json()
                item = data["data"][0]
                await message.answer(
                    f"📊 **شاخص ترس و طمع:**\n\n"
                    f"🎯 عدد: **{item['value']}/100**\n"
                    f"📌 وضعیت: **{item['value_classification']}**"
                )


# --- محاسبه ریسک ---
@dp.message(F.text == "🧮 محاسبه ریسک")
async def start_risk_calc(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = "awaiting_capital"
    user["risk_calc_data"] = {}
    await message.answer("🧮 **محاسبه مدیریت ریسک**\n\nموجودی کل حساب (دلار):")


# --- حساب کاربری ---
@dp.message(F.text == "👤 حساب کاربری")
async def user_profile(message: types.Message):
    user = get_user(message.from_user.id)
    status_text = "💎 VIP" if user["is_vip"] else "👤 رایگان"
    await message.answer(
        f"👤 **پروفایل کاربری:**\n\n🆔 آیدی: `{message.from_user.id}`\n"
        f"👑 وضعیت: {status_text}\n👥 تعداد دعوت‌ها: **{len(user['referrals'])}**",
        parse_mode="Markdown"
    )


# --- پنل ادمین ---
@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("🚫 شما دسترسی ادمین ندارید.")
        return
    log_admin_action(user_id, "Opened admin panel via /admin")
    await message.answer(
        "⚙️ **پنل ادمین AlphaEngine**\n\nاز منوی زیر یک گزینه را انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="Markdown"
    )


@dp.message(F.text == "⚙️ پنل ادمین")
async def admin_panel_button(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("🚫 شما دسترسی ادمین ندارید.")
        return
    log_admin_action(user_id, "Opened admin panel via button")
    await message.answer(
        "⚙️ **پنل ادمین AlphaEngine**\n\nاز منوی زیر یک گزینه را انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("🚫 دسترسی ندارید.", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    await callback.answer()

    if action == "back":
        try:
            await callback.message.edit_text(
                "⚙️ **پنل ادمین AlphaEngine**\n\nاز منوی زیر انتخاب کنید:",
                reply_markup=admin_panel_keyboard(),
                parse_mode="Markdown"
            )
        except Exception:
            await callback.message.answer(
                "⚙️ **پنل ادمین AlphaEngine**",
                reply_markup=admin_panel_keyboard(),
                parse_mode="Markdown"
            )
        return

    if action == "stats":
        total_users = len(user_data)
        total_vip = sum(1 for u in user_data.values() if u.get("is_vip"))
        total_alerts = len(price_alerts)
        total_banned = len(banned_users)
        today = datetime.datetime.now().date().isoformat()
        active_today = sum(1 for u in user_data.values() if u.get("last_seen", "").startswith(today))
        pump_status = "🟢" if system_settings["pump_detector_enabled"] else "🔴"
        digest_status = "🟢" if system_settings["daily_digest_enabled"] else "🔴"

        text = (
            f"📊 **آمار کلی بات**\n\n"
            f"👥 کل کاربران: **{total_users}**\n"
            f"💎 کاربران VIP: **{total_vip}**\n"
            f"🆓 کاربران رایگان: **{total_users - total_vip}**\n"
            f"🚫 کاربران بن‌شده: **{total_banned}**\n"
            f"🔔 هشدارهای فعال: **{total_alerts}**\n"
            f"📅 فعال امروز: **{active_today}**\n\n"
            f"⚙️ **وضعیت سیستم:**\n"
            f"🚀 رادار پامپ/دامپ: {pump_status}\n"
            f"☀️ بولتن روزانه: {digest_status}"
        )
        log_admin_action(user_id, "Viewed stats")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "users_menu":
        await callback.message.edit_text(
            "👥 **مدیریت کاربران**\n\nیکی از گزینه‌ها را انتخاب کنید:",
            reply_markup=admin_users_menu_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "users_list":
        if not user_data:
            text = "📋 هیچ کاربری ثبت نشده."
        else:
            sorted_users = sorted(user_data.items(), key=lambda x: x[1].get("last_seen", ""), reverse=True)[:20]
            text = "📋 **آخرین ۲۰ کاربر فعال:**\n\n"
            for uid, info in sorted_users:
                vip_tag = "💎" if info.get("is_vip") else "🆓"
                ban_tag = "🚫" if uid in banned_users else ""
                refs = len(info.get("referrals", []))
                text += f"{vip_tag}{ban_tag} `{uid}` | دعوت: {refs}\n"
            text += f"\n👥 کل: **{len(user_data)}** کاربر"
        log_admin_action(user_id, "Viewed users list")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "vip_list":
        vips = [(uid, u) for uid, u in user_data.items() if u.get("is_vip")]
        if not vips:
            text = "💎 هیچ کاربر VIP وجود ندارد."
        else:
            text = f"💎 **کاربران VIP ({len(vips)}):**\n\n"
            for uid, info in vips[:30]:
                refs = len(info.get("referrals", []))
                text += f"💎 `{uid}` | دعوت: {refs}\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "ban_list":
        if not banned_users:
            text = "🚫 لیست بن خالی است."
        else:
            text = f"🚫 **کاربران بن‌شده ({len(banned_users)}):**\n\n"
            for uid in list(banned_users)[:30]:
                text += f"🚫 `{uid}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "user_search":
        admin_state[user_id] = {"action": "awaiting_user_search"}
        await callback.message.answer("🔍 **جستجوی کاربر**\n\nآیدی عددی کاربر را ارسال کنید:")
        return

    if action == "broadcast_menu":
        await callback.message.edit_text(
            "📢 **ارسال پیام همگانی**\n\nمخاطب را انتخاب کنید:",
            reply_markup=admin_broadcast_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action in ("bcast_all", "bcast_vip", "bcast_free"):
        target = {
            "bcast_all": "همه کاربران",
            "bcast_vip": "فقط VIPها",
            "bcast_free": "فقط غیر VIPها",
        }[action]
        admin_state[user_id] = {"action": "awaiting_broadcast", "target": action}
        await callback.message.answer(
            f"📢 **ارسال پیام همگانی**\n\n🎯 مخاطب: **{target}**\n\nمتن پیام را ارسال کنید:"
        )
        return

    if action == "alerts_menu":
        await callback.message.edit_text(
            f"🔔 **مدیریت هشدارها**\n\nتعداد هشدارهای فعال: **{len(price_alerts)}**",
            reply_markup=admin_alerts_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "alerts_list":
        if not price_alerts:
            text = "📋 هیچ هشدار فعالی وجود ندارد."
        else:
            text = f"📋 **هشدارهای فعال ({len(price_alerts)}):**\n\n"
            for a in price_alerts[:30]:
                text += f"🪙 `{a['symbol']}` | هدف: `${a['target_price']}` | کاربر: `{a['user_id']}`\n"
        log_admin_action(user_id, "Viewed alerts list")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "alerts_clear":
        count = len(price_alerts)
        price_alerts.clear()
        log_admin_action(user_id, f"Cleared {count} alerts")
        try:
            await callback.message.edit_text(
                f"✅ **{count} هشدار پاک شد.**",
                reply_markup=admin_back_keyboard(),
                parse_mode="Markdown"
            )
        except Exception:
            await callback.message.answer(
                f"✅ **{count} هشدار پاک شد.**",
                reply_markup=admin_back_keyboard(),
                parse_mode="Markdown"
            )
        return

    if action == "logs":
        if not admin_logs:
            text = "📋 هیچ لاگی ثبت نشده."
        else:
            text = "📋 **آخرین ۲۰ فعالیت ادمین:**\n\n"
            for line in admin_logs[-20:]:
                text += f"`{line}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "settings":
        await callback.message.edit_text(
            "⚙️ **وضعیت سیستم**\n\nبرای روشن/خاموش کردن، روی دکمه بزنید:",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "toggle_pump":
        system_settings["pump_detector_enabled"] = not system_settings["pump_detector_enabled"]
        state_txt = "روشن" if system_settings["pump_detector_enabled"] else "خاموش"
        log_admin_action(user_id, f"Toggled pump detector to {state_txt}")
        await callback.message.edit_text(
            f"✅ رادار پامپ/دامپ: **{state_txt}**",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "toggle_digest":
        system_settings["daily_digest_enabled"] = not system_settings["daily_digest_enabled"]
        state_txt = "روشن" if system_settings["daily_digest_enabled"] else "خاموش"
        log_admin_action(user_id, f"Toggled daily digest to {state_txt}")
        await callback.message.edit_text(
            f"✅ بولتن روزانه: **{state_txt}**",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return


# --- انتخاب تایم‌فریم ---
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
                await callback.message.answer_photo(
                    photo=photo_file,
                    caption=f"📊 **چارت {symbol} ({tf})**",
                    parse_mode="Markdown"
                )
            except Exception:
                await callback.message.answer_photo(photo=photo_file, caption=f"📊 چارت {symbol} ({tf})")

        if signal_text:
            try:
                await callback.message.answer(signal_text, parse_mode="Markdown")
            except Exception:
                await callback.message.answer(signal_text)

    except Exception as e:
        logging.error(f"Callback Error: {type(e).__name__}: {e}")
        await callback.message.answer(f"⚠️ خطا:\n{e}")


# --- ورودی متنی (شامل state های ادمین و کاربر) ---
@dp.message(F.text)
async def handle_text_input(message: types.Message):
    user_id = message.from_user.id
    if user_id in banned_users:
        await message.answer("🚫 شما از استفاده از بات محروم شده‌اید.")
        return

    text = message.text.strip()

    # --- state های ادمین ---
    if is_admin(user_id) and user_id in admin_state:
        st = admin_state[user_id]

        if st.get("action") == "awaiting_user_search":
            admin_state.pop(user_id, None)
            if not text.isdigit():
                await message.answer("⚠️ آیدی باید عددی باشد.")
                return
            target_uid = int(text)
            if target_uid not in user_data:
                await message.answer(f"❌ کاربر `{target_uid}` پیدا نشد.", parse_mode="Markdown")
                return
            info = user_data[target_uid]
            vip_tag = "💎 VIP" if info.get("is_vip") else "🆓 رایگان"
            ban_tag = "🚫 بن‌شده" if target_uid in banned_users else "✅ فعال"
            refs = len(info.get("referrals", []))
            await message.answer(
                f"👤 **اطلاعات کاربر**\n\n"
                f"🆔 آیدی: `{target_uid}`\n"
                f"👑 وضعیت: {vip_tag}\n"
                f"🚫 بن: {ban_tag}\n"
                f"👥 دعوت‌ها: **{refs}**\n"
                f"🕐 آخرین فعالیت: `{info.get('last_seen', 'N/A')[:19]}`",
                parse_mode="Markdown"
            )
            return

        if st.get("action") == "awaiting_broadcast":
            admin_state.pop(user_id, None)
            target = st.get("target")
            count = 0
            for uid, info in list(user_data.items()):
                if target == "bcast_vip" and not info.get("is_vip"):
                    continue
                if target == "bcast_free" and info.get("is_vip"):
                    continue
                try:
                    await bot.send_message(uid, f"📢 **پیام از ادمین:**\n\n{text}", parse_mode="Markdown")
                    count += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            log_admin_action(user_id, f"Broadcast to {target} ({count} users)")
            await message.answer(f"✅ پیام به **{count}** کاربر ارسال شد.", parse_mode="Markdown")
            return

    # --- state های کاربر ---
    user = get_user(user_id)
    state = user.get("state")

    if state == "awaiting_alert_symbol":
        formatted = text.upper()
        if not formatted.endswith("/USDT"):
            formatted += "/USDT"
        user["alert_temp"]["symbol"] = formatted
        user["state"] = "awaiting_alert_price"
        await message.answer(f"قیمت مد نظر برای **{formatted}** (دلار) را وارد کنید:")
        return

    elif state == "awaiting_alert_price":
        try:
            target_p = float(text)
            symbol = user["alert_temp"]["symbol"]

            exchange = ccxt.coinex()
            try:
                ticker = await exchange.fetch_ticker(symbol)
                current_p = ticker.get('last') or ticker.get('close')
            finally:
                try:
                    await exchange.close()
                except Exception:
                    pass

            price_alerts.append({
                "user_id": user_id,
                "symbol": symbol,
                "target_price": target_p,
                "condition": "above" if target_p > current_p else "below"
            })

            user["state"] = None
            await message.answer(
                f"✅ **هشدار قیمت ثبت شد!**\nارز: {symbol} | هدف: `${target_p}`",
                reply_markup=alert_success_keyboard(),
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer("⚠️ قیمت نامعتبر است.")
        return

    elif state == "awaiting_capital":
        try:
            user["risk_calc_data"]["capital"] = float(text)
            user["state"] = "awaiting_risk_pct"
            await message.answer("درصد ریسک (مثلاً 1 یا 2):")
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

    # --- تشخیص نماد برای تحلیل ---
    symbol_text = text.upper()
    menu_buttons = [
        "🚀 اسکنر ارزهای پامپی", "🐳 رادار توکن‌های جدید (DEX)",
        "📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "👤 حساب کاربری",
        "🔔 هشدار قیمت", "📰 اخبار و تحلیل احساسات", "👥 سیستم دعوت و هدیه",
        "⚙️ پنل ادمین"
    ]
    if symbol_text.startswith("/") or symbol_text in menu_buttons:
        return

    await message.answer(
        f"⏱ تایم‌فریم تحلیل **{symbol_text}** را انتخاب کنید:",
        reply_markup=timeframe_keyboard(symbol_text)
    )


# --- رادار پامپ/دامپ ---
async def pump_dump_detector_loop():
    tracked_symbols = [
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT',
        'ADA/USDT', 'AVAX/USDT', 'LINK/USDT', 'SUI/USDT', 'PEPE/USDT'
    ]
    last_alerts = {}

    while True:
        try:
            await asyncio.sleep(60)
            if not system_settings["pump_detector_enabled"]:
                continue

            now_ts = datetime.datetime.now().timestamp()
            exchange = ccxt.coinex()

            try:
                for symbol in tracked_symbols:
                    try:
                        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=21)
                        if len(ohlcv) < 21:
                            continue

                        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                        last_candle = df.iloc[-1]
                        prev_candles = df.iloc[:-1]

                        avg_vol = prev_candles['Volume'].mean()
                        current_vol = last_candle['Volume']
                        price_change_pct = ((last_candle['Close'] - last_candle['Open']) / last_candle['Open']) * 100

                        is_volume_spike = (current_vol >= avg_vol * 4) and (avg_vol > 0)

                        alert_type = None
                        if is_volume_spike and price_change_pct >= 1.2:
                            alert_type = "🚀 پامپ احتمالی (Pump Alert)"
                        elif is_volume_spike and price_change_pct <= -1.2:
                            alert_type = "🩸 دامپ احتمالی (Dump Alert)"

                        if alert_type:
                            if symbol in last_alerts and (now_ts - last_alerts[symbol]) < 600:
                                continue

                            last_alerts[symbol] = now_ts
                            clean_sym = symbol.replace('/', '')
                            alert_msg = (
                                f"🚨 **هشدار هوشمند رادار بازار!**\n\n"
                                f"🪙 **نماد:** {clean_sym}\n"
                                f"📊 **نوع هشدار:** {alert_type}\n"
                                f"📈 **تغییر قیمت ۵ دقیقه:** `{price_change_pct:+.2f}%`\n"
                                f"⚡️ **جهش حجم:** `{current_vol/avg_vol:.1f}X` برابر میانگین!\n"
                                f"💵 **قیمت فعلی:** `${last_candle['Close']}`"
                            )

                            for u_id, u_info in list(user_data.items()):
                                if u_info.get("is_vip", False) and u_id not in banned_users:
                                    try:
                                        await bot.send_message(u_id, alert_msg, parse_mode="Markdown")
                                        await asyncio.sleep(0.05)
                                    except Exception:
                                        pass
                    except Exception as e:
                        logging.error(f"Error checking {symbol}: {e}")
            finally:
                try:
                    await exchange.close()
                except Exception:
                    pass

        except Exception as e:
            logging.error(f"Pump/Dump Loop Error: {type(e).__name__}: {e}")


# --- بولتن روزانه ---
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
    prompt = (
        f"یک خلاصه بسیار کوتاه (حداکثر ۳ سطر) از مهم‌ترین اخبار کریپتو ارائه بده:\n{raw_news}\n\n"
        "⚠️ توضیحات را فارسی بنویس، اما نام ارزها و اصطلاحات (ETF, DeFi, Whale, Halving) را انگلیسی نگه دار."
    )
    try:
        news_summary = await query_gemini(prompt)
    except Exception:
        news_summary = "تغییرات شدید نوسانی در بازار مشاهده می‌شود."

    return (
        f"☀️ **بولتن تحلیلی روزانه AlphaEngine**\n\n"
        f"🪙 **BTC:** `${btc_price:,.2f}` (`{btc_change:+.2f}%`)\n"
        f"📊 **شاخص ترس و طمع:** {fng_val}/100 ({fng_class})\n\n"
        f"📰 **خلاصه اخبار:**\n{news_summary}\n\n"
        f"💡 برای تحلیل کامل، نام ارز یا ویس خود را بفرستید."
    )


async def daily_digest_scheduler():
    while True:
        await asyncio.sleep(60)
        if not system_settings["daily_digest_enabled"]:
            continue
        now = datetime.datetime.now()
        if now.hour == 8 and now.minute == 0:
            digest_msg = await generate_daily_digest()
            for u_id in list(user_data.keys()):
                if u_id in banned_users:
                    continue
                try:
                    await bot.send_message(u_id, digest_msg, parse_mode="Markdown")
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logging.warning(f"Failed to send digest to {u_id}: {e}")
            await asyncio.sleep(300)


# --- هشدار قیمت پس‌زمینه ---
async def background_alert_checker():
    while True:
        try:
            await asyncio.sleep(30)
            if not price_alerts:
                continue

            exchange = ccxt.coinex()
            try:
                for alert in price_alerts[:]:
                    symbol = alert['symbol']
                    target = alert['target_price']
                    condition = alert['condition']

                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker.get('last') or ticker.get('close')

                        if ((condition == "above" and current_price >= target)
                                or (condition == "below" and current_price <= target)):
                            await bot.send_message(
                                chat_id=alert['user_id'],
                                text=(
                                    f"🚨 **هشدار قیمت رسید!**\n\n"
                                    f"🪙 ارز: **{symbol}**\n"
                                    f"🎯 هدف: `${target}`\n"
                                    f"💵 فعلی: `${current_price}`"
                                ),
                                parse_mode="Markdown"
                            )
                            price_alerts.remove(alert)
                    except Exception as e:
                        logging.error(f"Alert Check Error: {type(e).__name__}: {e}")
            finally:
                try:
                    await exchange.close()
                except Exception:
                    pass
        except Exception as e:
            logging.error(f"Alert Loop Error: {type(e).__name__}: {e}")


# --- وب سرور برای Render ---
async def handle_web(request):
    return web.Response(text="AlphaEngine Pro Active!")


app = web.Application()
app.router.add_get('/', handle_web)


# --- main ---
async def main():
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    asyncio.create_task(background_alert_checker())
    asyncio.create_task(daily_digest_scheduler())
    asyncio.create_task(pump_dump_detector_loop())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
