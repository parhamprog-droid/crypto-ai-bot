import os
import asyncio
import logging
import io
import time
import re
import random
import string
import asyncpg
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

# --- تنظیمات لاگینگ امن ---
class SensitiveFilter(logging.Filter):
    SENSITIVE_PATTERNS = [
        (re.compile(r'(postgresql://[^:]+:)[^@]+(@)'), r'\1***\2'),
        (re.compile(r'(bot\d*:)[A-Za-z0-9_\-]+'), r'\1***'),
        (re.compile(r'(AIza[A-Za-z0-9_\-]+)'), r'***'),
        (re.compile(r'(AQ\.[A-Za-z0-9_\-]+)'), r'***'),
        (re.compile(r'(npg_[A-Za-z0-9]+)'), r'***'),
    ]
    def filter(self, record):
        try:
            msg = record.getMessage()
            for pattern, repl in self.SENSITIVE_PATTERNS:
                msg = pattern.sub(repl, msg)
            record.msg = msg
            record.args = ()
        except Exception:
            pass
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
for handler in logging.root.handlers:
    handler.addFilter(SensitiveFilter())

# --- خواندن امن ENV ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0")
DATABASE_URL = os.getenv("DATABASE_URL")
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "0000-0000-0000-0000")
PAYMENT_HOLDER = os.getenv("PAYMENT_HOLDER", "نام صاحب کارت")
PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "100000"))
VIP_PRICE_TOMAN = os.getenv("VIP_PRICE_TOMAN", "100,000")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@AlphaEngine_Official")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "https://t.me/AlphaEngine_Official")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("❌ ENV 'TELEGRAM_BOT_TOKEN' or 'BOT_TOKEN' is not set!")
if not GEMINI_API_KEY:
    raise RuntimeError("❌ ENV 'GEMINI_API_KEY' is not set!")
if not DATABASE_URL:
    raise RuntimeError("❌ ENV 'DATABASE_URL' is not set!")

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

# --- Database Pool ---
db_pool: asyncpg.Pool = None

# --- State های RAM ---
user_cache = {}
admin_state = {}
rate_limit = {}
market_cache = {}
_AVAILABLE_GEMINI_MODELS = None

# --- تنظیمات ---
STATE_TIMEOUT_SECONDS = 300
RATE_LIMIT_PER_MINUTE = 5
MARKET_CACHE_TTL = 60
MIN_PUMP_VOLUME_USD = 500000
VIP_DURATION_DAYS = 30
FREE_VIP_DAYS = 7
POINTS_FOR_VIP = 10
POINTS_PER_ANALYSIS = 1
POINTS_PER_REFERRAL = 5
SYSTEM_SETTINGS = {
    "pump_detector_enabled": True,
    "daily_digest_enabled": True,
    "channel_broadcast_enabled": True,
}

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


# ============================================================
# ============ توابع پایگاه داده PostgreSQL ==================
# ============================================================

async def init_db():
    global db_pool
    try:
        clean_url = DATABASE_URL
        if "?" in clean_url:
            base_url, query = clean_url.split("?", 1)
            params = [p for p in query.split("&") if not p.startswith("channel_binding=")]
            clean_url = base_url + "?" + "&".join(params) if params else base_url

        db_pool = await asyncpg.create_pool(
            clean_url, min_size=1, max_size=5,
            command_timeout=60, ssl='require'
        )

        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    is_vip BOOLEAN DEFAULT FALSE,
                    vip_until TIMESTAMP,
                    referred_by BIGINT,
                    points INTEGER DEFAULT 0,
                    usage_count INTEGER DEFAULT 0,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_seen TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS referrals (
                    referrer_id BIGINT,
                    referred_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS price_alerts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    symbol TEXT,
                    target_price DOUBLE PRECISION,
                    condition TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id BIGINT PRIMARY KEY,
                    banned_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    action TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS system_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS payment_requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    amount INTEGER,
                    status TEXT DEFAULT 'pending',
                    receipt_file_id TEXT,
                    admin_note TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    reviewed_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS discount_codes (
                    code TEXT PRIMARY KEY,
                    created_by BIGINT,
                    used_by BIGINT,
                    used_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    is_used BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_alerts_user ON price_alerts(user_id);
                CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen);
                CREATE INDEX IF NOT EXISTS idx_payment_status ON payment_requests(status);
            """)

            # ✅ Migration: اضافه کردن ستون‌های جدید به جدول‌های موجود
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_until TIMESTAMP;")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS points INTEGER DEFAULT 0;")
                logging.info("✅ Migrations applied successfully")
            except Exception as e:
                logging.warning(f"Migration warning: {e}")

            for k, v in SYSTEM_SETTINGS.items():
                await conn.execute("""
                    INSERT INTO system_settings (key, value)
                    VALUES ($1, $2)
                    ON CONFLICT (key) DO NOTHING
                """, k, str(v).lower())

        logging.info("✅ Database connected and initialized")
    except Exception as e:
        logging.error(f"❌ DB Init Error: {type(e).__name__}: {e}")
        raise


async def db_get_or_create_user(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not row:
            await conn.execute(
                "INSERT INTO users (user_id, is_vip) VALUES ($1, $2)",
                user_id, (user_id == ADMIN_ID and ADMIN_ID != 0)
            )
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        else:
            await conn.execute("UPDATE users SET last_seen = NOW() WHERE user_id = $1", user_id)

        is_vip = row["is_vip"]
        vip_until = row.get("vip_until")
        if is_vip and vip_until and vip_until < datetime.datetime.now():
            is_vip = False
            await conn.execute("UPDATE users SET is_vip = FALSE WHERE user_id = $1", user_id)

        referrals = await conn.fetch(
            "SELECT referred_id FROM referrals WHERE referrer_id = $1", user_id
        )
        return {
            "user_id": row["user_id"],
            "is_vip": is_vip,
            "vip_until": vip_until,
            "referred_by": row.get("referred_by"),
            "points": row.get("points") or 0,
            "usage_count": row.get("usage_count") or 0,
            "referrals": [r["referred_id"] for r in referrals],
        }


async def db_set_vip(user_id: int, is_vip: bool, days: int = 0):
    async with db_pool.acquire() as conn:
        if is_vip and days > 0:
            until = datetime.datetime.now() + datetime.timedelta(days=days)
            await conn.execute(
                "UPDATE users SET is_vip = TRUE, vip_until = $1 WHERE user_id = $2",
                until, user_id
            )
        else:
            await conn.execute(
                "UPDATE users SET is_vip = $1, vip_until = NULL WHERE user_id = $2",
                is_vip, user_id
            )


async def db_extend_vip(user_id: int, days: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT vip_until FROM users WHERE user_id = $1", user_id)
        now = datetime.datetime.now()
        base = row["vip_until"] if (row and row["vip_until"] and row["vip_until"] > now) else now
        new_until = base + datetime.timedelta(days=days)
        await conn.execute(
            "UPDATE users SET is_vip = TRUE, vip_until = $1 WHERE user_id = $2",
            new_until, user_id
        )
        return new_until


async def db_is_vip(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_vip, vip_until FROM users WHERE user_id = $1", user_id)
        if not row or not row["is_vip"]:
            return False
        if row["vip_until"] and row["vip_until"] < datetime.datetime.now():
            await conn.execute("UPDATE users SET is_vip = FALSE WHERE user_id = $1", user_id)
            return False
        return True


async def db_add_points(user_id: int, points: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET points = COALESCE(points, 0) + $1 WHERE user_id = $2",
            points, user_id
        )


async def db_get_points(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT points FROM users WHERE user_id = $1", user_id)
        return (row["points"] if row else 0) or 0


async def db_deduct_points(user_id: int, points: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT points FROM users WHERE user_id = $1", user_id)
        current = (row["points"] if row else 0) or 0
        if current < points:
            return False
        await conn.execute(
            "UPDATE users SET points = points - $1 WHERE user_id = $2",
            points, user_id
        )
        return True


async def db_add_referral(referrer_id: int, referred_id: int):
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM referrals WHERE referred_id = $1", referred_id)
        if existing:
            return None
        await conn.execute(
            "INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)",
            referrer_id, referred_id
        )
        await conn.execute(
            "UPDATE users SET referred_by = $1 WHERE user_id = $2 AND referred_by IS NULL",
            referrer_id, referred_id
        )
        await conn.execute(
            "UPDATE users SET points = COALESCE(points, 0) + $1 WHERE user_id = $2",
            POINTS_PER_REFERRAL, referrer_id
        )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = $1", referrer_id
        )
        return {"count": count, "new_points": POINTS_PER_REFERRAL}


async def db_add_alert(user_id: int, symbol: str, target: float, condition: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO price_alerts (user_id, symbol, target_price, condition)
            VALUES ($1, $2, $3, $4)
        """, user_id, symbol, target, condition)


async def db_get_user_alerts(user_id: int) -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, symbol, target_price, condition FROM price_alerts WHERE user_id = $1 ORDER BY created_at DESC",
            user_id
        )
        return [dict(r) for r in rows]


async def db_get_all_alerts() -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, user_id, symbol, target_price, condition FROM price_alerts ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]


async def db_delete_alert(alert_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM price_alerts WHERE id = $1", alert_id)


async def db_clear_all_alerts():
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM price_alerts")
        await conn.execute("DELETE FROM price_alerts")
        return count


async def db_ban_user(user_id: int, banned: bool = True):
    async with db_pool.acquire() as conn:
        if banned:
            await conn.execute(
                "INSERT INTO banned_users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                user_id
            )
        else:
            await conn.execute("DELETE FROM banned_users WHERE user_id = $1", user_id)


async def db_is_banned(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM banned_users WHERE user_id = $1", user_id)
        return row is not None


async def db_get_banned_users() -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM banned_users")
        return [r["user_id"] for r in rows]


async def db_log_admin(admin_id: int, action: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO admin_logs (admin_id, action) VALUES ($1, $2)",
            admin_id, action
        )


async def db_get_admin_logs(limit: int = 20) -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT admin_id, action, created_at FROM admin_logs ORDER BY created_at DESC LIMIT $1",
            limit
        )
        return [dict(r) for r in rows]


async def db_get_all_users(limit: int = 20, order: str = "last_seen") -> list:
    async with db_pool.acquire() as conn:
        if order == "vip":
            rows = await conn.fetch(
                "SELECT * FROM users WHERE is_vip = TRUE ORDER BY last_seen DESC LIMIT $1", limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM users ORDER BY last_seen DESC LIMIT $1", limit
            )
        return [dict(r) for r in rows]


async def db_count_users() -> dict:
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        vips = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_vip = TRUE")
        banned = await conn.fetchval("SELECT COUNT(*) FROM banned_users")
        today = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE last_seen >= CURRENT_DATE"
        )
        return {"total": total, "vip": vips, "banned": banned, "today": today}


async def db_get_settings() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM system_settings")
        return {r["key"]: (r["value"] == "true") for r in rows}


async def db_set_setting(key: str, value: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO system_settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        """, key, str(value).lower())


# --- Payment Requests ---
async def db_create_payment_request(user_id: int, amount: int, receipt_file_id: str) -> int:
    async with db_pool.acquire() as conn:
        rid = await conn.fetchval("""
            INSERT INTO payment_requests (user_id, amount, receipt_file_id)
            VALUES ($1, $2, $3) RETURNING id
        """, user_id, amount, receipt_file_id)
        return rid


async def db_get_payment_request(req_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM payment_requests WHERE id = $1", req_id)
        return dict(row) if row else None


async def db_update_payment_request(req_id: int, status: str, note: str = None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE payment_requests
            SET status = $1, admin_note = $2, reviewed_at = NOW()
            WHERE id = $3
        """, status, note, req_id)


async def db_get_pending_payments() -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM payment_requests WHERE status = 'pending' ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]


# --- Discount Codes ---
async def db_create_discount_code(created_by: int, days_valid: int = 7) -> str:
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    expires = datetime.datetime.now() + datetime.timedelta(days=days_valid)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO discount_codes (code, created_by, expires_at)
            VALUES ($1, $2, $3)
        """, code, created_by, expires)
    return code


async def db_redeem_code(code: str, user_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM discount_codes WHERE code = $1", code.upper())
        if not row:
            return {"ok": False, "msg": "کد نامعتبر است."}
        if row["is_used"]:
            return {"ok": False, "msg": "این کد قبلاً استفاده شده."}
        if row["expires_at"] and row["expires_at"] < datetime.datetime.now():
            return {"ok": False, "msg": "این کد منقضی شده."}
        await conn.execute("""
            UPDATE discount_codes
            SET is_used = TRUE, used_by = $1, used_at = NOW()
            WHERE code = $2
        """, user_id, code.upper())
    return {"ok": True, "days": FREE_VIP_DAYS}


async def db_get_codes(limit: int = 20) -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM discount_codes ORDER BY created_at DESC LIMIT $1", limit
        )
        return [dict(r) for r in rows]

# ============================================================
# ==================== توابع کمکی ===========================
# ============================================================

def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def escape_md(text: str) -> str:
    if not text:
        return ""
    for ch in ['_', '*', '`', '[']:
        text = text.replace(ch, f"\\{ch}")
    return text


def chunk_text(text: str, size: int = 4000) -> list:
    if len(text) <= size:
        return [text]
    chunks = []
    while text:
        if len(text) <= size:
            chunks.append(text)
            break
        cut = text.rfind('\n', 0, size)
        if cut == -1:
            cut = size
        chunks.append(text[:cut])
        text = text[cut:].lstrip('\n')
    return chunks


def check_rate_limit(user_id: int) -> bool:
    now = time.time()
    user_times = rate_limit.get(user_id, [])
    user_times = [t for t in user_times if now - t < 60]
    if len(user_times) >= RATE_LIMIT_PER_MINUTE:
        return False
    user_times.append(now)
    rate_limit[user_id] = user_times
    return True


def check_state_timeout(user_id: int) -> bool:
    st = user_cache.get(user_id, {}).get("state_updated")
    if not st:
        return True
    if time.time() - st > STATE_TIMEOUT_SECONDS:
        user_cache[user_id]["state"] = None
        user_cache[user_id]["state_updated"] = None
        return False
    return True


def set_user_state(user_id: int, state):
    if user_id not in user_cache:
        user_cache[user_id] = {}
    user_cache[user_id]["state"] = state
    user_cache[user_id]["state_updated"] = time.time()


def cache_get(key: str):
    entry = market_cache.get(key)
    if entry and entry["expires"] > time.time():
        return entry["data"]
    return None


def cache_set(key: str, data, ttl: int = MARKET_CACHE_TTL):
    market_cache[key] = {"data": data, "expires": time.time() + ttl}


# ============================================================
# ================ لیست داینامیک Gemini ======================
# ============================================================

async def get_available_models():
    global _AVAILABLE_GEMINI_MODELS
    if _AVAILABLE_GEMINI_MODELS is not None:
        return _AVAILABLE_GEMINI_MODELS
    preferred = ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.0-flash", "gemini-1.5-flash", "gemini-2.0-flash-lite"]
    try:
        response = await asyncio.to_thread(gemini_client.models.list)
        dynamic = []
        for m in response:
            try:
                if hasattr(m, "supported_actions") and m.supported_actions:
                    if "generateContent" in m.supported_actions:
                        dynamic.append(m.name.replace("models/", ""))
                elif hasattr(m, "supported_generation_methods") and m.supported_generation_methods:
                    if "generateContent" in m.supported_generation_methods:
                        dynamic.append(m.name.replace("models/", ""))
            except Exception:
                continue
        combined = list(dict.fromkeys(preferred + dynamic))
        _AVAILABLE_GEMINI_MODELS = combined
        logging.info(f"✅ Available Gemini models: {combined[:5]}...")
        return combined
    except Exception as e:
        logging.warning(f"ListModels failed: {e}")
        _AVAILABLE_GEMINI_MODELS = preferred
        return preferred


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
            logging.warning(f"Gemini '{model_name}' failed: {type(e).__name__}")
            if "404" in err_str or "NOT_FOUND" in err_str:
                global _AVAILABLE_GEMINI_MODELS
                if _AVAILABLE_GEMINI_MODELS and model_name in _AVAILABLE_GEMINI_MODELS:
                    _AVAILABLE_GEMINI_MODELS.remove(model_name)
            continue
    raise last_error or Exception("هیچ‌کدام از مدل‌های Gemini پاسخ ندادند.")


# ============================================================
# ==================== کیبوردها ==============================
# ============================================================

def get_main_keyboard(user_id: int):
    """کیبورد اصلی کاربر عادی"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
            [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
            [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
            [KeyboardButton(text="💎 خرید VIP"), KeyboardButton(text="🎁 کد اشتراک")],
            [KeyboardButton(text="⭐ امتیاز من"), KeyboardButton(text="👤 حساب کاربری")],
            [KeyboardButton(text="📢 کانال ما"), KeyboardButton(text="👥 سیستم دعوت و هدیه")],
        ],
        resize_keyboard=True
    )


def get_admin_keyboard(user_id: int):
    """کیبورد ادمین — بدون امتیاز و دعوت، با پنل ادمین در ردیف آخر تنها"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
            [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
            [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
            [KeyboardButton(text="💎 خرید VIP"), KeyboardButton(text="🎁 کد اشتراک")],
            [KeyboardButton(text="📢 کانال ما"), KeyboardButton(text="👤 حساب کاربری")],
            [KeyboardButton(text="⚙️ پنل ادمین")],
        ],
        resize_keyboard=True
    )


def get_user_kb(user_id: int):
    return get_admin_keyboard(user_id) if is_admin(user_id) else get_main_keyboard(user_id)


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
        [InlineKeyboardButton(text="💎 خرید VIP با کارت به کارت", callback_data="vip:buy")],
        [InlineKeyboardButton(text="⭐ خرید VIP با امتیاز", callback_data="vip:points")],
    ])


def payment_approval_keyboard(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ تأیید", callback_data=f"pay:approve:{req_id}"),
            InlineKeyboardButton(text="❌ رد", callback_data=f"pay:reject:{req_id}"),
        ],
        [InlineKeyboardButton(text="⏸ بعداً", callback_data=f"pay:later:{req_id}")],
    ])


def admin_panel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin:stats"),
            InlineKeyboardButton(text="👥 کاربران", callback_data="admin:users_menu"),
        ],
        [
            InlineKeyboardButton(text="💳 درخواست‌های VIP", callback_data="admin:payments"),
            InlineKeyboardButton(text="🎁 کدهای اشتراک", callback_data="admin:codes_menu"),
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


def admin_codes_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ساخت کد جدید", callback_data="admin:code_new")],
        [InlineKeyboardButton(text="📋 لیست کدها", callback_data="admin:code_list")],
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
    pump_status = "🟢 روشن" if SYSTEM_SETTINGS["pump_detector_enabled"] else "🔴 خاموش"
    digest_status = "🟢 روشن" if SYSTEM_SETTINGS["daily_digest_enabled"] else "🔴 خاموش"
    channel_status = "🟢 روشن" if SYSTEM_SETTINGS.get("channel_broadcast_enabled", True) else "🔴 خاموش"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚀 رادار پامپ/دامپ: {pump_status}", callback_data="admin:toggle_pump")],
        [InlineKeyboardButton(text=f"☀️ بولتن روزانه: {digest_status}", callback_data="admin:toggle_digest")],
        [InlineKeyboardButton(text=f"📢 ارسال به کانال: {channel_status}", callback_data="admin:toggle_channel")],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back")],
    ])


def admin_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 بازگشت به پنل", callback_data="admin:back")]
    ])


def vip_action_keyboard(target_uid: int, is_vip: bool, is_banned: bool):
    rows = []
    if is_vip:
        rows.append([InlineKeyboardButton(text="🔻 لغو VIP", callback_data=f"admin:unvip:{target_uid}")])
    else:
        rows.append([InlineKeyboardButton(text="💎 ارتقا به VIP (30 روز)", callback_data=f"admin:setvip:{target_uid}")])
    if is_banned:
        rows.append([InlineKeyboardButton(text="✅ آنبن", callback_data=f"admin:unban:{target_uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 بن کردن", callback_data=f"admin:ban:{target_uid}")])
    rows.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:users_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ============================================================
# ==================== توابع داده بازار ======================
# ============================================================

async def get_crypto_dataframe(symbol="BTC/USDT", timeframe="1h", limit=100):
    cache_key = f"df:{symbol}:{timeframe}:{limit}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

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
        df['FVG_Bearish'] = (df['High'] < df['Low'].shift(2))
        df['OrderBlock_Bullish'] = (
            (df['Close'] > df['Open'])
            & (df['Close'].shift(1) < df['Open'].shift(1))
            & (df['Volume'] > df['Volume'].rolling(10).mean() * 1.5)
        )
        df['OrderBlock_Bearish'] = (
            (df['Close'] < df['Open'])
            & (df['Close'].shift(1) > df['Open'].shift(1))
            & (df['Volume'] > df['Volume'].rolling(10).mean() * 1.5)
        )

        result = (formatted_symbol, df)
        cache_set(cache_key, result, MARKET_CACHE_TTL)
        return result
    except Exception as e:
        logging.error(f"CCXT Error ({symbol}): {type(e).__name__}")
        return None, None
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def fetch_orderbook_and_futures(symbol="BTC/USDT"):
    cache_key = f"ob:{symbol}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    exchange = ccxt.coinex()
    try:
        formatted_symbol = symbol.upper()
        orderbook = await exchange.fetch_order_book(formatted_symbol, limit=20)
        bids_volume = sum([b[1] for b in orderbook['bids']])
        asks_volume = sum([a[1] for a in orderbook['asks']])
        orderbook_ratio = bids_volume / asks_volume if asks_volume > 0 else 1.0
        result = {
            "bids_vol": bids_volume,
            "asks_vol": asks_volume,
            "ratio": orderbook_ratio,
            "open_interest": "افزایشی 📈" if bids_volume > asks_volume else "کاهشی 📉"
        }
        cache_set(cache_key, result, MARKET_CACHE_TTL)
        return result
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
            logging.error(f"News Error: {e}")
    return "- Title: Crypto Market Volatility\n  Body: High volatility continues.\n\n"


async def scan_pump_candidates():
    exchange = ccxt.coinex()
    try:
        tickers = await exchange.fetch_tickers()
        candidates = []
        for symbol, data in tickers.items():
            if symbol.endswith("/USDT"):
                volume = data.get('quoteVolume') or 0
                change = data.get('percentage') or 0
                if volume >= MIN_PUMP_VOLUME_USD and change >= 2.0:
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


async def get_ticker_price(symbol: str) -> float:
    exchange = ccxt.coinex()
    try:
        ticker = await exchange.fetch_ticker(symbol.upper())
        return float(ticker.get('last') or ticker.get('close') or 0)
    except Exception:
        return 0.0
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def send_to_channel(text: str, photo_bytes: bytes = None):
    """ارسال پیام به کانال (اگه فعال باشه)"""
    try:
        settings = await db_get_settings()
        if not settings.get("channel_broadcast_enabled", True):
            return
        if photo_bytes:
            await bot.send_photo(
                chat_id=CHANNEL_USERNAME,
                photo=BufferedInputFile(photo_bytes, filename="chart.png"),
                caption=text[:1024],
                parse_mode="Markdown"
            )
        else:
            await bot.send_message(
                chat_id=CHANNEL_USERNAME,
                text=text,
                parse_mode="Markdown"
            )
    except Exception as e:
        logging.warning(f"Channel broadcast failed: {type(e).__name__}: {e}")


# ============================================================
# ================ چارت با FVG و OB =========================
# ============================================================

def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    clean_symbol = symbol.replace("/", "")
    fig, (ax_main, ax_rsi) = plt.subplots(
        2, 1, figsize=(12, 7),
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

    for i in range(2, n):
        if df['FVG_Bullish'].iloc[i]:
            ax_main.axhspan(df['High'].iloc[i - 2], df['Low'].iloc[i], color='#089981', alpha=0.08, zorder=0)
    for i in range(2, n):
        if df['FVG_Bearish'].iloc[i]:
            ax_main.axhspan(df['High'].iloc[i], df['Low'].iloc[i - 2], color='#f23645', alpha=0.08, zorder=0)

    for i in range(1, n):
        if df['OrderBlock_Bullish'].iloc[i]:
            ax_main.axhline(y=df['Low'].iloc[i], color='#00C853', linestyle=':', linewidth=1.2, alpha=0.7)
    for i in range(1, n):
        if df['OrderBlock_Bearish'].iloc[i]:
            ax_main.axhline(y=df['High'].iloc[i], color='#D50000', linestyle=':', linewidth=1.2, alpha=0.7)

    ax_main.plot(range(n), df['EMA_50'], color='#2196F3', linewidth=1.2, label='EMA 50')
    ax_main.plot(range(n), df['EMA_200'], color='#FF9800', linewidth=1.2, label='EMA 200')

    last_price = df['Close'].iloc[-1]
    ax_main.axhline(y=last_price, color='red', linestyle='--', linewidth=1)
    ax_main.text(n - 1, last_price, f" {last_price:.4f}", color='white', backgroundcolor='red',
                 fontsize=8, fontweight='bold', va='center')

    ax_main.grid(True, linestyle='--', alpha=0.4, color='#e0e0e0')
    ax_main.set_title(f"{clean_symbol} {timeframe} - Multi-Timeframe AlphaEngine Pro",
                      fontsize=12, fontweight='bold', pad=10)
    ax_main.legend(loc='upper left', fontsize=8)
    ax_main.yaxis.tick_right()

    ax_rsi.plot(range(n), df['RSI'], color='#8a2be2', linewidth=1.2)
    ax_rsi.axhline(70, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.axhline(30, color='gray', linestyle='--', linewidth=0.8)
    ax_rsi.fill_between(range(n), 30, 70, color='#e6e6fa', alpha=0.4)
    ax_rsi.set_ylim(0, 100)
    ax_rsi.grid(True, linestyle='--', alpha=0.4, color='#e0e0e0')
    ax_rsi.yaxis.tick_right()

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ============================================================
# ================== تحلیل سیگنال ===========================
# ============================================================

async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return f"⚠️ ارز **{escape_md(symbol)}** پیدا نشد.", None

    results = await asyncio.gather(
        get_crypto_dataframe(symbol, "1d", 30),
        get_crypto_dataframe(symbol, "4h", 30),
        get_crypto_dataframe("BTC/USDT", "1h", 30),
        fetch_orderbook_and_futures(formatted_symbol),
        return_exceptions=True,
    )
    (_, df_1d), (_, df_4h), (_, df_btc), ob_data = results

    if isinstance(df_1d, Exception): df_1d = None
    if isinstance(df_4h, Exception): df_4h = None
    if isinstance(df_btc, Exception): df_btc = None
    if isinstance(ob_data, Exception): ob_data = {"ratio": 1.0}

    trend_1d = "صعودی 🟢" if (df_1d is not None and df_1d['Close'].iloc[-1] > df_1d['EMA_50'].iloc[-1]) else "نزولی 🔴"
    trend_4h = "صعودی 🟢" if (df_4h is not None and df_4h['Close'].iloc[-1] > df_4h['EMA_50'].iloc[-1]) else "نزولی 🔴"
    btc_bullish = (df_btc is not None and df_btc['Close'].iloc[-1] > df_btc['EMA_50'].iloc[-1])

    df['TR'] = np.maximum(df['High'] - df['Low'], np.maximum(
        abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))
    ))
    df['ATR'] = df['TR'].rolling(window=14).mean()
    atr_val = round(float(df['ATR'].iloc[-1]), 4)
    atr_for_sl = round(atr_val * 1.5, 4)
    atr_sl_max = round(atr_val * 3, 4)

    is_squeeze = df['BB_Width'].iloc[-1] < df['BB_Width'].rolling(30).mean().iloc[-1] * 0.7
    squeeze_status = "⚠️ فشرده‌سازی نوسان (آماده‌باش انفجار قیمت 🔥)" if is_squeeze else "عادی 🟢"

    price = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    has_fvg = df['FVG_Bullish'].iloc[-3:].any()
    has_ob = df['OrderBlock_Bullish'].iloc[-5:].any()

    prompt = f"""
تو مدیر ارشد ریسک یک هج‌فاند کریپتو هستی. یک ستاپ فوق‌پیشرفته موسسه‌ای برای {formatted_symbol} در تایم‌فریم {timeframe} صادر کن.

همگرایی روندهای تایم‌فریم بالاتر:
- روند دیلی (1D): {trend_1d}
- روند چهارساعته (4H): {trend_4h}
- وضعیت کلان بیت‌کوین: {"صعودی 🟢" if btc_bullish else "نزولی 🔴"}

داده‌های فنی و ICT:
- قیمت فعلی: {price} | ATR: {atr_val}
- محدوده مجاز SL بر اساس ATR: حداقل {atr_for_sl} و حداکثر {atr_sl_max} از Entry
- وضعیت نوسان: {squeeze_status}
- FVG خریداران: {has_fvg} | Order Block: {has_ob}
- نسبت سفارشات خرید/فروش: {ob_data.get('ratio', 1.0):.2f} | RSI: {rsi:.2f}

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

⚠️ یادآوری:
- توضیحات را فارسی روان بنویس.
- اصطلاحات تکنیکال (RSI, MACD, EMA, ATR, FVG, OB, Bollinger, Long, Short, Entry, TP, SL) را انگلیسی نگه دار.
- با ⚡️ شروع کن. هرگز # استفاده نکن. املای فارسی دقیق.
- SL بین 1.5×ATR و 3×ATR از Entry.
- Entry Zone حداکثر ۲٪ از قیمت فعلی.
"""

    try:
        response_text = await query_gemini(prompt)
    except Exception as e:
        return f"⚠️ خطا در تحلیل Gemini:\n`{escape_md(str(e))}`", None

    chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    return response_text, chart_bytes

# ============================================================
# =================== پردازش پیام صوتی ======================
# ============================================================

@dp.message(F.voice)
async def handle_voice_message(message: types.Message):
    user_id = message.from_user.id
    if await db_is_banned(user_id):
        await message.answer("🚫 شما از استفاده از بات محروم شده‌اید.")
        return
    if not check_rate_limit(user_id):
        await message.answer("⏱ لطفاً کمی صبر کنید. حداکثر ۵ درخواست در دقیقه.")
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
                "این یک فایل صوتی از کاربر در مورد بازار کریپتو است. "
                "متن صحبت او را متوجه شو، سوال یا درخواست او را بررسی کن و پاسخ جامع بده.\n\n"
                "⚠️ توضیحات فارسی، اصطلاحات تکنیکال انگلیسی."
            )
            models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash", "gemini-2.5-pro"]
            last_exc = None
            for m_name in models_to_try:
                try:
                    resp = gemini_client.models.generate_content(
                        model=m_name, contents=[uploaded, prompt],
                        config=genai_types.GenerateContentConfig(system_instruction=PERSIAN_SYSTEM_INSTRUCTION),
                    )
                    if resp and resp.text:
                        return resp.text
                except Exception as ex:
                    last_exc = ex
                    continue
            if last_exc: raise last_exc
            return None

        response_text = await asyncio.to_thread(_upload_and_analyze)
        if response_text:
            chunks = chunk_text(f"🗣 **پاسخ دستیار صوتی:**\n\n{response_text}")
            try:
                await msg.edit_text(chunks[0], parse_mode="Markdown")
                for c in chunks[1:]:
                    await message.answer(c, parse_mode="Markdown")
            except Exception:
                await msg.edit_text(chunks[0])
                for c in chunks[1:]:
                    await message.answer(c)
        else:
            await msg.edit_text("⚠️ متنی از فایل صوتی تشخیص داده نشد.")
    except Exception as e:
        logging.error(f"Voice error: {type(e).__name__}")
        await msg.edit_text(f"⚠️ خطا در پردازش فایل صوتی:\n`{escape_md(str(e))}`", parse_mode="Markdown")
    finally:
        for path in [ogg_filename, wav_filename]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


# ============================================================
# ==================== دستورات پایه =========================
# ============================================================

@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    await message.answer(
        "📚 **راهنمای بات AlphaEngine**\n\n"
        "🔹 **تحلیل ارز:** نام نماد رو بفرست (مثل `BTC`)\n"
        "🔹 **تحلیل صوتی:** یه ویس بفرست\n"
        "🔹 **هشدار قیمت:** دکمه `🔔 هشدار قیمت`\n"
        "🔹 **محاسبه ریسک:** دکمه `🧮 محاسبه ریسک`\n"
        "🔹 **خرید VIP:** دکمه `💎 خرید VIP`\n"
        "🔹 **کد اشتراک:** دکمه `🎁 کد اشتراک`\n"
        "🔹 **امتیاز:** دکمه `⭐ امتیاز من`\n\n"
        "⚙️ **دستورات:**\n"
        "`/start` — شروع\n"
        "`/help` — راهنما\n"
        "`/cancel` — لغو\n"
        "`/admin` — پنل ادمین (فقط ادمین)",
        parse_mode="Markdown"
    )


@dp.message(Command("cancel"))
async def cancel_cmd(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_cache:
        user_cache[user_id]["state"] = None
        user_cache[user_id]["state_updated"] = None
    if user_id in admin_state:
        admin_state.pop(user_id, None)
    await message.answer("✅ عملیات لغو شد.", reply_markup=get_user_kb(user_id))


@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    if await db_is_banned(user_id):
        await message.answer("🚫 شما از استفاده از بات محروم شده‌اید.")
        return

    user = await db_get_or_create_user(user_id)
    set_user_state(user_id, None)

    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
        if referrer_id != user_id and user["referred_by"] is None:
            result = await db_add_referral(referrer_id, user_id)
            if result:
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 یکی از دوستانت با لینک تو اومد!\n"
                        f"⭐ +{POINTS_PER_REFERRAL} امتیاز گرفتی.\n"
                        f"👥 تعداد دعوت‌ها: {result['count']}"
                    )
                except Exception:
                    pass

    user = await db_get_or_create_user(user_id)
    status_text = "✨ VIP" if user["is_vip"] else "Standard 🔑"

    await message.answer(
        f"🏛 **AlphaEngine Terminal Pro**\n"
        f"────────────────────────\n\n"
        f"به ترمینال تخصصی تحلیل الگوریتمی بازار کریپتو خوش آمدید.\n\n"
        f"🔰 **وضعیت حساب:** `{status_text}`\n"
        f"📡 **وضعیت اتصال:** آنلاین 🟢\n\n"
        f"💡 **راهنمای سریع:**\n"
        f"برای دریافت ستاپ معاملاتی و چارت تحلیلی، کافی است **نام نماد** "
        f"(مانند `BTC` یا `SOL`) را ارسال کرده یا ویس بفرستید.\n\n"
        f"📢 **کانال ما:** {CHANNEL_LINK}",
        reply_markup=get_user_kb(user_id),
        parse_mode="Markdown"
    )


# ============================================================
# ==================== کانال ================================
# ============================================================

@dp.message(F.text == "📢 کانال ما")
async def channel_link_handler(message: types.Message):
    await message.answer(
        f"📢 **کانال رسمی AlphaEngine**\n\n"
        f"برای دنبال کردن سیگنال‌ها، اخبار و تحلیل‌های روزانه:\n\n"
        f"🔗 {CHANNEL_LINK}\n\n"
        f"💡 توی کانال، سیگنال‌های عمومی و اخبار مهم منتشر میشه.",
        parse_mode="Markdown"
    )


# ============================================================
# ==================== سیستم دعوت ===========================
# ============================================================

@dp.message(F.text == "👥 سیستم دعوت و هدیه")
async def referral_info_handler(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_or_create_user(user_id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

    await message.answer(
        f"🎁 **برنامه دعوت دوستان**\n\n"
        f"لینک اختصاصی شما:\n`{ref_link}`\n\n"
        f"📊 وضعیت شما:\n"
        f"• تعداد دعوت‌ها: **{len(user['referrals'])}**\n"
        f"• هر دعوت: **+{POINTS_PER_REFERRAL} امتیاز** ⭐\n"
        f"• امتیاز فعلی: **{user['points']}**\n\n"
        f"💡 **{POINTS_FOR_VIP} امتیاز = {FREE_VIP_DAYS} روز VIP رایگان**",
        parse_mode="Markdown"
    )


# ============================================================
# ==================== اخبار ================================
# ============================================================

@dp.message(F.text == "📰 اخبار و تحلیل احساسات")
async def crypto_news_handler(message: types.Message):
    user_id = message.from_user.id
    if not check_rate_limit(user_id):
        await message.answer("⏱ لطفاً کمی صبر کنید.")
        return
    msg = await message.answer("🔄 در حال دریافت آخرین اخبار...")
    raw_news = await fetch_crypto_news()
    prompt = (
        f"این اخبار کریپتو را تحلیلی و ساختاریافته خلاصه کن:\n{raw_news}\n\n"
        "⚠️ توضیحات و تحلیل را فارسی بنویس، اما نام ارزها و اصطلاحات (ETF, DeFi, Whale, Market Cap) را انگلیسی نگه دار."
    )
    try:
        response_text = await query_gemini(prompt)
        chunks = chunk_text(f"📰 **خلاصه اخبار:**\n\n{response_text}")
        try:
            await msg.edit_text(chunks[0], parse_mode="Markdown")
            for c in chunks[1:]:
                await message.answer(c, parse_mode="Markdown")
        except Exception:
            await msg.edit_text(chunks[0])
            for c in chunks[1:]:
                await message.answer(c)
    except Exception:
        await msg.edit_text(f"📰 خلاصه اخبار:\n\n{raw_news[:1000]}")


# ============================================================
# ==================== هشدار قیمت ===========================
# ============================================================

@dp.message(F.text == "🔔 هشدار قیمت")
async def start_price_alert(message: types.Message):
    set_user_state(message.from_user.id, "awaiting_alert_symbol")
    await message.answer("🔔 **تنظیم هشدار قیمت**\n\nلطفاً **نام ارز** را وارد کنید (مثال: BTC یا ETH):")


@dp.callback_query(F.data == "new_alert")
async def callback_new_alert(callback: types.CallbackQuery):
    await callback.answer()
    set_user_state(callback.from_user.id, "awaiting_alert_symbol")
    await callback.message.answer("🔔 نام ارز را وارد کنید:")


@dp.callback_query(F.data == "my_alerts")
async def callback_my_alerts(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    user_alerts = await db_get_user_alerts(user_id)
    if not user_alerts:
        await callback.message.answer("📋 هیچ هشدار فعالی ندارید.")
        return
    text = "📋 **هشدارهای فعال شما:**\n\n"
    for i, a in enumerate(user_alerts, 1):
        text += f"{i}. **{a['symbol']}** | هدف: `${a['target_price']}`\n"
    await callback.message.answer(text, parse_mode="Markdown")


# ============================================================
# ==================== اسکنر و DEX ==========================
# ============================================================

@dp.message(F.text == "🚀 اسکنر ارزهای پامپی")
async def pump_scanner_handler(message: types.Message):
    user_id = message.from_user.id
    is_vip = await db_is_vip(user_id) or is_admin(user_id)
    if not is_vip:
        await message.answer("🔒 مخصوص VIP", reply_markup=buy_vip_keyboard())
        return
    msg = await message.answer("🔍 در حال اسکن...")
    candidates = await scan_pump_candidates()
    if not candidates:
        await msg.edit_text("⚠️ ارزی یافت نشد.")
        return
    text = f"🔥 **ارزهای مستعد پامپ** (حجم ≥ ${MIN_PUMP_VOLUME_USD:,}):\n\n"
    for c in candidates:
        text += f"📌 **{c['symbol'].replace('/', '')}** | رشد: `+{c['change']:.2f}%`\n"
    await msg.edit_text(text, parse_mode="Markdown")


@dp.message(F.text == "🐳 رادار توکن‌های جدید (DEX)")
async def dex_radar_handler(message: types.Message):
    user_id = message.from_user.id
    is_vip = await db_is_vip(user_id) or is_admin(user_id)
    if not is_vip:
        await message.answer("🔒 مخصوص VIP", reply_markup=buy_vip_keyboard())
        return
    msg = await message.answer("🔎 در حال رصد...")
    tokens = await fetch_dex_tokens()
    text = "🐳 **توکن‌های ترند DEX:**\n\n"
    for t in tokens:
        text += f"🪙 **{t['symbol']}** | قیمت: `${t['price']}`\n"
    await msg.edit_text(text, parse_mode="Markdown")


# ============================================================
# ==================== شاخص ترس و طمع ======================
# ============================================================

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


# ============================================================
# ==================== محاسبه ریسک ==========================
# ============================================================

@dp.message(F.text == "🧮 محاسبه ریسک")
async def start_risk_calc(message: types.Message):
    user_id = message.from_user.id
    set_user_state(user_id, "awaiting_capital")
    if user_id not in user_cache:
        user_cache[user_id] = {}
    user_cache[user_id]["risk_calc_data"] = {}
    await message.answer("🧮 **محاسبه مدیریت ریسک**\n\nموجودی کل حساب (دلار):")


# ============================================================
# ==================== حساب کاربری ==========================
# ============================================================

@dp.message(F.text == "👤 حساب کاربری")
async def user_profile(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_or_create_user(user_id)
    status_text = "💎 VIP" if user["is_vip"] else "👤 رایگان"
    vip_until_text = ""
    if user["is_vip"] and user["vip_until"]:
        vip_until_text = f"\n📅 VIP تا: `{user['vip_until'].strftime('%Y-%m-%d')}`"

    points_text = ""
    if not is_admin(user_id):
        points_text = f"\n⭐ امتیاز: **{user['points']}**"

    await message.answer(
        f"👤 **پروفایل کاربری:**\n\n"
        f"🆔 آیدی: `{user_id}`\n"
        f"👑 وضعیت: {status_text}{vip_until_text}"
        f"{points_text}\n"
        f"👥 تعداد دعوت‌ها: **{len(user['referrals'])}**",
        parse_mode="Markdown"
    )


# ============================================================
# ==================== امتیاز من ============================
# ============================================================

@dp.message(F.text == "⭐ امتیاز من")
async def my_points(message: types.Message):
    user_id = message.from_user.id
    if is_admin(user_id):
        await message.answer("👑 شما ادمین هستید و نیازی به امتیاز ندارید.")
        return
    user = await db_get_or_create_user(user_id)
    points = user["points"]
    needed = max(0, POINTS_FOR_VIP - points)
    text = (
        f"⭐ **امتیاز شما: {points}**\n\n"
        f"📊 راه‌های کسب امتیاز:\n"
        f"• هر تحلیل ارز: **+{POINTS_PER_ANALYSIS}**\n"
        f"• هر دعوت موفق: **+{POINTS_PER_REFERRAL}**\n\n"
        f"🎁 **{POINTS_FOR_VIP} امتیاز = {FREE_VIP_DAYS} روز VIP رایگان**\n"
    )
    if points >= POINTS_FOR_VIP:
        text += f"\n✅ شما **{POINTS_FOR_VIP}** امتیاز دارید!\nبرای دریافت VIP، دکمه زیر رو بزن:"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🎁 دریافت {FREE_VIP_DAYS} روز VIP", callback_data="vip:redeem_points")]
        ])
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")
    else:
        text += f"\n🔸 **{needed}** امتیاز دیگه نیاز داری."
        await message.answer(text, parse_mode="Markdown")


# ============================================================
# ==================== خرید VIP =============================
# ============================================================

@dp.message(F.text == "💎 خرید VIP")
async def buy_vip_handler(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_or_create_user(user_id)
    if user["is_vip"]:
        until = user["vip_until"].strftime('%Y-%m-%d') if user["vip_until"] else "?"
        await message.answer(
            f"💎 شما **VIP** هستید!\n📅 اعتبار تا: `{until}`",
            parse_mode="Markdown"
        )
        return
    await message.answer(
        f"💎 **خرید VIP**\n"
        f"────────────────\n\n"
        f"💰 مبلغ: **{VIP_PRICE_TOMAN} تومان**\n"
        f"📅 مدت: **{VIP_DURATION_DAYS} روز**\n\n"
        f"💳 **شماره کارت:**\n`{PAYMENT_CARD}`\n"
        f"👤 به نام: **{PAYMENT_HOLDER}**\n\n"
        f"⚠️ **راهنمای پرداخت:**\n"
        f"۱. مبلغ رو به کارت بالا واریز کنید\n"
        f"۲. عکس فیش واریزی رو همین‌جا بفرستید\n"
        f"۳. بعد از تأیید ادمین، VIP فعال میشه\n\n"
        f"⏳ زمان تأیید: حداکثر ۲ ساعت",
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "vip:buy")
async def vip_buy_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await callback.message.answer(
        f"💎 **خرید VIP**\n"
        f"────────────────\n\n"
        f"💰 مبلغ: **{VIP_PRICE_TOMAN} تومان**\n"
        f"📅 مدت: **{VIP_DURATION_DAYS} روز**\n\n"
        f"💳 **شماره کارت:**\n`{PAYMENT_CARD}`\n"
        f"👤 به نام: **{PAYMENT_HOLDER}**\n\n"
        f"⚠️ عکس فیش واریزی رو همین‌جا بفرستید.",
        parse_mode="Markdown"
    )
    set_user_state(user_id, "awaiting_payment_receipt")


@dp.callback_query(F.data == "vip:points")
async def vip_points_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    points = await db_get_points(user_id)
    if points < POINTS_FOR_VIP:
        await callback.message.answer(
            f"❌ امتیاز کافی نداری.\n\n"
            f"⭐ امتیاز فعلی: **{points}**\n"
            f"🎯 نیاز: **{POINTS_FOR_VIP}**",
            parse_mode="Markdown"
        )
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ تأیید ({POINTS_FOR_VIP} امتیاز)", callback_data="vip:redeem_points")]
    ])
    await callback.message.answer(
        f"🎁 **دریافت VIP با امتیاز**\n\n"
        f"⭐ امتیاز فعلی: **{points}**\n"
        f"💎 دریافت: **{FREE_VIP_DAYS} روز VIP**\n"
        f"💸 هزینه: **{POINTS_FOR_VIP} امتیاز**\n\n"
        f"تأیید می‌کنی؟",
        reply_markup=kb,
        parse_mode="Markdown"
    )


@dp.callback_query(F.data == "vip:redeem_points")
async def vip_redeem_points(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    if is_admin(user_id):
        await callback.message.answer("👑 ادمین نیازی به این کار نداره.")
        return
    ok = await db_deduct_points(user_id, POINTS_FOR_VIP)
    if not ok:
        await callback.message.answer("❌ امتیاز کافی نداری.")
        return
    until = await db_extend_vip(user_id, FREE_VIP_DAYS)
    await callback.message.answer(
        f"🎉 **تبریک!** VIP فعال شد!\n\n"
        f"📅 مدت: **{FREE_VIP_DAYS} روز**\n"
        f"🗓 اعتبار تا: `{until.strftime('%Y-%m-%d')}`\n"
        f"💸 امتیاز کسر شده: **{POINTS_FOR_VIP}**",
        parse_mode="Markdown"
    )


# ============================================================
# ==================== کد اشتراک ============================
# ============================================================

@dp.message(F.text == "🎁 کد اشتراک")
async def discount_code_handler(message: types.Message):
    user_id = message.from_user.id
    set_user_state(user_id, "awaiting_discount_code")
    await message.answer(
        "🎁 **کد اشتراک**\n\n"
        f"با وارد کردن کد صحیح، **{FREE_VIP_DAYS} روز VIP رایگان** دریافت می‌کنید.\n\n"
        "کد خود را ارسال کنید:"
    )


# ============================================================
# ==================== آپلود فیش واریزی =====================
# ============================================================

@dp.message(F.photo)
async def handle_payment_photo(message: types.Message):
    user_id = message.from_user.id
    if await db_is_banned(user_id):
        await message.answer("🚫 شما محروم شده‌اید.")
        return

    check_state_timeout(user_id)
    state = user_cache.get(user_id, {}).get("state")
    if state != "awaiting_payment_receipt":
        await message.answer(
            "⚠️ برای ارسال فیش، اول دکمه **💎 خرید VIP** رو بزن.",
            parse_mode="Markdown"
        )
        return

    file_id = message.photo[-1].file_id
    req_id = await db_create_payment_request(user_id, PAYMENT_AMOUNT, file_id)
    set_user_state(user_id, None)

    username = message.from_user.username or "—"
    full_name = message.from_user.full_name or "—"

    admin_text = (
        f"🔔 **درخواست VIP جدید**\n"
        f"────────────────\n"
        f"🆔 آیدی: `{user_id}`\n"
        f"👤 نام: {full_name}\n"
        f"📛 یوزرنیم: @{username}\n"
        f"💰 مبلغ ادعایی: **{VIP_PRICE_TOMAN} تومان**\n"
        f"📅 مدت: **{VIP_DURATION_DAYS} روز**\n"
        f"🕐 زمان: `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n"
        f"🔖 شماره درخواست: `#{req_id}`"
    )

    try:
        await bot.send_photo(
            chat_id=ADMIN_ID,
            photo=file_id,
            caption=admin_text,
            reply_markup=payment_approval_keyboard(req_id),
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Send to admin failed: {e}")

    await message.answer(
        f"✅ **فیش شما دریافت شد!**\n\n"
        f"🔖 شماره درخواست: `#{req_id}`\n"
        f"⏳ بعد از تأیید ادمین، VIP فعال میشه.\n"
        f"📞 معمولاً کمتر از ۲ ساعت طول می‌کشه.",
        parse_mode="Markdown"
    )


# ============================================================
# ==================== تأیید/رد پرداخت ======================
# ============================================================

@dp.callback_query(F.data.startswith("pay:"))
async def payment_callbacks(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("🚫 دسترسی ندارید.", show_alert=True)
        return

    parts = callback.data.split(":")
    action = parts[1]
    req_id = int(parts[2])
    await callback.answer()

    req = await db_get_payment_request(req_id)
    if not req:
        await callback.message.answer("❌ درخواست پیدا نشد.")
        return

    target_uid = req["user_id"]

    if action == "approve":
        until = await db_extend_vip(target_uid, VIP_DURATION_DAYS)
        await db_update_payment_request(req_id, "approved")
        await db_log_admin(user_id, f"Approved payment #{req_id} for {target_uid}")
        try:
            await bot.send_message(
                target_uid,
                f"🎉 **تبریک! VIP فعال شد!**\n\n"
                f"📅 مدت: **{VIP_DURATION_DAYS} روز**\n"
                f"🗓 اعتبار تا: `{until.strftime('%Y-%m-%d')}`\n"
                f"🔖 درخواست: `#{req_id}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        try:
            await callback.message.edit_caption(
                caption=(callback.message.caption or "") + "\n\n✅ **تأیید شد**",
            )
        except Exception:
            pass

    elif action == "reject":
        await db_update_payment_request(req_id, "rejected")
        await db_log_admin(user_id, f"Rejected payment #{req_id} for {target_uid}")
        try:
            await bot.send_message(
                target_uid,
                f"❌ **درخواست VIP شما رد شد.**\n\n"
                f"🔖 درخواست: `#{req_id}`\n"
                f"💡 در صورت اشتباه، با پشتیبانی تماس بگیرید.",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        try:
            await callback.message.edit_caption(
                caption=(callback.message.caption or "") + "\n\n❌ **رد شد**",
            )
        except Exception:
            pass

    elif action == "later":
        await callback.answer("⏸ بعداً بررسی می‌کنی.", show_alert=True)

# ============================================================
# ==================== پنل ادمین ============================
# ============================================================

@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("🚫 شما دسترسی ادمین ندارید.")
        return
    await db_log_admin(user_id, "Opened admin panel via /admin")
    await message.answer(
        "⚙️ **پنل ادمین AlphaEngine**\n\nاز منوی زیر انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="Markdown"
    )


@dp.message(F.text == "⚙️ پنل ادمین")
async def admin_panel_button(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("🚫 دسترسی ندارید.")
        return
    await db_log_admin(user_id, "Opened admin panel via button")
    await message.answer(
        "⚙️ **پنل ادمین AlphaEngine**\n\nاز منوی زیر انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="Markdown"
    )


@dp.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("🚫 دسترسی ندارید.", show_alert=True)
        return

    data = callback.data.split(":")
    action = data[1] if len(data) > 1 else "back"
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
        c = await db_count_users()
        alerts = await db_get_all_alerts()
        payments = await db_get_pending_payments()
        settings = await db_get_settings()
        pump = "🟢" if settings.get("pump_detector_enabled") else "🔴"
        digest = "🟢" if settings.get("daily_digest_enabled") else "🔴"
        channel = "🟢" if settings.get("channel_broadcast_enabled", True) else "🔴"
        text = (
            f"📊 **آمار کلی بات**\n\n"
            f"👥 کل کاربران: **{c['total']}**\n"
            f"💎 کاربران VIP: **{c['vip']}**\n"
            f"🆓 کاربران رایگان: **{c['total'] - c['vip']}**\n"
            f"🚫 بن‌شده: **{c['banned']}**\n"
            f"🔔 هشدارهای فعال: **{len(alerts)}**\n"
            f"💳 درخواست VIP در انتظار: **{len(payments)}**\n"
            f"📅 فعال امروز: **{c['today']}**\n\n"
            f"⚙️ **وضعیت سیستم:**\n"
            f"🚀 رادار پامپ/دامپ: {pump}\n"
            f"☀️ بولتن روزانه: {digest}\n"
            f"📢 ارسال به کانال: {channel}"
        )
        await db_log_admin(user_id, "Viewed stats")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "users_menu":
        await callback.message.edit_text(
            "👥 **مدیریت کاربران**\n\nیکی رو انتخاب کن:",
            reply_markup=admin_users_menu_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "users_list":
        users = await db_get_all_users(limit=20)
        if not users:
            text = "📋 کاربری ثبت نشده."
        else:
            banned_list = await db_get_banned_users()
            text = "📋 **آخرین ۲۰ کاربر فعال:**\n\n"
            for u in users:
                vip_tag = "💎" if u["is_vip"] else "🆓"
                ban_tag = "🚫" if u["user_id"] in banned_list else ""
                text += f"{vip_tag}{ban_tag} `{u['user_id']}` ⭐{u.get('points', 0) or 0}\n"
            text += f"\n👥 کل: **{len(users)}**"
        await db_log_admin(user_id, "Viewed users list")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "vip_list":
        users = await db_get_all_users(limit=50, order="vip")
        if not users:
            text = "💎 VIP‌ای وجود ندارد."
        else:
            text = f"💎 **کاربران VIP ({len(users)}):**\n\n"
            for u in users:
                until = u["vip_until"].strftime('%Y-%m-%d') if u.get("vip_until") else "?"
                text += f"💎 `{u['user_id']}` تا `{until}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "ban_list":
        banned = await db_get_banned_users()
        if not banned:
            text = "🚫 لیست بن خالیه."
        else:
            text = f"🚫 **بن‌شده‌ها ({len(banned)}):**\n\n"
            for uid in banned[:30]:
                text += f"🚫 `{uid}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "user_search":
        admin_state[user_id] = {"action": "awaiting_user_search"}
        await callback.message.answer("🔍 **جستجو**\n\nآیدی عددی کاربر:")
        return

    if action in ("setvip", "unvip", "ban", "unban") and len(data) >= 3:
        target_uid = int(data[2])
        if action == "setvip":
            await db_extend_vip(target_uid, VIP_DURATION_DAYS)
            await db_log_admin(user_id, f"Set VIP for {target_uid}")
            await callback.answer(f"✅ {target_uid} VIP شد", show_alert=True)
        elif action == "unvip":
            await db_set_vip(target_uid, False)
            await db_log_admin(user_id, f"Removed VIP from {target_uid}")
            await callback.answer(f"🔻 VIP {target_uid} لغو شد", show_alert=True)
        elif action == "ban":
            await db_ban_user(target_uid, True)
            await db_log_admin(user_id, f"Banned {target_uid}")
            await callback.answer(f"🚫 {target_uid} بن شد", show_alert=True)
        elif action == "unban":
            await db_ban_user(target_uid, False)
            await db_log_admin(user_id, f"Unbanned {target_uid}")
            await callback.answer(f"✅ {target_uid} آنبن شد", show_alert=True)
        return

    if action == "payments":
        pendings = await db_get_pending_payments()
        if not pendings:
            await callback.message.edit_text(
                "💳 **درخواست‌های VIP**\n\n✅ هیچ درخواست در انتظاری نیست.",
                reply_markup=admin_back_keyboard(),
                parse_mode="Markdown"
            )
            return
        text = f"💳 **درخواست‌های VIP در انتظار ({len(pendings)}):**\n\n"
        text += "هر درخواست جداگانه با دکمه‌های تأیید/رد ارسال شده."
        await db_log_admin(user_id, "Viewed pending payments")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        for p in pendings[:5]:
            try:
                cap = (
                    f"💳 **درخواست #{p['id']}**\n\n"
                    f"🆔 کاربر: `{p['user_id']}`\n"
                    f"💰 مبلغ: `{p['amount']:,}` تومان\n"
                    f"🕐 زمان: `{p['created_at'].strftime('%Y-%m-%d %H:%M')}`"
                )
                await bot.send_photo(
                    chat_id=user_id,
                    photo=p["receipt_file_id"],
                    caption=cap,
                    reply_markup=payment_approval_keyboard(p["id"]),
                    parse_mode="Markdown"
                )
            except Exception:
                pass
        return

    if action == "codes_menu":
        await callback.message.edit_text(
            "🎁 **کدهای اشتراک**\n\nهر کد فقط یک بار استفاده میشه.",
            reply_markup=admin_codes_menu_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "code_new":
        code = await db_create_discount_code(user_id, days_valid=7)
        await db_log_admin(user_id, f"Created discount code {code}")
        await callback.message.edit_text(
            f"✅ **کد جدید ساخته شد:**\n\n"
            f"🎁 `{code}`\n\n"
            f"📅 اعتبار کد: **۷ روز**\n"
            f"💎 پاداش: **{FREE_VIP_DAYS} روز VIP رایگان**\n"
            f"🔖 یک بار قابل استفاده",
            reply_markup=admin_codes_menu_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "code_list":
        codes = await db_get_codes(limit=20)
        if not codes:
            text = "🎁 هیچ کدی ساخته نشده."
        else:
            text = "🎁 **آخرین ۲۰ کد:**\n\n"
            for c in codes:
                used = "✅" if c["is_used"] else "🟢"
                text += f"{used} `{c['code']}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_codes_menu_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_codes_menu_keyboard(), parse_mode="Markdown")
        return

    if action == "broadcast_menu":
        await callback.message.edit_text(
            "📢 **پیام همگانی**\n\nمخاطب:",
            reply_markup=admin_broadcast_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action in ("bcast_all", "bcast_vip", "bcast_free"):
        target = {"bcast_all": "همه", "bcast_vip": "فقط VIP", "bcast_free": "فقط غیر VIP"}[action]
        admin_state[user_id] = {"action": "awaiting_broadcast", "target": action}
        await callback.message.answer(f"📢 مخاطب: **{target}**\n\nمتن پیام:")
        return

    if action == "alerts_menu":
        alerts = await db_get_all_alerts()
        await callback.message.edit_text(
            f"🔔 **هشدارها**\n\nتعداد: **{len(alerts)}**",
            reply_markup=admin_alerts_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "alerts_list":
        alerts = await db_get_all_alerts()
        if not alerts:
            text = "📋 هشداری نیست."
        else:
            text = f"📋 **هشدارها ({len(alerts)}):**\n\n"
            for a in alerts[:30]:
                text += f"🪙 `{a['symbol']}` | `${a['target_price']}` | `{a['user_id']}`\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "alerts_clear":
        count = await db_clear_all_alerts()
        await db_log_admin(user_id, f"Cleared {count} alerts")
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
        logs = await db_get_admin_logs(limit=20)
        if not logs:
            text = "📋 لاگی نیست."
        else:
            text = "📋 **آخرین ۲۰ لاگ:**\n\n"
            for l in logs:
                ts = l["created_at"].strftime("%m-%d %H:%M") if l["created_at"] else "?"
                text += f"`[{ts}]` {l['action'][:80]}\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="Markdown")
        return

    if action == "settings":
        await callback.message.edit_text(
            "⚙️ **وضعیت سیستم**\n\nروشن/خاموش:",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "toggle_pump":
        settings = await db_get_settings()
        new_val = not settings.get("pump_detector_enabled", True)
        await db_set_setting("pump_detector_enabled", new_val)
        SYSTEM_SETTINGS["pump_detector_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled pump to {txt}")
        await callback.message.edit_text(
            f"✅ رادار پامپ/دامپ: **{txt}**",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "toggle_digest":
        settings = await db_get_settings()
        new_val = not settings.get("daily_digest_enabled", True)
        await db_set_setting("daily_digest_enabled", new_val)
        SYSTEM_SETTINGS["daily_digest_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled digest to {txt}")
        await callback.message.edit_text(
            f"✅ بولتن روزانه: **{txt}**",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return

    if action == "toggle_channel":
        settings = await db_get_settings()
        new_val = not settings.get("channel_broadcast_enabled", True)
        await db_set_setting("channel_broadcast_enabled", new_val)
        SYSTEM_SETTINGS["channel_broadcast_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled channel to {txt}")
        await callback.message.edit_text(
            f"✅ ارسال به کانال: **{txt}**",
            reply_markup=admin_settings_keyboard(),
            parse_mode="Markdown"
        )
        return


# ============================================================
# ==================== تایم‌فریم =============================
# ============================================================

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer()

    if await db_is_banned(user_id):
        await callback.message.answer("🚫 محروم هستید.")
        return
    if not check_rate_limit(user_id):
        await callback.message.answer("⏱ کمی صبر کن. حداکثر ۵ درخواست در دقیقه.")
        return

    _, symbol, tf = callback.data.split(":")
    loading_msg = await callback.message.answer(f"🔄 در حال پردازش **{escape_md(symbol)}**...")

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
                    caption=f"📊 **چارت {escape_md(symbol)} ({tf})**",
                    parse_mode="Markdown"
                )
            except Exception:
                await callback.message.answer_photo(photo=photo_file, caption=f"📊 چارت {symbol} ({tf})")

        if signal_text:
            for c in chunk_text(signal_text):
                try:
                    await callback.message.answer(c, parse_mode="Markdown")
                except Exception:
                    await callback.message.answer(c)

        if not is_admin(user_id):
            try:
                await db_add_points(user_id, POINTS_PER_ANALYSIS)
            except Exception:
                pass

    except Exception as e:
        logging.error(f"TF Error: {type(e).__name__}: {e}")
        await callback.message.answer(f"⚠️ خطا:\n{escape_md(str(e))}")


# ============================================================
# ================ ورودی متنی ===============================
# ============================================================

@dp.message(F.text)
async def handle_text_input(message: types.Message):
    user_id = message.from_user.id
    if await db_is_banned(user_id):
        await message.answer("🚫 محروم هستید.")
        return

    text = message.text.strip()

    if is_admin(user_id) and user_id in admin_state:
        st = admin_state[user_id]

        if st.get("action") == "awaiting_user_search":
            admin_state.pop(user_id, None)
            if not text.isdigit():
                await message.answer("⚠️ آیدی عددی وارد کن.")
                return
            target_uid = int(text)
            try:
                user = await db_get_or_create_user(target_uid)
            except Exception:
                await message.answer("❌ کاربر پیدا نشد.")
                return
            banned = await db_is_banned(target_uid)
            vip_tag = "💎 VIP" if user["is_vip"] else "🆓 رایگان"
            ban_tag = "🚫 بن" if banned else "✅ فعال"
            until = user["vip_until"].strftime('%Y-%m-%d') if user.get("vip_until") else "—"
            await message.answer(
                f"👤 **اطلاعات کاربر**\n\n"
                f"🆔 آیدی: `{target_uid}`\n"
                f"👑 وضعیت: {vip_tag}\n"
                f"📅 VIP تا: `{until}`\n"
                f"🚫 بن: {ban_tag}\n"
                f"⭐ امتیاز: **{user['points']}**\n"
                f"👥 دعوت: **{len(user['referrals'])}**",
                reply_markup=vip_action_keyboard(target_uid, user["is_vip"], banned),
                parse_mode="Markdown"
            )
            return

        if st.get("action") == "awaiting_broadcast":
            admin_state.pop(user_id, None)
            target = st.get("target")
            users = await db_get_all_users(limit=10000)
            count = 0
            for u in users:
                if target == "bcast_vip" and not u["is_vip"]:
                    continue
                if target == "bcast_free" and u["is_vip"]:
                    continue
                try:
                    await bot.send_message(u["user_id"], f"📢 **پیام از ادمین:**\n\n{text}", parse_mode="Markdown")
                    count += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            await db_log_admin(user_id, f"Broadcast to {target} ({count} users)")
            await message.answer(f"✅ پیام به **{count}** کاربر ارسال شد.", parse_mode="Markdown")
            return

    check_state_timeout(user_id)
    st_data = user_cache.get(user_id, {})
    state = st_data.get("state")

    if state == "awaiting_alert_symbol":
        formatted = text.upper()
        if not formatted.endswith("/USDT"):
            formatted += "/USDT"
        st_data["alert_temp"] = {"symbol": formatted}
        set_user_state(user_id, "awaiting_alert_price")
        await message.answer(f"قیمت مد نظر برای **{escape_md(formatted)}** (دلار):")
        return

    elif state == "awaiting_alert_price":
        try:
            target_p = float(text)
            symbol = st_data.get("alert_temp", {}).get("symbol", "")
            current_p = await get_ticker_price(symbol)
            if current_p == 0:
                await message.answer("⚠️ قیمت فعلی دریافت نشد. دوباره امتحان کن.")
                return
            condition = "above" if target_p > current_p else "below"
            await db_add_alert(user_id, symbol, target_p, condition)
            set_user_state(user_id, None)
            await message.answer(
                f"✅ **هشدار ثبت شد!**\n{symbol} | هدف: `${target_p}`",
                reply_markup=alert_success_keyboard(),
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer("⚠️ قیمت نامعتبر است.")
        return

    elif state == "awaiting_capital":
        try:
            st_data["risk_calc_data"] = {"capital": float(text)}
            set_user_state(user_id, "awaiting_risk_pct")
            await message.answer("درصد ریسک (مثلاً 1 یا 2):")
        except ValueError:
            await message.answer("لطفاً عدد وارد کن.")
        return

    elif state == "awaiting_risk_pct":
        try:
            st_data["risk_calc_data"]["risk_pct"] = float(text)
            set_user_state(user_id, "awaiting_entry")
            await message.answer("قیمت ورود (Entry):")
        except ValueError:
            await message.answer("لطفاً عدد وارد کن.")
        return

    elif state == "awaiting_entry":
        try:
            st_data["risk_calc_data"]["entry"] = float(text)
            set_user_state(user_id, "awaiting_sl")
            await message.answer("قیمت حد ضرر (Stop Loss):")
        except ValueError:
            await message.answer("لطفاً عدد وارد کن.")
        return

    elif state == "awaiting_sl":
        try:
            sl = float(text)
            d = st_data.get("risk_calc_data", {})
            capital, risk_pct, entry = d["capital"], d["risk_pct"], d["entry"]
            set_user_state(user_id, None)
            risk_amount = capital * (risk_pct / 100)
            sl_distance_pct = abs(entry - sl) / entry
            position_size = risk_amount / sl_distance_pct
            await message.answer(
                f"🧮 **نتیجه مدیریت ریسک:**\n\n"
                f"💵 کل سرمایه: `${capital:,.2f}`\n"
                f"🎯 ریسک: `${risk_amount:,.2f}` ({risk_pct}%)\n"
                f"✅ **حجم پیشنهادی:** `${position_size:,.2f}`",
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer("لطفاً عدد وارد کن.")
        return

    elif state == "awaiting_discount_code":
        code = text.strip().upper()
        set_user_state(user_id, None)
        result = await db_redeem_code(code, user_id)
        if result["ok"]:
            until = await db_extend_vip(user_id, result["days"])
            await message.answer(
                f"🎉 **کد تأیید شد!**\n\n"
                f"💎 VIP فعال شد: **{result['days']} روز**\n"
                f"🗓 اعتبار تا: `{until.strftime('%Y-%m-%d')}`",
                parse_mode="Markdown"
            )
        else:
            await message.answer(f"❌ {result['msg']}")
        return

    menu_buttons = [
        "🚀 اسکنر ارزهای پامپی", "🐳 رادار توکن‌های جدید (DEX)",
        "📊 شاخص ترس و طمع", "🧮 محاسبه ریسک", "👤 حساب کاربری",
        "🔔 هشدار قیمت", "📰 اخبار و تحلیل احساسات", "👥 سیستم دعوت و هدیه",
        "⚙️ پنل ادمین", "💎 خرید VIP", "🎁 کد اشتراک", "⭐ امتیاز من",
        "📢 کانال ما"
    ]
    symbol_text = text.upper()
    if symbol_text.startswith("/") or symbol_text in menu_buttons:
        return

    await message.answer(
        f"⏱ تایم‌فریم **{escape_md(symbol_text)}**:",
        reply_markup=timeframe_keyboard(symbol_text)
    )


# ============================================================
# ================ رادار پامپ/دامپ ==========================
# ============================================================

async def pump_dump_detector_loop():
    tracked = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT',
               'ADA/USDT', 'AVAX/USDT', 'LINK/USDT', 'SUI/USDT', 'PEPE/USDT']
    last_alerts = {}
    while True:
        try:
            await asyncio.sleep(60)
            settings = await db_get_settings()
            if not settings.get("pump_detector_enabled", True):
                continue
            now_ts = datetime.datetime.now().timestamp()
            exchange = ccxt.coinex()
            try:
                for symbol in tracked:
                    try:
                        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=21)
                        if len(ohlcv) < 21: continue
                        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                        last = df.iloc[-1]
                        prev = df.iloc[:-1]
                        avg_vol = prev['Volume'].mean()
                        cur_vol = last['Volume']
                        pct = ((last['Close'] - last['Open']) / last['Open']) * 100
                        spike = (cur_vol >= avg_vol * 4) and (avg_vol > 0)
                        alert_type = None
                        if spike and pct >= 1.2:
                            alert_type = "🚀 پامپ احتمالی (Pump Alert)"
                        elif spike and pct <= -1.2:
                            alert_type = "🩸 دامپ احتمالی (Dump Alert)"
                        if alert_type:
                            if symbol in last_alerts and (now_ts - last_alerts[symbol]) < 600:
                                continue
                            last_alerts[symbol] = now_ts
                            clean = symbol.replace('/', '')
                            alert_msg = (
                                f"🚨 **هشدار رادار بازار!**\n\n"
                                f"🪙 **{clean}**\n"
                                f"📊 {alert_type}\n"
                                f"📈 `{pct:+.2f}%`\n"
                                f"⚡️ `{cur_vol/avg_vol:.1f}X`\n"
                                f"💵 `${last['Close']}`"
                            )
                            users = await db_get_all_users(limit=10000)
                            for u in users:
                                if u["is_vip"]:
                                    try:
                                        await bot.send_message(u["user_id"], alert_msg, parse_mode="Markdown")
                                        await asyncio.sleep(0.05)
                                    except Exception:
                                        pass
                            await send_to_channel(alert_msg)
                    except Exception as e:
                        logging.error(f"Pump {symbol}: {e}")
            finally:
                try:
                    await exchange.close()
                except Exception:
                    pass
        except Exception as e:
            logging.error(f"Pump loop: {type(e).__name__}: {e}")


# ============================================================
# ================ بولتن روزانه =============================
# ============================================================

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
        f"یه خلاصه کوتاه (۳ سطر) از اخبار کریپتو:\n{raw_news}\n\n"
        "⚠️ فارسی، اما اصطلاحات (ETF, DeFi, Whale) انگلیسی."
    )
    try:
        news_summary = await query_gemini(prompt)
    except Exception:
        news_summary = "تغییرات نوسانی در بازار."
    return (
        f"☀️ **بولتن روزانه AlphaEngine**\n\n"
        f"🪙 **BTC:** `${btc_price:,.2f}` (`{btc_change:+.2f}%`)\n"
        f"📊 **شاخص ترس و طمع:** {fng_val}/100 ({fng_class})\n\n"
        f"📰 {news_summary}\n\n"
        f"📢 {CHANNEL_LINK}"
    )


async def daily_digest_scheduler():
    while True:
        await asyncio.sleep(60)
        try:
            settings = await db_get_settings()
            if not settings.get("daily_digest_enabled", True):
                continue
            now = datetime.datetime.now()
            if now.hour == 8 and now.minute == 0:
                digest = await generate_daily_digest()
                users = await db_get_all_users(limit=10000)
                for u in users:
                    try:
                        await bot.send_message(u["user_id"], digest, parse_mode="Markdown")
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
                await send_to_channel(digest)
                await asyncio.sleep(300)
        except Exception as e:
            logging.error(f"Digest loop: {e}")


async def background_alert_checker():
    while True:
        try:
            await asyncio.sleep(30)
            alerts = await db_get_all_alerts()
            if not alerts:
                continue
            exchange = ccxt.coinex()
            try:
                for alert in alerts:
                    try:
                        ticker = await exchange.fetch_ticker(alert["symbol"])
                        current = float(ticker.get('last') or ticker.get('close') or 0)
                        if current == 0: continue
                        triggered = False
                        if alert["condition"] == "above" and current >= alert["target_price"]:
                            triggered = True
                        elif alert["condition"] == "below" and current <= alert["target_price"]:
                            triggered = True
                        if triggered:
                            await bot.send_message(
                                chat_id=alert["user_id"],
                                text=(
                                    f"🚨 **هشدار قیمت!**\n\n"
                                    f"🪙 **{alert['symbol']}**\n"
                                    f"🎯 هدف: `${alert['target_price']}`\n"
                                    f"💵 فعلی: `${current}`"
                                ),
                                parse_mode="Markdown"
                            )
                            await db_delete_alert(alert["id"])
                    except Exception as e:
                        logging.error(f"Alert: {e}")
            finally:
                try:
                    await exchange.close()
                except Exception:
                    pass
        except Exception as e:
            logging.error(f"Alert loop: {type(e).__name__}: {e}")


async def handle_web(request):
    return web.Response(text="AlphaEngine Pro Active!")


app = web.Application()
app.router.add_get('/', handle_web)


async def main():
    await init_db()
    try:
        settings = await db_get_settings()
        for k, v in settings.items():
            SYSTEM_SETTINGS[k] = v
    except Exception as e:
        logging.warning(f"Load settings: {e}")

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    asyncio.create_task(background_alert_checker())
    asyncio.create_task(daily_digest_scheduler())
    asyncio.create_task(pump_dump_detector_loop())

    logging.info("🚀 Bot starting polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
