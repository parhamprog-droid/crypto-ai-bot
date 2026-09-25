import os
import re
import io
import html
import math
import time
import string
import asyncio
import logging
import secrets
import tempfile
import datetime

import asyncpg
import ccxt.async_support as ccxt
import aiohttp
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
from matplotlib.figure import Figure

from pydub import AudioSegment

from aiogram import Bot, Dispatcher, BaseMiddleware, types, F
from aiogram.filters import Command
from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
)
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, ErrorEvent
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


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logging.warning(f"ENV '{name}' is not a valid integer. Using {default}.")
        return default


# --- خواندن امن ENV ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = _env_int("ADMIN_ID", 0)
DATABASE_URL = os.getenv("DATABASE_URL")
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "0000-0000-0000-0000")
PAYMENT_HOLDER = os.getenv("PAYMENT_HOLDER", "نام صاحب کارت")
PAYMENT_AMOUNT = _env_int("PAYMENT_AMOUNT", 100000)
VIP_PRICE_TOMAN = os.getenv("VIP_PRICE_TOMAN") or f"{PAYMENT_AMOUNT:,}"
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@AlphaEngine_Official")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "https://t.me/AlphaEngine_Official")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("❌ ENV 'TELEGRAM_BOT_TOKEN' or 'BOT_TOKEN' is not set!")
if not GEMINI_API_KEY:
    raise RuntimeError("❌ ENV 'GEMINI_API_KEY' is not set!")
if not DATABASE_URL:
    raise RuntimeError("❌ ENV 'DATABASE_URL' is not set!")

# --- کلاینت Gemini ---
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
STATE_TIMEOUTS = {"awaiting_payment_receipt": 3600}
RATE_LIMIT_PER_MINUTE = 5
MARKET_CACHE_TTL = 60
MIN_PUMP_VOLUME_USD = 500000
VIP_DURATION_DAYS = 30
FREE_VIP_DAYS = 7
POINTS_FOR_VIP = 10
POINTS_PER_ANALYSIS = 1
POINTS_PER_REFERRAL = 5
MAX_ANALYSIS_POINTS_PER_DAY = 5
MAX_ALERTS_PER_USER = 10
MAX_VOICE_SECONDS = 60
GEMINI_TIMEOUT_SECONDS = 40
DIGEST_HOUR = 8
BOT_TZ = datetime.timezone(datetime.timedelta(hours=3, minutes=30))
VALID_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w")
SYSTEM_SETTINGS = {
    "pump_detector_enabled": True,
    "daily_digest_enabled": True,
    "channel_broadcast_enabled": True,
}

SQL_UTC_NOW = "(NOW() AT TIME ZONE 'UTC')"
VIP_ACTIVE_SQL = (
    f"(COALESCE(is_vip, FALSE) = TRUE AND (vip_until IS NULL OR vip_until > {SQL_UTC_NOW}))"
)

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

def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


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
            command_timeout=60, ssl='require',
            max_inactive_connection_lifetime=120,
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
                CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
            """)

            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_until TIMESTAMP;")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS points INTEGER DEFAULT 0;")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS analysis_points_today INTEGER DEFAULT 0;")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS analysis_points_date DATE;")
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


async def _build_user(conn, row) -> dict:
    is_vip = bool(row["is_vip"])
    vip_until = row["vip_until"]
    if is_vip and vip_until and vip_until < utcnow():
        is_vip = False
        await conn.execute("UPDATE users SET is_vip = FALSE WHERE user_id = $1", row["user_id"])
    referrals = await conn.fetch(
        "SELECT referred_id FROM referrals WHERE referrer_id = $1", row["user_id"]
    )
    return {
        "user_id": row["user_id"],
        "is_vip": is_vip,
        "vip_until": vip_until,
        "referred_by": row["referred_by"],
        "points": row["points"] or 0,
        "usage_count": row["usage_count"] or 0,
        "referrals": [r["referred_id"] for r in referrals],
    }


async def db_get_or_create_user(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        is_new = False
        if row is None:
            inserted = await conn.fetchval(
                "INSERT INTO users (user_id, is_vip) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO NOTHING RETURNING user_id",
                user_id, (user_id == ADMIN_ID and ADMIN_ID != 0)
            )
            is_new = inserted is not None
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        else:
            await conn.execute("UPDATE users SET last_seen = NOW() WHERE user_id = $1", user_id)
        user = await _build_user(conn, row)
        user["is_new"] = is_new
        return user


async def db_get_user(user_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return await _build_user(conn, row) if row else None


async def db_touch_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_seen = NOW() WHERE user_id = $1", user_id)


async def db_set_vip(user_id: int, is_vip: bool, days: int = 0):
    async with db_pool.acquire() as conn:
        if is_vip and days > 0:
            until = utcnow() + datetime.timedelta(days=days)
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
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id
        )
        return await conn.fetchval(f"""
            UPDATE users
            SET is_vip = TRUE,
                vip_until = GREATEST(COALESCE(vip_until, {SQL_UTC_NOW}), {SQL_UTC_NOW})
                            + make_interval(days => $2::int)
            WHERE user_id = $1
            RETURNING vip_until
        """, user_id, days)


async def db_is_vip(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_vip, vip_until FROM users WHERE user_id = $1", user_id)
        if not row or not row["is_vip"]:
            return False
        if row["vip_until"] and row["vip_until"] < utcnow():
            await conn.execute("UPDATE users SET is_vip = FALSE WHERE user_id = $1", user_id)
            return False
        return True


async def db_add_points(user_id: int, points: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET points = COALESCE(points, 0) + $1 WHERE user_id = $2",
            points, user_id
        )


async def db_add_analysis_points(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE users
            SET points = COALESCE(points, 0) + $2,
                analysis_points_today = CASE
                    WHEN analysis_points_date = CURRENT_DATE
                    THEN COALESCE(analysis_points_today, 0) + $2
                    ELSE $2 END,
                analysis_points_date = CURRENT_DATE
            WHERE user_id = $1
              AND (analysis_points_date IS DISTINCT FROM CURRENT_DATE
                   OR COALESCE(analysis_points_today, 0) + $2 <= $3)
            RETURNING points
        """, user_id, POINTS_PER_ANALYSIS, MAX_ANALYSIS_POINTS_PER_DAY)
        return row is not None


async def db_get_points(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT points FROM users WHERE user_id = $1", user_id)
        return (row["points"] if row else 0) or 0


async def db_deduct_points(user_id: int, points: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE users SET points = points - $1 "
            "WHERE user_id = $2 AND COALESCE(points, 0) >= $1 RETURNING points",
            points, user_id
        )
        return row is not None


async def db_add_referral(referrer_id: int, referred_id: int):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            referrer_exists = await conn.fetchval(
                "SELECT 1 FROM users WHERE user_id = $1", referrer_id
            )
            if not referrer_exists:
                return None
            inserted = await conn.fetchval(
                "INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2) "
                "ON CONFLICT (referred_id) DO NOTHING RETURNING referred_id",
                referrer_id, referred_id
            )
            if inserted is None:
                return None
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


async def db_delete_alert(alert_id: int, user_id: int = None) -> bool:
    async with db_pool.acquire() as conn:
        if user_id is None:
            row = await conn.fetchrow(
                "DELETE FROM price_alerts WHERE id = $1 RETURNING id", alert_id
            )
        else:
            row = await conn.fetchrow(
                "DELETE FROM price_alerts WHERE id = $1 AND user_id = $2 RETURNING id",
                alert_id, user_id
            )
        return row is not None


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
                f"SELECT * FROM users WHERE {VIP_ACTIVE_SQL} ORDER BY last_seen DESC LIMIT $1", limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM users ORDER BY last_seen DESC LIMIT $1", limit
            )
        return [dict(r) for r in rows]


async def db_get_broadcast_ids(target: str = "all") -> list:
    conditions = {
        "all": "TRUE",
        "vip": VIP_ACTIVE_SQL,
        "free": f"NOT {VIP_ACTIVE_SQL}",
    }
    cond = conditions.get(target, "TRUE")
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT user_id FROM users WHERE {cond} "
            f"AND user_id NOT IN (SELECT user_id FROM banned_users)"
        )
        return [r["user_id"] for r in rows]


async def db_count_users() -> dict:
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        vips = await conn.fetchval(f"SELECT COUNT(*) FROM users WHERE {VIP_ACTIVE_SQL}")
        banned = await conn.fetchval("SELECT COUNT(*) FROM banned_users")
        today = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE last_seen >= CURRENT_DATE"
        )
        return {"total": total, "vip": vips, "banned": banned, "today": today}


async def db_get_settings() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM system_settings")
        return {r["key"]: (r["value"] == "true") for r in rows if r["key"] in SYSTEM_SETTINGS}


async def db_set_setting(key: str, value: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO system_settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        """, key, str(value).lower())


async def db_get_kv(key: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM system_settings WHERE key = $1", key)
        return row["value"] if row else None


async def db_set_kv(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO system_settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, key, value)


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


async def db_resolve_payment(req_id: int, status: str, note: str = None):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE payment_requests
            SET status = $1, admin_note = $2, reviewed_at = NOW()
            WHERE id = $3 AND status = 'pending'
            RETURNING *
        """, status, note, req_id)
        return dict(row) if row else None


async def db_user_has_pending_payment(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM payment_requests WHERE user_id = $1 AND status = 'pending' LIMIT 1",
            user_id
        )
        return row is not None


async def db_get_pending_payments() -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM payment_requests WHERE status = 'pending' ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]


async def db_create_discount_code(created_by: int, days_valid: int = 7) -> str:
    alphabet = string.ascii_uppercase + string.digits
    code = ''.join(secrets.choice(alphabet) for _ in range(8))
    expires = utcnow() + datetime.timedelta(days=days_valid)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO discount_codes (code, created_by, expires_at)
            VALUES ($1, $2, $3)
        """, code, created_by, expires)
    return code


async def db_redeem_code(code: str, user_id: int):
    code = code.strip().upper()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            UPDATE discount_codes
            SET is_used = TRUE, used_by = $1, used_at = NOW()
            WHERE code = $2 AND is_used = FALSE
              AND (expires_at IS NULL OR expires_at > {SQL_UTC_NOW})
            RETURNING code
        """, user_id, code)
        if row:
            return {"ok": True, "days": FREE_VIP_DAYS}
        existing = await conn.fetchrow(
            "SELECT is_used FROM discount_codes WHERE code = $1", code
        )
    if not existing:
        return {"ok": False, "msg": "کد نامعتبر است."}
    if existing["is_used"]:
        return {"ok": False, "msg": "این کد قبلاً استفاده شده."}
    return {"ok": False, "msg": "این کد منقضی شده."}


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


_DIGIT_TABLE = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def parse_number(text):
    if text is None:
        return None
    t = str(text).strip().translate(_DIGIT_TABLE)
    for ch in (",", "٬", " "):
        t = t.replace(ch, "")
    t = t.replace("٫", ".")
    try:
        value = float(t)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def normalize_symbol(text):
    s = (text or "").strip().upper().replace(" ", "")
    if s.endswith("/USDT"):
        base = s[:-5]
    elif s.endswith("USDT") and len(s) > 4:
        base = s[:-4]
    else:
        base = s
    if base == "USDT" or not re.fullmatch(r"[A-Z0-9]{2,12}", base):
        return None
    return f"{base}/USDT"


def fmt_price(x) -> str:
    x = float(x)
    if x == 0:
        return "0"
    if abs(x) >= 1000:
        s = f"{x:.2f}"
    elif abs(x) >= 1:
        s = f"{x:.4f}"
    else:
        s = f"{x:.8f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def format_ai_text(text: str) -> str:
    text = html.escape(text or "", quote=False)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.S)
    text = re.sub(r"(?m)^[ \t]*[\*\-][ \t]+", "• ", text)
    text = text.replace("`", "")
    return text.strip()


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
        rate_limit[user_id] = user_times
        return False
    user_times.append(now)
    rate_limit[user_id] = user_times
    return True


def check_state_timeout(user_id: int) -> bool:
    entry = user_cache.get(user_id, {})
    st = entry.get("state_updated")
    if not st:
        return True
    limit = STATE_TIMEOUTS.get(entry.get("state"), STATE_TIMEOUT_SECONDS)
    if time.time() - st > limit:
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
    if len(market_cache) > 300:
        now = time.time()
        for k in [k for k, v in market_cache.items() if v["expires"] <= now]:
            market_cache.pop(k, None)
    market_cache[key] = {"data": data, "expires": time.time() + ttl}


# --- ارسال امن ---
_bg_tasks = set()


def spawn(coro):
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


async def safe_send(chat_id, text: str, **kwargs) -> bool:
    for _ in range(3):
        try:
            await bot.send_message(chat_id, text, **kwargs)
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except TelegramForbiddenError:
            return False
        except Exception as e:
            logging.warning(f"send to {chat_id} failed: {type(e).__name__}")
            return False
    return False


async def broadcast_to(user_ids: list, text: str, **kwargs) -> int:
    sent = 0
    for uid in user_ids:
        if await safe_send(uid, text, parse_mode="HTML", **kwargs):
            sent += 1
        await asyncio.sleep(0.05)
    return sent


async def send_chunked(msg: types.Message, message: types.Message, text: str):
    chunks = chunk_text(text)
    try:
        await msg.edit_text(chunks[0], parse_mode="HTML")
        for c in chunks[1:]:
            await message.answer(c, parse_mode="HTML")
    except Exception:
        await msg.edit_text(chunks[0])
        for c in chunks[1:]:
            await message.answer(c)


# --- Exchange مشترک ---
_exchange = None


def get_exchange():
    global _exchange
    if _exchange is None:
        _exchange = ccxt.coinex({"enableRateLimit": True})
    return _exchange


async def close_exchange():
    global _exchange
    if _exchange is not None:
        try:
            await _exchange.close()
        except Exception:
            pass
        _exchange = None


# --- Middleware ---
_last_touch = {}


class BanAndTouchMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user and not is_admin(user.id):
            try:
                if await db_is_banned(user.id):
                    text = "🚫 شما از استفاده از بات محروم شده‌اید."
                    if isinstance(event, types.CallbackQuery):
                        await event.answer(text, show_alert=True)
                    else:
                        await event.answer(text)
                    return None
            except Exception as e:
                logging.error(f"Ban check failed: {type(e).__name__}")
        if user:
            now = time.time()
            if now - _last_touch.get(user.id, 0) > 300:
                _last_touch[user.id] = now
                try:
                    await db_touch_user(user.id)
                except Exception:
                    pass
        return await handler(event, data)


# ============================================================
# ==================== کیبوردها =============================
# ============================================================

def get_main_keyboard(user_id: int):
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
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 اسکنر ارزهای پامپی"), KeyboardButton(text="🐳 رادار توکن‌های جدید (DEX)")],
            [KeyboardButton(text="🔔 هشدار قیمت"), KeyboardButton(text="📰 اخبار و تحلیل احساسات")],
            [KeyboardButton(text="📊 شاخص ترس و طمع"), KeyboardButton(text="🧮 محاسبه ریسک")],
            [KeyboardButton(text="🎁 کد اشتراک")],
            [KeyboardButton(text="👤 حساب کاربری")],
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
# ================ لیست داینامیک Gemini ======================
# ============================================================

async def get_available_models():
    global _AVAILABLE_GEMINI_MODELS
    if _AVAILABLE_GEMINI_MODELS is not None:
        return list(_AVAILABLE_GEMINI_MODELS)
    # ⭐ مدل‌های جدید اضافه شدند
    preferred = [
        "gemini-3-flash",
        "gemini-3-pro",
        "gemini-3.1-pro-preview",
        "gemini-2.5-flash",
        "gemini-2.5-pro",
        "gemini-2.0-flash",
        "gemini-2.0-flash-lite",
        "gemini-1.5-flash",
        "gemini-1.5-flash-002",
        "gemini-1.5-pro",
    ]
    try:
        models = await asyncio.to_thread(lambda: list(gemini_client.models.list()))
        names = set()
        for m in models:
            try:
                actions = (getattr(m, "supported_actions", None)
                           or getattr(m, "supported_generation_methods", None) or [])
                if "generateContent" in actions:
                    names.add(m.name.replace("models/", ""))
            except Exception:
                continue
        # اول مدل‌های preferred که موجودند
        available = [m for m in preferred if m in names]
        # اگه هیچ‌کدام از preferred ها نبودند، هر مدل متنی موجود رو انتخاب کن
        if not available:
            skip = ("image", "tts", "embedding", "live", "audio", "vision", "robotics", "computer", "learnlm")
            available = [n for n in sorted(names)
                         if n.startswith("gemini-") and not any(s in n for s in skip)][:6]
        if not available:
            raise RuntimeError("no usable Gemini model found")
        _AVAILABLE_GEMINI_MODELS = available
        logging.info(f"✅ Available Gemini models: {available}")
        return list(available)
    except Exception as e:
        logging.warning(f"ListModels failed: {e}")
        # در بدترین حالت، preferred رو برمی‌گردونیم (شاید کار کنه)
        return list(preferred)


async def query_gemini(prompt: str) -> str:
    models = await get_available_models()
    last_error = None
    for model_name in models:
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    gemini_client.models.generate_content,
                    model=model_name,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=PERSIAN_SYSTEM_INSTRUCTION,
                        temperature=0.7,
                    ),
                ),
                timeout=GEMINI_TIMEOUT_SECONDS,
            )
            text = response.text if response else None
            if text:
                return text
        except Exception as e:
            last_error = e
            logging.warning(f"Gemini '{model_name}' failed: {type(e).__name__}")
            err_str = str(e)
            if ("404" in err_str or "NOT_FOUND" in err_str) and _AVAILABLE_GEMINI_MODELS \
                    and model_name in _AVAILABLE_GEMINI_MODELS:
                _AVAILABLE_GEMINI_MODELS.remove(model_name)
    raise last_error or RuntimeError("هیچ‌کدام از مدل‌های Gemini پاسخ ندادند.")


# ============================================================
# ==================== توابع داده بازار ======================
# ============================================================

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

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
    avg_vol = df['Volume'].rolling(10).mean()
    df['OrderBlock_Bullish'] = (
        (df['Close'] > df['Open'])
        & (df['Close'].shift(1) < df['Open'].shift(1))
        & (df['Volume'] > avg_vol * 1.5)
    )
    df['OrderBlock_Bearish'] = (
        (df['Close'] < df['Open'])
        & (df['Close'].shift(1) > df['Open'].shift(1))
        & (df['Volume'] > avg_vol * 1.5)
    )

    df['TR'] = np.maximum(df['High'] - df['Low'], np.maximum(
        abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))
    ))
    df['ATR'] = df['TR'].rolling(window=14).mean()
    return df


async def get_crypto_dataframe(symbol="BTC/USDT", timeframe="1h", limit=300):
    formatted_symbol = normalize_symbol(symbol)
    if not formatted_symbol:
        return None, None

    cache_key = f"df:{formatted_symbol}:{timeframe}:{limit}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        ohlcv = await get_exchange().fetch_ohlcv(formatted_symbol, timeframe=timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 15:
            return None, None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = add_indicators(df)

        result = (formatted_symbol, df)
        cache_set(cache_key, result, MARKET_CACHE_TTL)
        return result
    except Exception as e:
        logging.error(f"CCXT Error ({symbol}): {type(e).__name__}")
        return None, None


async def fetch_orderbook_and_futures(symbol="BTC/USDT"):
    formatted_symbol = normalize_symbol(symbol) or symbol
    cache_key = f"ob:{formatted_symbol}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        orderbook = await get_exchange().fetch_order_book(formatted_symbol, limit=20)
        bids_volume = sum([b[1] for b in orderbook['bids']])
        asks_volume = sum([a[1] for a in orderbook['asks']])
        orderbook_ratio = bids_volume / asks_volume if asks_volume > 0 else 1.0
        result = {"bids_vol": bids_volume, "asks_vol": asks_volume, "ratio": orderbook_ratio}
        cache_set(cache_key, result, MARKET_CACHE_TTL)
        return result
    except Exception:
        return {"bids_vol": 0, "asks_vol": 0, "ratio": 1.0}


async def fetch_crypto_news():
    cached = cache_get("news_raw")
    if cached is not None:
        return cached
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    articles = data.get("Data", [])
                    if isinstance(articles, list) and articles:
                        raw = "".join(
                            f"- Title: {a.get('title')}\n  Body: {(a.get('body') or '')[:150]}...\n\n"
                            for a in articles[:5]
                        )
                        cache_set("news_raw", raw, 300)
                        return raw
    except Exception as e:
        logging.error(f"News Error: {type(e).__name__}: {e}")
    return None


async def scan_pump_candidates():
    cached = cache_get("pump_scan")
    if cached is not None:
        return cached
    try:
        tickers = await get_exchange().fetch_tickers()
        candidates = []
        for symbol, data in tickers.items():
            if not symbol.endswith("/USDT"):
                continue
            volume = data.get('quoteVolume') or 0
            change = data.get('percentage')
            if change is None:
                o, last = data.get('open'), data.get('last')
                change = ((last - o) / o * 100) if (o and last) else 0
            if volume >= MIN_PUMP_VOLUME_USD and change >= 2.0:
                candidates.append({'symbol': symbol, 'change': float(change), 'volume': float(volume)})
        result = sorted(candidates, key=lambda x: x['change'], reverse=True)[:5]
        cache_set("pump_scan", result, MARKET_CACHE_TTL)
        return result
    except Exception as e:
        logging.error(f"Pump scan error: {type(e).__name__}")
        return []


async def fetch_dex_tokens():
    cached = cache_get("dex_tokens")
    if cached is not None:
        return cached
    headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    url = "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1"
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    filtered = []
                    for pool in data.get("data", []):
                        attr = pool.get("attributes", {})
                        raw_name = attr.get("name", "N/A")
                        symbol = raw_name.split("/")[0].strip() if "/" in raw_name else raw_name
                        raw_price = float(attr.get("base_token_price_usd") or 0)
                        filtered.append({"symbol": symbol, "price": fmt_price(raw_price)})
                        if len(filtered) >= 5:
                            break
                    if filtered:
                        cache_set("dex_tokens", filtered, 120)
                        return filtered
    except Exception as e:
        logging.error(f"Gecko Error: {type(e).__name__}: {e}")
    return None


async def get_ticker_price(symbol: str) -> float:
    formatted = normalize_symbol(symbol)
    if not formatted:
        return 0.0
    try:
        ticker = await get_exchange().fetch_ticker(formatted)
        return float(ticker.get('last') or ticker.get('close') or 0)
    except Exception:
        return 0.0


async def send_to_channel(text: str, photo_bytes: bytes = None):
    try:
        if not SYSTEM_SETTINGS.get("channel_broadcast_enabled", True):
            return
        if photo_bytes:
            await bot.send_photo(
                chat_id=CHANNEL_USERNAME,
                photo=BufferedInputFile(photo_bytes, filename="chart.png"),
                caption=text[:1024],
                parse_mode="HTML"
            )
        else:
            await bot.send_message(
                chat_id=CHANNEL_USERNAME,
                text=text,
                parse_mode="HTML"
            )
    except Exception as e:
        logging.warning(f"Channel broadcast failed: {type(e).__name__}: {e}")


# ============================================================
# ================ چارت با FVG و OB =========================
# ============================================================

def generate_custom_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> bytes:
    df = df.tail(120).reset_index(drop=True)
    clean_symbol = symbol.replace("/", "")
    fig = Figure(figsize=(12, 7), facecolor='#f8f9fa')
    ax_main, ax_rsi = fig.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
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
    ax_main.text(n - 1, last_price, f" {fmt_price(last_price)}", color='white', backgroundcolor='red',
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

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    buf.seek(0)
    return buf.getvalue()


# ============================================================
# ================== تحلیل سیگنال ===========================
# ============================================================

DISCLAIMER = (
    "\n\n⚠️ <i>این خروجی الگوریتمی و آموزشی است و توصیه مالی نیست. "
    "مسئولیت هر معامله با خود شماست؛ همیشه حد ضرر را رعایت کنید.</i>"
)


def _trend_label(d) -> str:
    if d is None or d.empty:
        return "نامشخص ⚪️"
    return "صعودی 🟢" if d['Close'].iloc[-1] > d['EMA_50'].iloc[-1] else "نزولی 🔴"


def _yn(v) -> str:
    return "بله" if bool(v) else "خیر"


async def generate_signal(symbol: str, timeframe: str):
    formatted_symbol, df = await get_crypto_dataframe(symbol, timeframe)
    if df is None or df.empty:
        return (f"⚠️ ارز <b>{html.escape(str(symbol))}</b> پیدا نشد یا داده‌ای برای این تایم‌فریم وجود ندارد.",
                None, False)

    results = await asyncio.gather(
        get_crypto_dataframe(formatted_symbol, "1d", 120),
        get_crypto_dataframe(formatted_symbol, "4h", 120),
        get_crypto_dataframe("BTC/USDT", "1h", 120),
        fetch_orderbook_and_futures(formatted_symbol),
        return_exceptions=True,
    )
    df_1d, df_4h, df_btc = (r[1] if isinstance(r, tuple) else None for r in results[:3])
    ob_data = results[3] if isinstance(results[3], dict) else {"ratio": 1.0}

    trend_1d = _trend_label(df_1d)
    trend_4h = _trend_label(df_4h)
    trend_btc = _trend_label(df_btc)

    atr_raw = df['ATR'].iloc[-1]
    if pd.isna(atr_raw):
        atr_raw = (df['High'] - df['Low']).tail(14).mean()
    atr_val = float(atr_raw)
    atr_for_sl = fmt_price(atr_val * 1.5)
    atr_sl_max = fmt_price(atr_val * 3)
    atr_txt = fmt_price(atr_val)

    bb_mean = df['BB_Width'].rolling(30).mean().iloc[-1]
    is_squeeze = bool(df['BB_Width'].iloc[-1] < bb_mean * 0.7)
    squeeze_status = "⚠️ فشرده‌سازی نوسان (آماده‌باش انفجار قیمت 🔥)" if is_squeeze else "عادی 🟢"

    price = fmt_price(df['Close'].iloc[-1])
    rsi = float(df['RSI'].iloc[-1])
    fvg_bull = df['FVG_Bullish'].iloc[-3:].any()
    fvg_bear = df['FVG_Bearish'].iloc[-3:].any()
    ob_bull = df['OrderBlock_Bullish'].iloc[-5:].any()
    ob_bear = df['OrderBlock_Bearish'].iloc[-5:].any()
    ratio = float(ob_data.get('ratio', 1.0))

    prompt = f"""
تو مدیر ارشد ریسک یک هج‌فاند کریپتو هستی. یک ستاپ فوق‌پیشرفته موسسه‌ای برای {formatted_symbol} در تایم‌فریم {timeframe} صادر کن.

همگرایی روندهای تایم‌فریم بالاتر:
- روند دیلی (1D): {trend_1d}
- روند چهارساعته (4H): {trend_4h}
- وضعیت کلان بیت‌کوین: {trend_btc}

داده‌های فنی و ICT:
- قیمت فعلی: {price} | ATR: {atr_txt}
- محدوده مجاز SL بر اساس ATR: حداقل {atr_for_sl} و حداکثر {atr_sl_max} از Entry
- وضعیت نوسان: {squeeze_status}
- FVG صعودی: {_yn(fvg_bull)} | FVG نزولی: {_yn(fvg_bear)}
- Order Block صعودی: {_yn(ob_bull)} | Order Block نزولی: {_yn(ob_bear)}
- نسبت سفارشات خرید/فروش: {ratio:.2f} | RSI: {rsi:.2f}

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
- اگر داده‌ها متناقض یا ضعیف است، جهت معامله را Wait بگذار.
"""

    try:
        response_text = await query_gemini(prompt)
    except Exception as e:
        logging.error(f"Gemini analysis failed: {type(e).__name__}: {e}")
        return "⚠️ سرویس تحلیل هوش مصنوعی موقتاً در دسترس نیست. چند دقیقه دیگر دوباره تلاش کنید.", None, False

    chart_bytes = None
    try:
        chart_bytes = await asyncio.to_thread(generate_custom_chart, df, formatted_symbol, timeframe)
    except Exception as e:
        logging.error(f"Chart error: {type(e).__name__}: {e}")
    return format_ai_text(response_text) + DISCLAIMER, chart_bytes, True

# ============================================================
# =================== پردازش پیام صوتی ======================
# ============================================================

@dp.message(F.voice)
async def handle_voice_message(message: types.Message):
    user_id = message.from_user.id
    if not check_rate_limit(user_id):
        await message.answer("⏱ لطفاً کمی صبر کنید. حداکثر ۵ درخواست در دقیقه.")
        return
    if message.voice.duration and message.voice.duration > MAX_VOICE_SECONDS:
        await message.answer(f"⏱ حداکثر مدت ویس {MAX_VOICE_SECONDS} ثانیه است.")
        return

    msg = await message.answer("🎙 در حال تبدیل و تحلیل ویس توسط هوش مصنوعی...")
    base = os.path.join(tempfile.gettempdir(), f"voice_{user_id}_{message.message_id}")
    ogg_filename, wav_filename = base + ".ogg", base + ".wav"

    try:
        file = await bot.get_file(message.voice.file_id)
        await bot.download_file(file.file_path, destination=ogg_filename)
        models = await get_available_models()

        def _convert_upload_analyze():
            sound = AudioSegment.from_file(ogg_filename, format="ogg")
            sound.export(wav_filename, format="wav")
            uploaded = gemini_client.files.upload(file=wav_filename)
            try:
                prompt = (
                    "این یک فایل صوتی از کاربر در مورد بازار کریپتو است. "
                    "متن صحبت او را متوجه شو، سوال یا درخواست او را بررسی کن و پاسخ جامع بده.\n\n"
                    "⚠️ توضیحات فارسی، اصطلاحات تکنیکال انگلیسی."
                )
                last_exc = None
                for m_name in models:
                    try:
                        resp = gemini_client.models.generate_content(
                            model=m_name, contents=[uploaded, prompt],
                            config=genai_types.GenerateContentConfig(system_instruction=PERSIAN_SYSTEM_INSTRUCTION),
                        )
                        if resp and resp.text:
                            return resp.text
                    except Exception as ex:
                        last_exc = ex
                if last_exc:
                    raise last_exc
                return None
            finally:
                try:
                    gemini_client.files.delete(name=uploaded.name)
                except Exception:
                    pass

        response_text = await asyncio.wait_for(asyncio.to_thread(_convert_upload_analyze), timeout=180)
        if response_text:
            await send_chunked(msg, message, f"🗣 <b>پاسخ دستیار صوتی:</b>\n\n{format_ai_text(response_text)}")
        else:
            await msg.edit_text("⚠️ متنی از فایل صوتی تشخیص داده نشد.")
    except Exception as e:
        logging.error(f"Voice error: {type(e).__name__}: {e}")
        await msg.edit_text("⚠️ خطا در پردازش فایل صوتی. لطفاً دوباره تلاش کنید.")
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
        "📚 <b>راهنمای بات AlphaEngine</b>\n\n"
        "🔹 <b>تحلیل ارز:</b> نام نماد رو بفرست (مثل <code>BTC</code>)\n"
        "🔹 <b>تحلیل صوتی:</b> یه ویس بفرست\n"
        "🔹 <b>هشدار قیمت:</b> دکمه 🔔 هشدار قیمت\n"
        "🔹 <b>محاسبه ریسک:</b> دکمه 🧮 محاسبه ریسک\n"
        "🔹 <b>خرید VIP:</b> دکمه 💎 خرید VIP\n"
        "🔹 <b>کد اشتراک:</b> دکمه 🎁 کد اشتراک\n"
        "🔹 <b>امتیاز:</b> دکمه ⭐ امتیاز من\n\n"
        "⚙️ <b>دستورات:</b>\n"
        "<code>/start</code> — شروع\n"
        "<code>/help</code> — راهنما\n"
        "<code>/cancel</code> — لغو\n"
        "<code>/admin</code> — پنل ادمین (فقط ادمین)",
        parse_mode="HTML"
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

    user = await db_get_or_create_user(user_id)
    set_user_state(user_id, None)

    args = (message.text or "").split()
    if len(args) > 1 and args[1].isascii() and args[1].isdigit() and user.get("is_new"):
        referrer_id = int(args[1])
        if referrer_id != user_id:
            result = await db_add_referral(referrer_id, user_id)
            if result:
                await safe_send(
                    referrer_id,
                    f"🎉 یکی از دوستانت با لینک تو اومد!\n"
                    f"⭐ +{POINTS_PER_REFERRAL} امتیاز گرفتی.\n"
                    f"👥 تعداد دعوت‌ها: {result['count']}"
                )

    user = await db_get_or_create_user(user_id)
    status_text = "✨ VIP" if user["is_vip"] else "Standard 🔑"

    start_text = (
        f"🏛 <b>AlphaEngine Terminal Pro</b>\n"
        f"────────────────────────\n\n"
        f"به ترمینال تخصصی تحلیل الگوریتمی بازار کریپتو خوش آمدید.\n\n"
        f"🔰 <b>وضعیت حساب:</b> <code>{status_text}</code>\n"
        f"📡 <b>وضعیت اتصال:</b> آنلاین 🟢\n\n"
        f"💡 <b>راهنمای سریع:</b>\n"
        f"برای دریافت ستاپ معاملاتی و چارت تحلیلی، کافی است <b>نام نماد</b> "
        f"(مانند <code>BTC</code> یا <code>SOL</code>) را ارسال کرده یا ویس بفرستید.\n\n"
        f"📢 <b>کانال ما:</b> <a href=\"{CHANNEL_LINK}\">AlphaEngine Official</a>"
    )

    await message.answer(
        start_text,
        reply_markup=get_user_kb(user_id),
        parse_mode="HTML",
        disable_web_page_preview=True
    )


# ============================================================
# ==================== کانال ================================
# ============================================================

@dp.message(F.text == "📢 کانال ما")
async def channel_link_handler(message: types.Message):
    await message.answer(
        f"📢 <b>کانال رسمی AlphaEngine</b>\n\n"
        f"برای دنبال کردن سیگنال‌ها، اخبار و تحلیل‌های روزانه:\n\n"
        f"🔗 <a href=\"{CHANNEL_LINK}\">AlphaEngine Official</a>\n\n"
        f"💡 توی کانال، سیگنال‌های عمومی و اخبار مهم منتشر میشه.",
        parse_mode="HTML",
        disable_web_page_preview=True
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
        f"🎁 <b>برنامه دعوت دوستان</b>\n\n"
        f"لینک اختصاصی شما:\n<code>{ref_link}</code>\n\n"
        f"📊 وضعیت شما:\n"
        f"• تعداد دعوت‌ها: <b>{len(user['referrals'])}</b>\n"
        f"• هر دعوت: <b>+{POINTS_PER_REFERRAL} امتیاز</b> ⭐\n"
        f"• امتیاز فعلی: <b>{user['points']}</b>\n\n"
        f"💡 <b>{POINTS_FOR_VIP} امتیاز = {FREE_VIP_DAYS} روز VIP رایگان</b>",
        parse_mode="HTML"
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

    cached = cache_get("news_summary")
    if cached:
        await send_chunked(msg, message, cached)
        return

    raw_news = await fetch_crypto_news()
    if not raw_news:
        await msg.edit_text("⚠️ دریافت اخبار الان ممکن نیست. چند دقیقه بعد دوباره تلاش کنید.")
        return

    prompt = (
        f"این اخبار کریپتو را تحلیلی و ساختاریافته خلاصه کن:\n{raw_news}\n\n"
        "⚠️ توضیحات و تحلیل را فارسی بنویس، اما نام ارزها و اصطلاحات (ETF, DeFi, Whale, Market Cap) را انگلیسی نگه دار."
    )
    try:
        response_text = await query_gemini(prompt)
    except Exception:
        await msg.edit_text(f"📰 خلاصه اخبار (بدون تحلیل هوش مصنوعی):\n\n{raw_news[:1000]}")
        return
    body = f"📰 <b>خلاصه اخبار:</b>\n\n{format_ai_text(response_text)}"
    cache_set("news_summary", body, 600)
    await send_chunked(msg, message, body)


# ============================================================
# ==================== هشدار قیمت ===========================
# ============================================================

@dp.message(F.text == "🔔 هشدار قیمت")
async def start_price_alert(message: types.Message):
    user_id = message.from_user.id
    alerts = await db_get_user_alerts(user_id)
    if len(alerts) >= MAX_ALERTS_PER_USER:
        await message.answer(
            f"⚠️ حداکثر <b>{MAX_ALERTS_PER_USER}</b> هشدار فعال می‌توانید داشته باشید.\n"
            f"ابتدا یکی از هشدارهای قبلی را حذف کنید:",
            reply_markup=alert_success_keyboard(),
            parse_mode="HTML"
        )
        return
    set_user_state(user_id, "awaiting_alert_symbol")
    await message.answer("🔔 <b>تنظیم هشدار قیمت</b>\n\nلطفاً <b>نام ارز</b> را وارد کنید (مثال: BTC یا ETH):", parse_mode="HTML")


@dp.callback_query(F.data == "new_alert")
async def callback_new_alert(callback: types.CallbackQuery):
    await callback.answer()
    set_user_state(callback.from_user.id, "awaiting_alert_symbol")
    await callback.message.answer("🔔 نام ارز را وارد کنید:")


async def send_user_alerts(target_message: types.Message, user_id: int):
    alerts = await db_get_user_alerts(user_id)
    if not alerts:
        await target_message.answer("📋 هیچ هشدار فعالی ندارید.")
        return
    text = "📋 <b>هشدارهای فعال شما:</b>\n\n"
    rows = []
    for i, a in enumerate(alerts, 1):
        arrow = "📈" if a["condition"] == "above" else "📉"
        text += f"{i}. {arrow} <b>{html.escape(a['symbol'])}</b> | هدف: <code>{fmt_price(a['target_price'])}</code>\n"
        rows.append([InlineKeyboardButton(
            text=f"🗑 حذف #{i} ({a['symbol']})", callback_data=f"alert_del:{a['id']}"
        )])
    await target_message.answer(
        text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML"
    )


@dp.callback_query(F.data == "my_alerts")
async def callback_my_alerts(callback: types.CallbackQuery):
    await callback.answer()
    await send_user_alerts(callback.message, callback.from_user.id)


@dp.callback_query(F.data.startswith("alert_del:"))
async def callback_delete_alert(callback: types.CallbackQuery):
    try:
        alert_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer()
        return
    ok = await db_delete_alert(alert_id, callback.from_user.id)
    await callback.answer("🗑 حذف شد" if ok else "این هشدار قبلاً حذف شده.")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_user_alerts(callback.message, callback.from_user.id)


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
    text = f"🔥 <b>ارزهای مستعد پامپ</b> (حجم ≥ ${MIN_PUMP_VOLUME_USD:,}):\n\n"
    for c in candidates:
        text += f"📌 <b>{c['symbol'].replace('/', '')}</b> | رشد: <code>+{c['change']:.2f}%</code>\n"
    await msg.edit_text(text, parse_mode="HTML")


@dp.message(F.text == "🐳 رادار توکن‌های جدید (DEX)")
async def dex_radar_handler(message: types.Message):
    user_id = message.from_user.id
    is_vip = await db_is_vip(user_id) or is_admin(user_id)
    if not is_vip:
        await message.answer("🔒 مخصوص VIP", reply_markup=buy_vip_keyboard())
        return
    msg = await message.answer("🔎 در حال رصد...")
    tokens = await fetch_dex_tokens()
    if not tokens:
        await msg.edit_text("⚠️ دریافت اطلاعات DEX الان ممکن نیست. چند دقیقه بعد دوباره تلاش کنید.")
        return
    text = "🐳 <b>توکن‌های ترند DEX:</b>\n\n"
    for t in tokens:
        text += f"🪙 <b>{html.escape(t['symbol'])}</b> | قیمت: <code>{t['price']}</code>\n"
    await msg.edit_text(text, parse_mode="HTML")


# ============================================================
# ==================== شاخص ترس و طمع ======================
# ============================================================

FNG_FA = {
    "Extreme Fear": "ترس شدید 😱",
    "Fear": "ترس 😨",
    "Neutral": "خنثی 😐",
    "Greed": "طمع 🤑",
    "Extreme Greed": "طمع شدید 🚀",
}


@dp.message(F.text == "📊 شاخص ترس و طمع")
async def fear_and_greed(message: types.Message):
    try:
        cached = cache_get("fng")
        if cached is None:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.alternative.me/fng/", timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"status {resp.status}")
                    data = await resp.json()
                    cached = data["data"][0]
                    cache_set("fng", cached, 300)
        item = cached
        label = FNG_FA.get(item["value_classification"], item["value_classification"])
        await message.answer(
            f"📊 <b>شاخص ترس و طمع:</b>\n\n"
            f"🎯 عدد: <b>{item['value']}/100</b>\n"
            f"📌 وضعیت: <b>{label}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"FNG error: {type(e).__name__}: {e}")
        await message.answer("⚠️ دریافت شاخص ترس و طمع الان ممکن نیست. بعداً دوباره تلاش کنید.")


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
    await message.answer("🧮 <b>محاسبه مدیریت ریسک</b>\n\nموجودی کل حساب (دلار):", parse_mode="HTML")


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
        vip_until_text = f"\n📅 VIP تا: <code>{user['vip_until'].strftime('%Y-%m-%d')}</code>"

    points_text = ""
    if not is_admin(user_id):
        points_text = f"\n⭐ امتیاز: <b>{user['points']}</b>"

    await message.answer(
        f"👤 <b>پروفایل کاربری:</b>\n\n"
        f"🆔 آیدی: <code>{user_id}</code>\n"
        f"👑 وضعیت: {status_text}{vip_until_text}"
        f"{points_text}\n"
        f"👥 تعداد دعوت‌ها: <b>{len(user['referrals'])}</b>",
        parse_mode="HTML"
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
        f"⭐ <b>امتیاز شما: {points}</b>\n\n"
        f"📊 راه‌های کسب امتیاز:\n"
        f"• هر تحلیل ارز: <b>+{POINTS_PER_ANALYSIS}</b> (حداکثر {MAX_ANALYSIS_POINTS_PER_DAY} امتیاز در روز)\n"
        f"• هر دعوت موفق: <b>+{POINTS_PER_REFERRAL}</b>\n\n"
        f"🎁 <b>{POINTS_FOR_VIP} امتیاز = {FREE_VIP_DAYS} روز VIP رایگان</b>\n"
    )
    if points >= POINTS_FOR_VIP:
        text += f"\n✅ شما <b>{POINTS_FOR_VIP}</b> امتیاز دارید!\nبرای دریافت VIP، دکمه زیر رو بزن:"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🎁 دریافت {FREE_VIP_DAYS} روز VIP", callback_data="vip:redeem_points")]
        ])
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        text += f"\n🔸 <b>{needed}</b> امتیاز دیگه نیاز داری."
        await message.answer(text, parse_mode="HTML")


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
            f"💎 شما <b>VIP</b> هستید!\n📅 اعتبار تا: <code>{until}</code>",
            parse_mode="HTML"
        )
        return
    set_user_state(user_id, "awaiting_payment_receipt")
    await message.answer(
        f"💎 <b>خرید VIP</b>\n"
        f"────────────────\n\n"
        f"💰 مبلغ: <b>{VIP_PRICE_TOMAN} تومان</b>\n"
        f"📅 مدت: <b>{VIP_DURATION_DAYS} روز</b>\n\n"
        f"💳 <b>شماره کارت:</b>\n<code>{PAYMENT_CARD}</code>\n"
        f"👤 به نام: <b>{PAYMENT_HOLDER}</b>\n\n"
        f"⚠️ <b>راهنمای پرداخت:</b>\n"
        f"۱. مبلغ رو به کارت بالا واریز کنید\n"
        f"۲. عکس فیش واریزی رو همین‌جا بفرستید\n"
        f"۳. بعد از تأیید ادمین، VIP فعال میشه\n\n"
        f"⏳ زمان تأیید: حداکثر ۲ ساعت",
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "vip:buy")
async def vip_buy_callback(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    await callback.message.answer(
        f"💎 <b>خرید VIP</b>\n"
        f"────────────────\n\n"
        f"💰 مبلغ: <b>{VIP_PRICE_TOMAN} تومان</b>\n"
        f"📅 مدت: <b>{VIP_DURATION_DAYS} روز</b>\n\n"
        f"💳 <b>شماره کارت:</b>\n<code>{PAYMENT_CARD}</code>\n"
        f"👤 به نام: <b>{PAYMENT_HOLDER}</b>\n\n"
        f"⚠️ عکس فیش واریزی رو همین‌جا بفرستید.",
        parse_mode="HTML"
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
            f"⭐ امتیاز فعلی: <b>{points}</b>\n"
            f"🎯 نیاز: <b>{POINTS_FOR_VIP}</b>",
            parse_mode="HTML"
        )
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ تأیید ({POINTS_FOR_VIP} امتیاز)", callback_data="vip:redeem_points")]
    ])
    await callback.message.answer(
        f"🎁 <b>دریافت VIP با امتیاز</b>\n\n"
        f"⭐ امتیاز فعلی: <b>{points}</b>\n"
        f"💎 دریافت: <b>{FREE_VIP_DAYS} روز VIP</b>\n"
        f"💸 هزینه: <b>{POINTS_FOR_VIP} امتیاز</b>\n\n"
        f"تأیید می‌کنی؟",
        reply_markup=kb,
        parse_mode="HTML"
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
        f"🎉 <b>تبریک! VIP فعال شد!</b>\n\n"
        f"📅 مدت: <b>{FREE_VIP_DAYS} روز</b>\n"
        f"🗓 اعتبار تا: <code>{until.strftime('%Y-%m-%d')}</code>\n"
        f"💸 امتیاز کسر شده: <b>{POINTS_FOR_VIP}</b>",
        parse_mode="HTML"
    )


# ============================================================
# ==================== کد اشتراک ============================
# ============================================================

@dp.message(F.text == "🎁 کد اشتراک")
async def discount_code_handler(message: types.Message):
    user_id = message.from_user.id
    set_user_state(user_id, "awaiting_discount_code")
    await message.answer(
        "🎁 <b>کد اشتراک</b>\n\n"
        f"با وارد کردن کد صحیح، <b>{FREE_VIP_DAYS} روز VIP رایگان</b> دریافت می‌کنید.\n\n"
        "کد خود را ارسال کنید:",
        parse_mode="HTML"
    )


# ============================================================
# ==================== آپلود فیش واریزی =====================
# ============================================================

@dp.message(F.photo)
async def handle_payment_photo(message: types.Message):
    user_id = message.from_user.id

    check_state_timeout(user_id)
    state = user_cache.get(user_id, {}).get("state")
    if state != "awaiting_payment_receipt":
        await message.answer(
            "⚠️ برای ارسال فیش، اول دکمه <b>💎 خرید VIP</b> رو بزن.",
            parse_mode="HTML"
        )
        return

    if await db_user_has_pending_payment(user_id):
        set_user_state(user_id, None)
        await message.answer("⏳ درخواست قبلی شما هنوز در انتظار بررسی است. لطفاً کمی صبر کنید.")
        return

    file_id = message.photo[-1].file_id
    req_id = await db_create_payment_request(user_id, PAYMENT_AMOUNT, file_id)
    set_user_state(user_id, None)

    username = html.escape(message.from_user.username or "—")
    full_name = html.escape(message.from_user.full_name or "—")

    admin_text = (
        f"🔔 <b>درخواست VIP جدید</b>\n"
        f"────────────────\n"
        f"🆔 آیدی: <code>{user_id}</code>\n"
        f"👤 نام: {full_name}\n"
        f"📛 یوزرنیم: @{username}\n"
        f"💰 مبلغ: <b>{VIP_PRICE_TOMAN} تومان</b>\n"
        f"📅 مدت: <b>{VIP_DURATION_DAYS} روز</b>\n"
        f"🕐 زمان: <code>{datetime.datetime.now(BOT_TZ).strftime('%Y-%m-%d %H:%M')}</code>\n\n"
        f"🔖 شماره: <code>#{req_id}</code>"
    )

    try:
        await bot.send_photo(
            chat_id=ADMIN_ID,
            photo=file_id,
            caption=admin_text,
            reply_markup=payment_approval_keyboard(req_id),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Send to admin failed: {e}")

    await message.answer(
        f"✅ <b>فیش شما دریافت شد!</b>\n\n"
        f"🔖 شماره: <code>#{req_id}</code>\n"
        f"⏳ بعد از تأیید ادمین، VIP فعال میشه.\n"
        f"📞 معمولاً کمتر از ۲ ساعت طول می‌کشه.",
        parse_mode="HTML"
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
    try:
        action = parts[1]
        req_id = int(parts[2])
    except (IndexError, ValueError):
        await callback.answer()
        return

    if action == "later":
        await callback.answer("⏸ بعداً بررسی می‌کنی.", show_alert=True)
        return
    if action not in ("approve", "reject"):
        await callback.answer()
        return

    new_status = "approved" if action == "approve" else "rejected"
    req = await db_resolve_payment(req_id, new_status)
    if not req:
        await callback.answer("⚠️ این درخواست قبلاً بررسی شده یا وجود ندارد.", show_alert=True)
        return
    await callback.answer("✅ انجام شد")
    target_uid = req["user_id"]

    if action == "approve":
        try:
            until = await db_extend_vip(target_uid, VIP_DURATION_DAYS)
        except Exception as e:
            logging.error(f"Extend VIP failed: {type(e).__name__}: {e}")
            await db_update_payment_request(req_id, "pending")
            await callback.message.answer("❌ خطا در فعال‌سازی VIP؛ دوباره تأیید را بزنید.")
            return
        await db_log_admin(user_id, f"Approved payment #{req_id} for {target_uid}")
        await safe_send(
            target_uid,
            f"🎉 <b>تبریک! VIP فعال شد!</b>\n\n"
            f"📅 مدت: <b>{VIP_DURATION_DAYS} روز</b>\n"
            f"🗓 اعتبار تا: <code>{until.strftime('%Y-%m-%d')}</code>\n"
            f"🔖 درخواست: <code>#{req_id}</code>",
            parse_mode="HTML"
        )
        suffix = "\n\n✅ تأیید شد"
    else:
        await db_log_admin(user_id, f"Rejected payment #{req_id} for {target_uid}")
        await safe_send(
            target_uid,
            f"❌ <b>درخواست VIP شما رد شد.</b>\n\n"
            f"🔖 درخواست: <code>#{req_id}</code>\n"
            f"💡 در صورت اشتباه، با پشتیبانی تماس بگیرید.",
            parse_mode="HTML"
        )
        suffix = "\n\n❌ رد شد"

    try:
        await callback.message.edit_caption(caption=(callback.message.caption or "") + suffix)
    except Exception:
        pass

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
        "⚙️ <b>پنل ادمین AlphaEngine</b>\n\nاز منوی زیر انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.message(F.text == "⚙️ پنل ادمین")
async def admin_panel_button(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("🚫 دسترسی ندارید.")
        return
    await db_log_admin(user_id, "Opened admin panel via button")
    await message.answer(
        "⚙️ <b>پنل ادمین AlphaEngine</b>\n\nاز منوی زیر انتخاب کنید:",
        reply_markup=admin_panel_keyboard(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("🚫 دسترسی ندارید.", show_alert=True)
        return

    data = callback.data.split(":")
    action = data[1] if len(data) > 1 else "back"
    if action not in ("setvip", "unvip", "ban", "unban"):
        await callback.answer()

    if action == "back":
        try:
            await callback.message.edit_text(
                "⚙️ <b>پنل ادمین AlphaEngine</b>\n\nاز منوی زیر انتخاب کنید:",
                reply_markup=admin_panel_keyboard(),
                parse_mode="HTML"
            )
        except Exception:
            await callback.message.answer(
                "⚙️ <b>پنل ادمین AlphaEngine</b>",
                reply_markup=admin_panel_keyboard(),
                parse_mode="HTML"
            )
        return

    if action == "stats":
        c = await db_count_users()
        alerts = await db_get_all_alerts()
        payments = await db_get_pending_payments()
        pump = "🟢" if SYSTEM_SETTINGS.get("pump_detector_enabled") else "🔴"
        digest = "🟢" if SYSTEM_SETTINGS.get("daily_digest_enabled") else "🔴"
        channel = "🟢" if SYSTEM_SETTINGS.get("channel_broadcast_enabled", True) else "🔴"
        text = (
            f"📊 <b>آمار کلی بات</b>\n\n"
            f"👥 کل کاربران: <b>{c['total']}</b>\n"
            f"💎 کاربران VIP: <b>{c['vip']}</b>\n"
            f"🆓 کاربران رایگان: <b>{c['total'] - c['vip']}</b>\n"
            f"🚫 بن‌شده: <b>{c['banned']}</b>\n"
            f"🔔 هشدارهای فعال: <b>{len(alerts)}</b>\n"
            f"💳 درخواست VIP در انتظار: <b>{len(payments)}</b>\n"
            f"📅 فعال امروز: <b>{c['today']}</b>\n\n"
            f"⚙️ <b>وضعیت سیستم:</b>\n"
            f"🚀 رادار پامپ/دامپ: {pump}\n"
            f"☀️ بولتن روزانه: {digest}\n"
            f"📢 ارسال به کانال: {channel}"
        )
        await db_log_admin(user_id, "Viewed stats")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "users_menu":
        await callback.message.edit_text(
            "👥 <b>مدیریت کاربران</b>\n\nیکی رو انتخاب کن:",
            reply_markup=admin_users_menu_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "users_list":
        users = await db_get_all_users(limit=20)
        if not users:
            text = "📋 کاربری ثبت نشده."
        else:
            banned_list = await db_get_banned_users()
            text = "📋 <b>آخرین ۲۰ کاربر فعال:</b>\n\n"
            for u in users:
                vip_tag = "💎" if u["is_vip"] else "🆓"
                ban_tag = "🚫" if u["user_id"] in banned_list else ""
                text += f"{vip_tag}{ban_tag} <code>{u['user_id']}</code> ⭐{u.get('points', 0) or 0}\n"
            total = (await db_count_users())["total"]
            text += f"\n👥 نمایش {len(users)} از <b>{total}</b> کاربر"
        await db_log_admin(user_id, "Viewed users list")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "vip_list":
        users = await db_get_all_users(limit=50, order="vip")
        if not users:
            text = "💎 VIP‌ای وجود ندارد."
        else:
            text = f"💎 <b>کاربران VIP ({len(users)}):</b>\n\n"
            for u in users:
                until = u["vip_until"].strftime('%Y-%m-%d') if u.get("vip_until") else "?"
                text += f"💎 <code>{u['user_id']}</code> تا <code>{until}</code>\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "ban_list":
        banned = await db_get_banned_users()
        if not banned:
            text = "🚫 لیست بن خالیه."
        else:
            text = f"🚫 <b>بن‌شده‌ها ({len(banned)}):</b>\n\n"
            for uid in banned[:30]:
                text += f"🚫 <code>{uid}</code>\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "user_search":
        admin_state[user_id] = {"action": "awaiting_user_search", "ts": time.time()}
        await callback.message.answer("🔍 <b>جستجو</b>\n\nآیدی عددی کاربر:", parse_mode="HTML")
        return

    if action in ("setvip", "unvip", "ban", "unban") and len(data) >= 3:
        try:
            target_uid = int(data[2])
        except ValueError:
            await callback.answer()
            return
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
                "💳 <b>درخواست‌های VIP</b>\n\n✅ هیچ درخواست در انتظاری نیست.",
                reply_markup=admin_back_keyboard(),
                parse_mode="HTML"
            )
            return
        text = f"💳 <b>درخواست‌های VIP در انتظار ({len(pendings)}):</b>\n\n"
        text += "هر درخواست جداگانه با دکمه‌های تأیید/رد ارسال شده."
        await db_log_admin(user_id, "Viewed pending payments")
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        for p in pendings[:5]:
            try:
                cap = (
                    f"💳 <b>درخواست #{p['id']}</b>\n\n"
                    f"🆔 کاربر: <code>{p['user_id']}</code>\n"
                    f"💰 مبلغ: <code>{p['amount']:,}</code> تومان\n"
                    f"🕐 زمان: <code>{p['created_at'].strftime('%Y-%m-%d %H:%M')}</code>"
                )
                await bot.send_photo(
                    chat_id=user_id,
                    photo=p["receipt_file_id"],
                    caption=cap,
                    reply_markup=payment_approval_keyboard(p["id"]),
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return

    if action == "codes_menu":
        await callback.message.edit_text(
            "🎁 <b>کدهای اشتراک</b>\n\nهر کد فقط یک بار استفاده میشه.",
            reply_markup=admin_codes_menu_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "code_new":
        code = await db_create_discount_code(user_id, days_valid=7)
        await db_log_admin(user_id, f"Created discount code {code}")
        await callback.message.edit_text(
            f"✅ <b>کد جدید ساخته شد:</b>\n\n"
            f"🎁 <code>{code}</code>\n\n"
            f"📅 اعتبار کد: <b>۷ روز</b>\n"
            f"💎 پاداش: <b>{FREE_VIP_DAYS} روز VIP رایگان</b>\n"
            f"🔖 یک بار قابل استفاده",
            reply_markup=admin_codes_menu_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "code_list":
        codes = await db_get_codes(limit=20)
        if not codes:
            text = "🎁 هیچ کدی ساخته نشده."
        else:
            text = "🎁 <b>آخرین ۲۰ کد:</b>\n\n"
            for c in codes:
                used = "✅" if c["is_used"] else "🟢"
                text += f"{used} <code>{c['code']}</code>\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_codes_menu_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_codes_menu_keyboard(), parse_mode="HTML")
        return

    if action == "broadcast_menu":
        await callback.message.edit_text(
            "📢 <b>پیام همگانی</b>\n\nمخاطب:",
            reply_markup=admin_broadcast_keyboard(),
            parse_mode="HTML"
        )
        return

    if action in ("bcast_all", "bcast_vip", "bcast_free"):
        target = {"bcast_all": "همه", "bcast_vip": "فقط VIP", "bcast_free": "فقط غیر VIP"}[action]
        admin_state[user_id] = {"action": "awaiting_broadcast", "target": action, "ts": time.time()}
        await callback.message.answer(f"📢 مخاطب: <b>{target}</b>\n\nمتن پیام:", parse_mode="HTML")
        return

    if action == "alerts_menu":
        alerts = await db_get_all_alerts()
        await callback.message.edit_text(
            f"🔔 <b>هشدارها</b>\n\nتعداد: <b>{len(alerts)}</b>",
            reply_markup=admin_alerts_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "alerts_list":
        alerts = await db_get_all_alerts()
        if not alerts:
            text = "📋 هشداری نیست."
        else:
            text = f"📋 <b>هشدارها ({len(alerts)}):</b>\n\n"
            for a in alerts[:30]:
                text += f"🪙 <code>{a['symbol']}</code> | <code>{a['target_price']}</code> | <code>{a['user_id']}</code>\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "alerts_clear":
        count = await db_clear_all_alerts()
        await db_log_admin(user_id, f"Cleared {count} alerts")
        try:
            await callback.message.edit_text(
                f"✅ <b>{count} هشدار پاک شد.</b>",
                reply_markup=admin_back_keyboard(),
                parse_mode="HTML"
            )
        except Exception:
            await callback.message.answer(
                f"✅ <b>{count} هشدار پاک شد.</b>",
                reply_markup=admin_back_keyboard(),
                parse_mode="HTML"
            )
        return

    if action == "logs":
        logs = await db_get_admin_logs(limit=20)
        if not logs:
            text = "📋 لاگی نیست."
        else:
            text = "📋 <b>آخرین ۲۰ لاگ:</b>\n\n"
            for l in logs:
                ts = l["created_at"].strftime("%m-%d %H:%M") if l["created_at"] else "?"
                text += f"<code>[{ts}]</code> {html.escape(l['action'][:80])}\n"
        try:
            await callback.message.edit_text(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        except Exception:
            await callback.message.answer(text, reply_markup=admin_back_keyboard(), parse_mode="HTML")
        return

    if action == "settings":
        await callback.message.edit_text(
            "⚙️ <b>وضعیت سیستم</b>\n\nروشن/خاموش:",
            reply_markup=admin_settings_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "toggle_pump":
        new_val = not SYSTEM_SETTINGS.get("pump_detector_enabled", True)
        await db_set_setting("pump_detector_enabled", new_val)
        SYSTEM_SETTINGS["pump_detector_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled pump to {txt}")
        await callback.message.edit_text(
            f"✅ رادار پامپ/دامپ: <b>{txt}</b>",
            reply_markup=admin_settings_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "toggle_digest":
        new_val = not SYSTEM_SETTINGS.get("daily_digest_enabled", True)
        await db_set_setting("daily_digest_enabled", new_val)
        SYSTEM_SETTINGS["daily_digest_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled digest to {txt}")
        await callback.message.edit_text(
            f"✅ بولتن روزانه: <b>{txt}</b>",
            reply_markup=admin_settings_keyboard(),
            parse_mode="HTML"
        )
        return

    if action == "toggle_channel":
        new_val = not SYSTEM_SETTINGS.get("channel_broadcast_enabled", True)
        await db_set_setting("channel_broadcast_enabled", new_val)
        SYSTEM_SETTINGS["channel_broadcast_enabled"] = new_val
        txt = "روشن" if new_val else "خاموش"
        await db_log_admin(user_id, f"Toggled channel to {txt}")
        await callback.message.edit_text(
            f"✅ ارسال به کانال: <b>{txt}</b>",
            reply_markup=admin_settings_keyboard(),
            parse_mode="HTML"
        )
        return


# ============================================================
# ==================== تایم‌فریم =============================
# ============================================================

@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    parts = callback.data.split(":")
    if len(parts) != 3 or parts[2] not in VALID_TIMEFRAMES or not normalize_symbol(parts[1]):
        await callback.answer("⚠️ درخواست نامعتبر.", show_alert=True)
        return
    await callback.answer()

    if not check_rate_limit(user_id):
        await callback.message.answer("⏱ کمی صبر کن. حداکثر ۵ درخواست در دقیقه.")
        return

    symbol, tf = parts[1], parts[2]
    loading_msg = await callback.message.answer(
        f"🔄 در حال پردازش <b>{html.escape(symbol)}</b>...", parse_mode="HTML"
    )

    try:
        signal_text, chart_bytes, ok = await generate_signal(symbol, tf)
        try:
            await loading_msg.delete()
        except Exception:
            pass

        if chart_bytes:
            photo_file = BufferedInputFile(chart_bytes, filename=f"{symbol}.png")
            caption = f"📊 <b>چارت {html.escape(symbol)} ({tf})</b>"
            try:
                await callback.message.answer_photo(photo=photo_file, caption=caption, parse_mode="HTML")
            except Exception:
                await callback.message.answer_photo(photo=photo_file, caption=f"📊 چارت {symbol} ({tf})")

        if signal_text:
            for c in chunk_text(signal_text):
                try:
                    await callback.message.answer(c, parse_mode="HTML")
                except Exception:
                    await callback.message.answer(c)

        if ok and not is_admin(user_id):
            try:
                await db_add_analysis_points(user_id)
            except Exception as e:
                logging.warning(f"add points failed: {type(e).__name__}")

    except Exception as e:
        logging.error(f"TF Error: {type(e).__name__}: {e}")
        await callback.message.answer("⚠️ خطایی رخ داد. لطفاً دوباره تلاش کنید.")


# ============================================================
# ================ ورودی متنی ===============================
# ============================================================

@dp.message(F.text)
async def handle_text_input(message: types.Message):
    user_id = message.from_user.id
    text = message.text.strip()

    if is_admin(user_id) and user_id in admin_state:
        st = admin_state[user_id]
        if time.time() - st.get("ts", time.time()) > STATE_TIMEOUT_SECONDS:
            admin_state.pop(user_id, None)
        elif st.get("action") == "awaiting_user_search":
            admin_state.pop(user_id, None)
            if not (text.isascii() and text.isdigit()):
                await message.answer("⚠️ آیدی عددی وارد کن.")
                return
            target_uid = int(text)
            user = await db_get_user(target_uid)
            if not user:
                await message.answer("❌ کاربر پیدا نشد.")
                return
            banned = await db_is_banned(target_uid)
            vip_tag = "💎 VIP" if user["is_vip"] else "🆓 رایگان"
            ban_tag = "🚫 بن" if banned else "✅ فعال"
            until = user["vip_until"].strftime('%Y-%m-%d') if user.get("vip_until") else "—"
            await message.answer(
                f"👤 <b>اطلاعات کاربر</b>\n\n"
                f"🆔 آیدی: <code>{target_uid}</code>\n"
                f"👑 وضعیت: {vip_tag}\n"
                f"📅 VIP تا: <code>{until}</code>\n"
                f"🚫 بن: {ban_tag}\n"
                f"⭐ امتیاز: <b>{user['points']}</b>\n"
                f"👥 دعوت: <b>{len(user['referrals'])}</b>",
                reply_markup=vip_action_keyboard(target_uid, user["is_vip"], banned),
                parse_mode="HTML"
            )
            return
        elif st.get("action") == "awaiting_broadcast":
            admin_state.pop(user_id, None)
            target = st.get("target")
            kind = {"bcast_all": "all", "bcast_vip": "vip", "bcast_free": "free"}.get(target, "all")
            ids = await db_get_broadcast_ids(kind)
            body = f"📢 <b>پیام از ادمین:</b>\n\n{html.escape(text)}"
            await message.answer(f"⏳ ارسال به <b>{len(ids)}</b> کاربر شروع شد...", parse_mode="HTML")

            async def _run_broadcast():
                count = await broadcast_to(ids, body)
                await db_log_admin(user_id, f"Broadcast to {target} ({count}/{len(ids)} users)")
                await safe_send(
                    user_id, f"✅ پیام به <b>{count}</b> از {len(ids)} کاربر ارسال شد.", parse_mode="HTML"
                )

            spawn(_run_broadcast())
            return

    check_state_timeout(user_id)
    st_data = user_cache.setdefault(user_id, {})
    state = st_data.get("state")

    if state == "awaiting_alert_symbol":
        symbol = normalize_symbol(text)
        if not symbol:
            await message.answer("⚠️ نام ارز نامعتبر است. فقط نماد را بفرستید (مثال: BTC یا ETH):")
            return
        price = await get_ticker_price(symbol)
        if price <= 0:
            await message.answer(
                f"⚠️ ارز <b>{html.escape(symbol)}</b> روی صرافی پیدا نشد. نماد دیگری بفرستید:",
                parse_mode="HTML"
            )
            return
        st_data["alert_temp"] = {"symbol": symbol}
        set_user_state(user_id, "awaiting_alert_price")
        await message.answer(
            f"💵 قیمت فعلی <b>{symbol}</b>: <code>{fmt_price(price)}</code>\n\n"
            f"قیمت هدف را (به دلار) وارد کنید:",
            parse_mode="HTML"
        )
        return

    elif state == "awaiting_alert_price":
        target_p = parse_number(text)
        if target_p is None or target_p <= 0:
            await message.answer("⚠️ قیمت نامعتبر است. یک عدد مثبت وارد کنید (مثلاً 65000).")
            return
        symbol = st_data.get("alert_temp", {}).get("symbol")
        if not symbol:
            set_user_state(user_id, None)
            await message.answer("⏱ زمان این عملیات تمام شده؛ دوباره از 🔔 هشدار قیمت شروع کنید.")
            return
        current_p = await get_ticker_price(symbol)
        if current_p <= 0:
            await message.answer("⚠️ قیمت فعلی دریافت نشد. دوباره امتحان کن.")
            return
        alerts = await db_get_user_alerts(user_id)
        if len(alerts) >= MAX_ALERTS_PER_USER:
            set_user_state(user_id, None)
            await message.answer(
                f"⚠️ به سقف {MAX_ALERTS_PER_USER} هشدار رسیده‌اید. یکی را حذف کنید.",
                reply_markup=alert_success_keyboard()
            )
            return
        condition = "above" if target_p > current_p else "below"
        await db_add_alert(user_id, symbol, target_p, condition)
        set_user_state(user_id, None)
        await message.answer(
            f"✅ <b>هشدار ثبت شد!</b>\n{symbol} | هدف: <code>{fmt_price(target_p)}</code>",
            reply_markup=alert_success_keyboard(),
            parse_mode="HTML"
        )
        return

    elif state == "awaiting_capital":
        value = parse_number(text)
        if value is None or value <= 0:
            await message.answer("لطفاً یک عدد مثبت وارد کن.")
            return
        st_data["risk_calc_data"] = {"capital": value}
        set_user_state(user_id, "awaiting_risk_pct")
        await message.answer("درصد ریسک (مثلاً 1 یا 2):")
        return

    elif state == "awaiting_risk_pct":
        value = parse_number(text)
        if value is None or not (0 < value <= 100):
            await message.answer("لطفاً عددی بین 0 تا 100 وارد کن (مثلاً 1 یا 2).")
            return
        st_data.setdefault("risk_calc_data", {})["risk_pct"] = value
        set_user_state(user_id, "awaiting_entry")
        await message.answer("قیمت ورود (Entry):")
        return

    elif state == "awaiting_entry":
        value = parse_number(text)
        if value is None or value <= 0:
            await message.answer("لطفاً یک قیمت مثبت وارد کن.")
            return
        st_data.setdefault("risk_calc_data", {})["entry"] = value
        set_user_state(user_id, "awaiting_sl")
        await message.answer("قیمت حد ضرر (Stop Loss):")
        return

    elif state == "awaiting_sl":
        sl = parse_number(text)
        d = st_data.get("risk_calc_data") or {}
        if not all(k in d for k in ("capital", "risk_pct", "entry")):
            set_user_state(user_id, None)
            await message.answer("⏱ اطلاعات قبلی از بین رفته؛ دوباره از 🧮 محاسبه ریسک شروع کن.")
            return
        capital, risk_pct, entry = d["capital"], d["risk_pct"], d["entry"]
        if sl is None or sl <= 0:
            await message.answer("لطفاً یک قیمت مثبت وارد کن.")
            return
        if sl == entry:
            await message.answer("⚠️ حد ضرر نمی‌تواند برابر قیمت ورود باشد. عدد دیگری وارد کن.")
            return
        set_user_state(user_id, None)
        risk_amount = capital * (risk_pct / 100)
        sl_distance_pct = abs(entry - sl) / entry
        position_size = risk_amount / sl_distance_pct
        leverage_needed = position_size / capital
        side = "Long 🟢" if sl < entry else "Short 🔴"
        await message.answer(
            f"🧮 <b>نتیجه مدیریت ریسک:</b>\n\n"
            f"💵 کل سرمایه: <code>{capital:,.2f}</code>\n"
            f"🎯 ریسک: <code>{risk_amount:,.2f}</code> ({risk_pct}%)\n"
            f"📐 جهت (بر اساس SL): <b>{side}</b>\n"
            f"📏 فاصله SL تا Entry: <code>{sl_distance_pct * 100:.2f}%</code>\n"
            f"✅ <b>حجم پیشنهادی:</b> <code>{position_size:,.2f}</code>\n"
            f"⚖️ اهرم لازم: <code>{leverage_needed:.2f}x</code>",
            parse_mode="HTML"
        )
        return

    elif state == "awaiting_discount_code":
        if not check_rate_limit(user_id):
            await message.answer("⏱ لطفاً کمی صبر کنید.")
            return
        code = text.strip().upper()
        set_user_state(user_id, None)
        result = await db_redeem_code(code, user_id)
        if result["ok"]:
            until = await db_extend_vip(user_id, result["days"])
            await message.answer(
                f"🎉 <b>کد تأیید شد!</b>\n\n"
                f"💎 VIP فعال شد: <b>{result['days']} روز</b>\n"
                f"🗓 اعتبار تا: <code>{until.strftime('%Y-%m-%d')}</code>",
                parse_mode="HTML"
            )
        else:
            await message.answer(f"❌ {result['msg']}")
        return

    if text.startswith("/"):
        return

    symbol = normalize_symbol(text)
    if not symbol:
        if state == "awaiting_payment_receipt":
            await message.answer("📸 لطفاً <b>عکس فیش واریزی</b> را ارسال کنید (برای لغو: /cancel).", parse_mode="HTML")
        else:
            await message.answer(
                "🤔 نماد نامعتبر است. فقط نام ارز را بفرستید (مثل <code>BTC</code> یا <code>SOL</code>) یا از منوی پایین استفاده کنید.",
                parse_mode="HTML"
            )
        return

    base = symbol.split("/")[0]
    await message.answer(
        f"⏱ تایم‌فریم <b>{base}</b>:",
        reply_markup=timeframe_keyboard(base),
        parse_mode="HTML"
    )


# ============================================================
# ================ هندلر خطای سراسری ========================
# ============================================================

@dp.errors()
async def global_error_handler(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, TelegramBadRequest) and "message is not modified" in str(exc):
        return True
    logging.error(f"Unhandled handler error: {type(exc).__name__}: {exc}")
    return True


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
            if not SYSTEM_SETTINGS.get("pump_detector_enabled", True):
                continue
            exchange = get_exchange()
            for symbol in tracked:
                try:
                    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=21)
                    if len(ohlcv) < 21:
                        continue
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    last = df.iloc[-1]
                    prev = df.iloc[:-1]
                    avg_vol = prev['Volume'].mean()
                    cur_vol = last['Volume']
                    if not last['Open']:
                        continue
                    pct = ((last['Close'] - last['Open']) / last['Open']) * 100
                    spike = (cur_vol >= avg_vol * 4) and (avg_vol > 0)
                    alert_type = None
                    if spike and pct >= 1.2:
                        alert_type = "🚀 پامپ احتمالی (Pump Alert)"
                    elif spike and pct <= -1.2:
                        alert_type = "🩸 دامپ احتمالی (Dump Alert)"
                    if not alert_type:
                        continue
                    if time.time() - last_alerts.get(symbol, 0) < 600:
                        continue
                    last_alerts[symbol] = time.time()

                    clean = symbol.replace('/', '')
                    alert_msg = (
                        f"🚨 <b>هشدار رادار بازار!</b>\n\n"
                        f"🪙 <b>{clean}</b>\n"
                        f"📊 {alert_type}\n"
                        f"📈 <code>{pct:+.2f}%</code>\n"
                        f"⚡️ <code>{cur_vol / avg_vol:.1f}X</code>\n"
                        f"💵 <code>{fmt_price(last['Close'])}</code>"
                    )
                    vip_ids = await db_get_broadcast_ids("vip")
                    if ADMIN_ID and ADMIN_ID not in vip_ids:
                        vip_ids.append(ADMIN_ID)
                    spawn(broadcast_to(vip_ids, alert_msg))
                    await send_to_channel(alert_msg)
                except Exception as e:
                    logging.error(f"Pump {symbol}: {type(e).__name__}: {e}")
        except Exception as e:
            logging.error(f"Pump loop: {type(e).__name__}: {e}")


# ============================================================
# ================ بولتن روزانه =============================
# ============================================================

async def generate_daily_digest():
    fng_val, fng_class = "N/A", "N/A"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.alternative.me/fng/", timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    fng_val = d["data"][0]["value"]
                    fng_class = FNG_FA.get(d["data"][0]["value_classification"],
                                           d["data"][0]["value_classification"])
    except Exception:
        pass

    _, df_btc = await get_crypto_dataframe("BTC/USDT", "1d", 30)
    if df_btc is not None:
        btc_line = (f"🪙 <b>BTC:</b> <code>{df_btc['Close'].iloc[-1]:,.2f}</code> "
                    f"(<code>{df_btc['Close'].pct_change().iloc[-1] * 100:+.2f}%</code>)")
    else:
        btc_line = "🪙 <b>BTC:</b> N/A"

    news_summary = "تغییرات نوسانی در بازار."
    raw_news = await fetch_crypto_news()
    if raw_news:
        prompt = (
            f"یه خلاصه کوتاه (۳ سطر) از اخبار کریپتو:\n{raw_news}\n\n"
            "⚠️ فارسی، اما اصطلاحات (ETF, DeFi, Whale) انگلیسی."
        )
        try:
            news_summary = format_ai_text(await query_gemini(prompt))
        except Exception:
            pass
    return (
        f"☀️ <b>بولتن روزانه AlphaEngine</b>\n\n"
        f"{btc_line}\n"
        f"📊 <b>شاخص ترس و طمع:</b> {fng_val}/100 ({fng_class})\n\n"
        f"📰 {news_summary}\n\n"
        f"📢 <a href=\"{CHANNEL_LINK}\">AlphaEngine Official</a>"
    )


async def daily_digest_scheduler():
    while True:
        await asyncio.sleep(60)
        try:
            if not SYSTEM_SETTINGS.get("daily_digest_enabled", True):
                continue
            now = datetime.datetime.now(BOT_TZ)
            if not (DIGEST_HOUR <= now.hour < DIGEST_HOUR + 4):
                continue
            today = now.strftime("%Y-%m-%d")
            if await db_get_kv("last_digest_date") == today:
                continue
            await db_set_kv("last_digest_date", today)
            try:
                digest = await generate_daily_digest()
            except Exception:
                await db_set_kv("last_digest_date", "")
                raise
            ids = await db_get_broadcast_ids("all")
            spawn(broadcast_to(ids, digest, disable_web_page_preview=True))
            await send_to_channel(digest)
        except Exception as e:
            logging.error(f"Digest loop: {type(e).__name__}: {e}")


# ============================================================
# ================ چکر هشدار قیمت ===========================
# ============================================================

def _ticker_last(ticker) -> float:
    try:
        return float(ticker.get('last') or ticker.get('close') or 0)
    except (TypeError, ValueError):
        return 0.0


async def background_alert_checker():
    while True:
        try:
            await asyncio.sleep(30)
            alerts = await db_get_all_alerts()
            if not alerts:
                continue
            exchange = get_exchange()

            symbols = sorted({a["symbol"] for a in alerts})
            prices = {}
            if len(symbols) > 5:
                try:
                    tickers = await exchange.fetch_tickers()
                    for s in symbols:
                        if s in tickers:
                            p = _ticker_last(tickers[s])
                            if p > 0:
                                prices[s] = p
                except Exception as e:
                    logging.warning(f"fetch_tickers failed: {type(e).__name__}")
            else:
                for s in symbols:
                    try:
                        p = _ticker_last(await exchange.fetch_ticker(s))
                        if p > 0:
                            prices[s] = p
                    except Exception as e:
                        logging.warning(f"Alert ticker {s}: {type(e).__name__}")

            for alert in alerts:
                current = prices.get(alert["symbol"])
                if not current:
                    continue
                hit = ((alert["condition"] == "above" and current >= alert["target_price"])
                       or (alert["condition"] == "below" and current <= alert["target_price"]))
                if not hit:
                    continue
                if not await db_delete_alert(alert["id"]):
                    continue
                await safe_send(
                    alert["user_id"],
                    f"🚨 <b>هشدار قیمت!</b>\n\n"
                    f"🪙 <b>{html.escape(alert['symbol'])}</b>\n"
                    f"🎯 هدف: <code>{fmt_price(alert['target_price'])}</code>\n"
                    f"💵 فعلی: <code>{fmt_price(current)}</code>",
                    parse_mode="HTML"
                )
        except Exception as e:
            logging.error(f"Alert loop: {type(e).__name__}: {e}")


async def maintenance_loop():
    while True:
        await asyncio.sleep(600)
        try:
            now = time.time()
            for k in [k for k, v in market_cache.items() if v["expires"] <= now]:
                market_cache.pop(k, None)
            for uid in [u for u, ts in rate_limit.items() if not any(now - t < 60 for t in ts)]:
                rate_limit.pop(uid, None)
            for uid in [u for u, d in user_cache.items()
                        if not d.get("state") and now - (d.get("state_updated") or 0) > 3600]:
                user_cache.pop(uid, None)
            for uid in [u for u, t in _last_touch.items() if now - t > 3600]:
                _last_touch.pop(uid, None)
        except Exception as e:
            logging.error(f"Maintenance: {type(e).__name__}: {e}")


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

    dp.message.outer_middleware(BanAndTouchMiddleware())
    dp.callback_query.outer_middleware(BanAndTouchMiddleware())

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    tasks = [
        asyncio.create_task(background_alert_checker()),
        asyncio.create_task(daily_digest_scheduler()),
        asyncio.create_task(pump_dump_detector_loop()),
        asyncio.create_task(maintenance_loop()),
    ]

    logging.info("🚀 Bot starting polling...")
    try:
        await dp.start_polling(bot)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_exchange()
        await runner.cleanup()
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
