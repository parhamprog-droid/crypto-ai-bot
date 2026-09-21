import os
import io
import logging
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import google.generativeai as genai
from pydub import AudioSegment

# تنظیم لاگ‌ها
logging.basicConfig(level=logging.INFO)

# بارگیری متغیرهای محیطی (.env)
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not BOT_TOKEN or not GEMINI_API_KEY:
    raise ValueError("❌ لطفاً متغیرهای BOT_TOKEN و GEMINI_API_KEY را در تنظیمات .env یا Render وارد کنید.")

# تنظیمات اتصال به Gemini
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-1.5-flash")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------------------------------------------------------
# تابع کمکی برای ارسال متن‌های طولانی (جلوگیری از ارور تلگرام)
# ---------------------------------------------------------
async def send_long_message(message: types.Message, text: str, parse_mode: str = "Markdown"):
    """اگر متن پاسخ بیشتر از ۴۰۰۰ کاراکتر باشد، آن را خرد کرده و در چند پیام می‌فرستد."""
    MAX_LENGTH = 4000
    if len(text) <= MAX_LENGTH:
        try:
            await message.answer(text, parse_mode=parse_mode)
        except Exception:
            await message.answer(text)
        return

    # تقسیم متن به بخش‌های ۴۰۰۰ کاراکتری
    for i in range(0, len(text), MAX_LENGTH):
        chunk = text[i:i + MAX_LENGTH]
        try:
            await message.answer(chunk, parse_mode=parse_mode)
        except Exception:
            await message.answer(chunk)

# ---------------------------------------------------------
# دستور /start
# ---------------------------------------------------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = (
        "⚡ **AlphaEngine Pro | AI Trading Suite** ⚡\n\n"
        "به ربات دستیار هوشمند کریپتو خوش آمدید.\n\n"
        "🔸 **تحلیل تصویر:** عکس چارت خود را ارسال کنید تا تحلیل دریافت کنید.\n"
        "🔸 **تحلیل صوتی:** ویس بفرستید تا پاسخ آن پردازش شود."
    )
    await message.answer(welcome_text, parse_mode="Markdown")

# ---------------------------------------------------------
# دریافت و پردازش پیام صوتی (Voice)
# ---------------------------------------------------------
@dp.message(F.voice)
async def handle_voice(message: types.Message):
    status_msg = await message.answer("🎙 در حال دریافت و تحلیل فایل صوتی...")
    try:
        # دانلود فایل ویس
        file_info = await bot.get_file(message.voice.file_id)
        voice_bytes = await bot.download_file(file_info.file_path)

        # تبدیل OGG به MP3 با pydub
        audio = AudioSegment.from_file(io.BytesIO(voice_bytes.read()), format="ogg")
        out_io = io.BytesIO()
        audio.export(out_io, format="mp3")
        out_io.seek(0)
        audio_data = out_io.read()

        # ارسال فایل صوتی به Gemini
        response = gemini_model.generate_content([
            "این یک پیام صوتی درباره بازار کریپتوکارنسی است. لطفاً متن آن را متوجه شده و پاسخ کاربردی و دقیقی به آن بده.",
            {"mime_type": "audio/mp3", "data": audio_data}
        ])

        await status_msg.delete()
        await send_long_message(message, response.text)

    except Exception as e:
        logging.error(f"Voice handling error: {e}")
        await status_msg.edit_text(f"⚠️ متأسفانه در پردازش فایل صوتی خطایی رخ داد:\n`{e}`", parse_mode="Markdown")

# ---------------------------------------------------------
# دریافت تصویر چارت و نمایش کیبورد انتخاب تایم‌فریم
# ---------------------------------------------------------
@dp.message(F.photo)
async def handle_photo(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="15m", callback_data="tf_15m"),
            InlineKeyboardButton(text="1h", callback_data="tf_1h"),
            InlineKeyboardButton(text="4h", callback_data="tf_4h"),
            InlineKeyboardButton(text="1d", callback_data="tf_1d"),
        ]
    ])
    await message.answer("📊 تایم‌فریم تحلیل را انتخاب کنید:", reply_markup=keyboard)

# ---------------------------------------------------------
# پردازش دکمه‌های کلیک‌شده تایم‌فریم (15m, 1h, 4h, 1d)
# ---------------------------------------------------------
@dp.callback_query(F.data.startswith("tf_"))
async def handle_timeframe_click(callback: types.CallbackQuery):
    tf = callback.data.split("_")[1]
    await callback.answer(f"تایم‌فریم {tf} انتخاب شد.")
    
    status_msg = await callback.message.answer(f"🧠 در حال تحلیل چارت در تایم‌فریم {tf} توسط Gemini AI...")

    try:
        # پیدا کردن عکس ارسال‌شده
        photo_msg = callback.message.reply_to_message or callback.message
        if not photo_msg.photo:
            await status_msg.edit_text("❌ تصاویری برای تحلیل پیدا نشد. لطفاً عکس چارت را مجدداً ارسال کنید.")
            return

        photo = photo_msg.photo[-1] # دریافت بزرگ‌ترین کیفیت تصویر
        file_info = await bot.get_file(photo.file_id)
        img_bytes = await bot.download_file(file_info.file_path)

        prompt = (
            f"شما یک تحلیل‌گر حرفه‌ای کریپتو هستید. این تصویر یک چارت تکنیکال در تایم‌فریم {tf} است.\n"
            "لطفاً تحلیل جامعی شامل موارد زیر ارائه دهید:\n"
            "۱. روند کلی قیمت (صعودی/نزولی/رِنج)\n"
            "۲. سطوح مهم حمایت و مقاومت\n"
            "۳. اندیکاتورهای قابل مشاهده و پترن‌ها\n"
            "۴. پیشنهاد معاملاتی و مدیریت ریسک\n"
            "پاسخ را خلاصه، روان و کاربردی بنویس."
        )

        response = gemini_model.generate_content([
            prompt,
            {"mime_type": "image/jpeg", "data": img_bytes.read()}
        ])

        await status_msg.delete()
        await send_long_message(callback.message, response.text)

    except Exception as e:
        logging.error(f"Analysis error: {e}")
        await status_msg.edit_text(f"⚠️ خطایی در اجرای تحلیل رخ داد:\n`{e}`", parse_mode="Markdown")

# ---------------------------------------------------------
# نقطه شروع اجرای ربات
# ---------------------------------------------------------
async def main():
    print("🤖 Bot is running...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
