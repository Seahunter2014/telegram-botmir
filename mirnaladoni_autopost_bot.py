import asyncio
import logging
import os
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Tuple

import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, ReplyKeyboardMarkup
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID", "").strip()
TELEGRAM_ADMIN_ID_RAW = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "").strip()
DEFAULT_POST_TIMES = os.getenv("POST_TIMES", "09:00,14:00,19:00").strip()

# Фиксированное время UTC+3, независимо от сервера/VPN
BOT_TZ = timezone(timedelta(hours=3))

TELEGRAM_ADMIN_ID = int(TELEGRAM_ADMIN_ID_RAW) if TELEGRAM_ADMIN_ID_RAW else None

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHANNEL_ID:
    raise RuntimeError("Не задан TELEGRAM_CHANNEL_ID")
if not OPENAI_API_KEY:
    raise RuntimeError("Не задан OPENAI_API_KEY")
if not PEXELS_API_KEY:
    raise RuntimeError("Не задан PEXELS_API_KEY")

client = AsyncOpenAI(api_key=OPENAI_API_KEY)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
PHOTOS_DIR = BASE_DIR / "photos"
CACHE_DIR = PHOTOS_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("mirnaladoni_bot")

scheduler_instance: Optional[AsyncIOScheduler] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(text: str, max_len: int = 60) -> str:
    text = text.lower().strip()
    text = text.replace("ё", "e")
    text = re.sub(r"[^a-zA-Zа-яА-Я0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:max_len] or "photo"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT,
                theme_source TEXT,
                goal TEXT,
                post_type TEXT,
                content TEXT NOT NULL,
                referral_url TEXT,
                photo_path TEXT,
                photo_credit TEXT,
                photo_source_url TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL,
                published_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS topics(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_text TEXT NOT NULL,
                source_type TEXT DEFAULT 'manual',
                source_name TEXT DEFAULT '',
                used INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS partners(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                keywords TEXT NOT NULL,
                url TEXT NOT NULL,
                marker TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1
            )
        """)

        conn.commit()

    ensure_default_settings()
    ensure_default_topics()
    ensure_default_partners()


def set_setting(key: str, value: str):
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        conn.commit()


def get_setting(key: str, default: str = "") -> str:
    with closing(db()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def ensure_default_settings():
    defaults = {
        "autopost_enabled": "1",
        "post_times": DEFAULT_POST_TIMES or "09:00,14:00,19:00",
        "default_ref_url": "https://t.me/TourJin_bot",
        "tourjin_bot_url": "https://t.me/TourJin_bot",
        "test_channel_id": TEST_CHANNEL_ID,
        "topic_mode": "mixed",
        "pexels_orientation": "landscape",
        "pexels_size": "large",
        "pexels_per_page": "10",
        "photo_attribution_mode": "0",  # можно включить 1, если захотите показывать credit
    }
    for key, value in defaults.items():
        if get_setting(key, "") == "":
            set_setting(key, value)


def ensure_default_topics():
    default_topics = [
        "Как выбрать тур без переплаты",
        "5 ошибок при бронировании отпуска",
        "Куда поехать на море недорого",
        "Семейный отдых: как выбрать комфортный тур",
        "Когда лучше покупать туры",
        "Как сэкономить на all inclusive",
        "Что взять с собой в отпуск",
        "Как выбрать отель без разочарования",
        "Короткие путешествия на 3–5 дней",
        "Лучшие идеи для романтического отдыха",
        "Отдых с детьми без стресса",
        "Как проверить тур и не попасть на лишние траты",
    ]
    with closing(db()) as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM topics").fetchone()["c"]
        if count == 0:
            for topic in default_topics:
                conn.execute(
                    """
                    INSERT INTO topics(topic_text, source_type, source_name, used, created_at)
                    VALUES(?, 'default', 'system', 0, ?)
                    """,
                    (topic, now_iso()),
                )
            conn.commit()


def ensure_default_partners():
    with closing(db()) as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM partners").fetchone()["c"]
        if count == 0:
            conn.execute("""
                INSERT INTO partners(name, keywords, url, marker, is_active)
                VALUES(?, ?, ?, ?, 1)
            """, (
                "TourJin",
                "тур,туры,путешествие,путешествия,отпуск,отель,отели,перелет,море,пляж,курорт,отдых",
                "https://t.me/TourJin_bot",
                "main",
            ))
            conn.commit()


def is_admin(user_id: Optional[int]) -> bool:
    if TELEGRAM_ADMIN_ID is None:
        return True
    return user_id == TELEGRAM_ADMIN_ID


def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id if update.effective_user else None
        if not is_admin(user_id):
            await safe_reply(update, "Доступ запрещён.")
            return
        return await func(update, context)
    return wrapper


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            ["/gen", "/last", "/schedule"],
            ["/autopost_on", "/autopost_off", "/test_channel"],
            ["/topics", "/partners", "/menu"],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


async def safe_reply(update: Update, text: str):
    if update.message:
        await update.message.reply_text(text, reply_markup=main_menu_keyboard())
    elif update.effective_chat:
        await update.get_bot().send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=main_menu_keyboard(),
        )


def normalize_text(text: str) -> str:
    return (text or "").lower().strip()


def extract_keywords(text: str) -> List[str]:
    raw = normalize_text(text)
    cleaned = re.sub(r"[^\w\sа-яА-ЯёЁ-]", " ", raw)
    tokens = [t.strip("-_ ") for t in cleaned.split() if len(t.strip()) >= 3]
    return list(dict.fromkeys(tokens))


def choose_referral_url(topic: str, content: str) -> str:
    combined = f"{topic} {content}".lower()

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT name, keywords, url FROM partners WHERE is_active=1 ORDER BY id ASC"
        ).fetchall()

    best_url = get_setting("default_ref_url", "https://t.me/TourJin_bot")
    best_score = 0

    for row in rows:
        keywords = [k.strip().lower() for k in row["keywords"].split(",") if k.strip()]
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_url = row["url"]

    return best_url


def build_pexels_queries(topic: str) -> List[str]:
    topic_lower = normalize_text(topic)
    base = [
        topic_lower,
        f"{topic_lower} travel",
        f"{topic_lower} vacation",
        f"{topic_lower} tourism",
    ]

    mappings = [
        (["турция", "turkey"], ["turkey beach resort", "antalya resort", "turkey travel"]),
        (["егип", "egypt"], ["egypt resort sea", "egypt beach vacation"]),
        (["таиланд", "thai", "thailand"], ["thailand beach resort", "phuket travel"]),
        (["отель", "hotel"], ["hotel room travel", "resort hotel"]),
        (["море", "пляж", "beach"], ["beach resort", "sea vacation"]),
        (["семей", "дет", "family"], ["family vacation beach", "family travel"]),
        (["роман", "romantic"], ["romantic vacation beach", "couple travel resort"]),
        (["бюджет", "деш", "эконом"], ["budget travel", "cheap vacation"]),
        (["all inclusive"], ["all inclusive resort", "resort buffet vacation"]),
        (["перелет", "самолет", "flight"], ["airport travel", "airplane travel"]),
    ]

    for keys, queries in mappings:
        if any(k in topic_lower for k in keys):
            base.extend(queries)

    seen = []
    for q in base:
        q = q.strip()
        if q and q not in seen:
            seen.append(q)
    return seen[:6]


async def fetch_pexels_photo(topic: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    queries = build_pexels_queries(topic)
    orientation = get_setting("pexels_orientation", "landscape")
    size = get_setting("pexels_size", "large")
    per_page = int(get_setting("pexels_per_page", "10") or "10")

    headers = {"Authorization": PEXELS_API_KEY}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as http:
        for query in queries:
            try:
                response = await http.get(
                    "https://api.pexels.com/v1/search",
                    headers=headers,
                    params={
                        "query": query,
                        "per_page": per_page,
                        "orientation": orientation,
                        "size": size,
                    },
                )
                response.raise_for_status()
                data = response.json()
                photos = data.get("photos", [])
                if not photos:
                    continue

                picked = random.choice(photos[: min(len(photos), 5)])
                src = picked.get("src", {})
                image_url = (
                    src.get("large2x")
                    or src.get("large")
                    or src.get("medium")
                    or src.get("original")
                )
                if not image_url:
                    continue

                photographer = picked.get("photographer") or "Unknown photographer"
                photo_page = picked.get("url") or "https://www.pexels.com/"

                filename = f"{slugify(topic)}_{picked.get('id', random.randint(1000, 9999))}.jpg"
                filepath = CACHE_DIR / filename

                if not filepath.exists():
                    image_resp = await http.get(image_url)
                    image_resp.raise_for_status()
                    filepath.write_bytes(image_resp.content)

                credit = f"Фото: {photographer} / Pexels"
                return str(filepath), credit, photo_page
            except Exception:
                logger.exception("Ошибка загрузки фото из Pexels | query=%s", query)

    return None, None, None


def cleanup_post_text(text: str) -> str:
    text = text.replace("***", "").replace("**", "").replace("__", "")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = re.sub(r"[ \t]+", " ", text)

    broken_endings = ["осн", "под", "пер", "отд", "тур", "поезд", "путеш", "основан", "наиб", "предлож"]
    for ending in broken_endings:
        if text.lower().endswith(ending):
            text = text[:-len(ending)].rstrip(" ,.-:;")

    return text.strip()


def looks_incomplete(text: str) -> bool:
    stripped = text.strip()

    if len(stripped) < 500:
        return True

    if stripped.endswith((":", ",", ";", "—", "-", "…")):
        return True

    last_line = stripped.splitlines()[-1].strip()
    if stripped and stripped[-1].isalnum() and len(last_line.split()) <= 2:
        return True

    return False


def trim_to_limit(text: str, max_len: int) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text

    shortened = text[:max_len].rstrip()
    last_break = max(
        shortened.rfind("\n"),
        shortened.rfind(". "),
        shortened.rfind("! "),
        shortened.rfind("? "),
    )

    if last_break > max_len * 0.6:
        shortened = shortened[:last_break + 1].rstrip()

    return shortened.rstrip(" ,;:-") + "…"


def choose_post_ending(topic: str, referral_url: str) -> str:
    topic_lower = normalize_text(topic)

    sales_keywords = [
        "тур", "туры", "отель", "отдых", "море", "пляж", "курорт",
        "путешествие", "путешествия", "отпуск", "перелет", "all inclusive"
    ]

    neutral_keywords = [
        "лайфхак", "совет", "ошибка", "чек", "список", "что взять",
        "как выбрать", "как проверить"
    ]

    sales_match = any(word in topic_lower for word in sales_keywords)
    neutral_match = any(word in topic_lower for word in neutral_keywords)

    sales_endings = [
        f"Если хочешь упростить выбор поездки, загляни сюда 👇\n{referral_url}\n\nПодпишись на канал, поставь ❤️ и поделись постом с друзьями ✈️",
        f"Для удобного поиска вариантов можно использовать бота 👇\n{referral_url}\n\nНапиши в комментариях своё мнение и отправь пост друзьям 🌍",
        f"Сохрани пост, чтобы не потерять.\nА если захочешь посмотреть варианты поездки — вот бот 👇\n{referral_url}\n\nИ не забудь поставить ❤️",
    ]

    mixed_endings = [
        f"Подпишись на канал — здесь только полезные travel-разборы ✈️\n\nЕсли тема уже актуальна для тебя, можно посмотреть варианты тут:\n{referral_url}",
        f"Сохрани пост, поставь ❤️ и поделись с друзьями.\n\nА для удобного поиска вариантов есть бот 👇\n{referral_url}",
    ]

    neutral_endings = [
        "Подписывайся на канал, если любишь практичные советы для путешествий ✈️\n\nПоставь ❤️ и поделись постом с друзьями.",
        "Сохрани пост, чтобы не потерять, и поделись с друзьями 🌍\n\nА в комментариях напиши своё мнение или опыт.",
        "Если было полезно — подпишись на канал 💙\n\nПоставь ❤️ и напиши в комментариях, как у тебя было на практике.",
    ]

    if sales_match:
        return random.choice(sales_endings)
    if neutral_match:
        return random.choice(mixed_endings)
    return random.choice(neutral_endings)


def format_post_text(
    content: str,
    referral_url: str,
    photo_credit: Optional[str],
    photo_source_url: Optional[str],
    topic: str,
) -> str:
    ending = choose_post_ending(topic, referral_url)

    credit_block = ""
    if get_setting("photo_attribution_mode", "0") == "1" and photo_credit:
        credit_block = f"\n\n{photo_credit}"
        if photo_source_url:
            credit_block += f"\n{photo_source_url}"

    base_limit = 1024 - len(ending) - len(credit_block) - 4
    clean_content = trim_to_limit(content, max(520, base_limit))

    final_text = f"{clean_content}\n\n{ending}{credit_block}".strip()

    if len(final_text) > 1024:
        overflow = len(final_text) - 1024
        clean_content = trim_to_limit(clean_content, max(400, len(clean_content) - overflow - 10))
        final_text = f"{clean_content}\n\n{ending}{credit_block}".strip()

    return final_text[:1024].strip()


def parse_schedule(raw: str) -> List[Tuple[int, int]]:
    result = []
    items = [item.strip() for item in raw.split(",") if item.strip()]
    for item in items:
        hour_str, minute_str = item.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Некорректное время: {item}")
        result.append((hour, minute))
    if not result:
        raise ValueError("Пустое расписание")
    return result


def save_post(
    topic: str,
    theme_source: str,
    goal: str,
    post_type: str,
    content: str,
    referral_url: str,
    photo_path: Optional[str],
    photo_credit: Optional[str],
    photo_source_url: Optional[str],
    status: str = "draft",
) -> int:
    with closing(db()) as conn:
        cur = conn.execute("""
            INSERT INTO posts(
                topic, theme_source, goal, post_type, content,
                referral_url, photo_path, photo_credit, photo_source_url,
                status, created_at, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            topic,
            theme_source,
            goal,
            post_type,
            content,
            referral_url,
            photo_path,
            photo_credit,
            photo_source_url,
            status,
            now_iso(),
            None,
        ))
        conn.commit()
        return cur.lastrowid


def update_post_status(post_id: int, status: str):
    with closing(db()) as conn:
        published_at = now_iso() if status in {"published", "tested"} else None
        conn.execute(
            "UPDATE posts SET status=?, published_at=COALESCE(?, published_at) WHERE id=?",
            (status, published_at, post_id),
        )
        conn.commit()


def get_post(post_id: int):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM posts WHERE id=?", (post_id,)).fetchone()


def get_last_post():
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 1").fetchone()


def get_next_topic() -> Tuple[str, str]:
    with closing(db()) as conn:
        row = conn.execute("""
            SELECT * FROM topics
            WHERE used=0
            ORDER BY id ASC
            LIMIT 1
        """).fetchone()

        if row:
            conn.execute("UPDATE topics SET used=1 WHERE id=?", (row["id"],))
            conn.commit()
            return row["topic_text"], row["source_type"]

    fallback = random.choice([
        "Как выбрать тур без переплаты",
        "Куда уехать на море недорого",
        "Как найти хороший отель без ошибок",
        "Что важно знать перед отпуском",
        "Как сэкономить на путешествии",
    ])
    return fallback, "fallback"


async def generate_post_via_openai(topic: str) -> str:
    tourjin_url = get_setting("tourjin_bot_url", "https://t.me/TourJin_bot")

    prompt = f"""
Ты пишешь Telegram-пост для travel-канала «Мир на ладони».

Тема: {topic}

Жёсткие требования:
- язык: русский
- стиль: живой, экспертный, дружелюбный
- текст должен легко читаться с телефона
- основной текст: строго 700–900 символов
- итоговый пост после добавления концовки должен быть не длиннее 1024 символов
- обязательно делай абзацы
- не делай сплошное полотно текста
- используй 2–5 уместных эмодзи
- можно использовать короткие акценты и мини-подзаголовки
- не используй markdown со звёздочками
- не пиши слишком длинные предложения
- не выдумывай факты, цены и обещания
- текст должен быть законченным, без обрыва
- текст должен быть оригинальным
- можно мягко подвести к боту {tourjin_url}, если тема подходит
- не делай призывов поручить нам индивидуальный подбор
- не вставляй ссылку в основной текст
- не пиши пояснений для редактора
- верни только готовый текст поста

Структура:
1. короткий заголовок с эмодзи
2. заход 1–2 предложения
3. 2–4 коротких абзаца с пользой
4. короткий вывод
""".strip()

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.8,
        max_tokens=900,
        messages=[
            {"role": "system", "content": "Ты сильный редактор Telegram-постов про путешествия."},
            {"role": "user", "content": prompt},
        ],
    )

    text = (response.choices[0].message.content or "").strip()
    if not text:
        raise ValueError("OpenAI вернул пустой текст")

    return cleanup_post_text(text)


async def generate_post_with_retry(topic: str, retries: int = 3, delay: int = 4) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            text = await generate_post_via_openai(topic)

            if looks_incomplete(text):
                raise ValueError("Сгенерированный текст выглядит незавершённым")

            return text

        except Exception as exc:
            last_error = exc
            logger.exception(
                "Ошибка генерации поста | topic=%s | attempt=%s/%s",
                topic,
                attempt,
                retries,
            )
            if attempt < retries:
                await asyncio.sleep(delay)

    raise last_error


async def create_post_record(topic: str, source_type: str, goal: str, post_type: str) -> int:
    content = await generate_post_with_retry(topic)
    referral_url = choose_referral_url(topic, content)
    photo_path, photo_credit, photo_source_url = await fetch_pexels_photo(topic)

    post_id = save_post(
        topic=topic,
        theme_source=source_type,
        goal=goal,
        post_type=post_type,
        content=content,
        referral_url=referral_url,
        photo_path=photo_path,
        photo_credit=photo_credit,
        photo_source_url=photo_source_url,
        status="draft",
    )
    return post_id


async def publish_post_record(bot, channel_id: str, row, mark_status: str = "published"):
    text = format_post_text(
        row["content"],
        row["referral_url"] or get_setting("default_ref_url", "https://t.me/TourJin_bot"),
        row["photo_credit"],
        row["photo_source_url"],
        row["topic"] or "",
    )

    photo_path = row["photo_path"]
    if photo_path and Path(photo_path).exists():
        with open(photo_path, "rb") as photo_file:
            await bot.send_photo(
                chat_id=channel_id,
                photo=photo_file,
                caption=text[:1024],
            )
    else:
        await bot.send_message(chat_id=channel_id, text=text)

    update_post_status(row["id"], mark_status)


def format_post_card(row) -> str:
    if not row:
        return "Постов пока нет."

    return (
        f"ID: {row['id']}\n"
        f"Тема: {row['topic'] or '-'}\n"
        f"Источник темы: {row['theme_source'] or '-'}\n"
        f"Тип: {row['post_type'] or '-'}\n"
        f"Цель: {row['goal'] or '-'}\n"
        f"Канал: {TELEGRAM_CHANNEL_ID}\n"
        f"Статус: {row['status']}\n"
        f"Реф-ссылка: {row['referral_url'] or '-'}\n"
        f"Фото: {'да' if row['photo_path'] else 'нет'}\n\n"
        f"{row['content']}"
    )


async def scheduled_autopost_job(app: Application):
    if get_setting("autopost_enabled", "1") != "1":
        logger.info("Автопостинг отключён, задача пропущена")
        return

    logger.info("Запуск scheduled_autopost_job")

    topic, source_type = get_next_topic()
    post_id = await create_post_record(
        topic=topic,
        source_type=source_type,
        goal="trust",
        post_type="auto",
    )
    row = get_post(post_id)
    await publish_post_record(app.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
    logger.info("Автопост опубликован | id=%s | topic=%s", post_id, topic)


def rebuild_scheduler(app: Application):
    global scheduler_instance

    schedule_raw = get_setting("post_times", DEFAULT_POST_TIMES or "09:00,14:00,19:00")
    autopost_enabled = get_setting("autopost_enabled", "1") == "1"

    if scheduler_instance is None:
        scheduler_instance = AsyncIOScheduler(timezone=BOT_TZ)

    if scheduler_instance.running:
        scheduler_instance.remove_all_jobs()

    if autopost_enabled:
        for hour, minute in parse_schedule(schedule_raw):
            scheduler_instance.add_job(
                scheduled_autopost_job,
                trigger="cron",
                hour=hour,
                minute=minute,
                args=[app],
                id=f"autopost_{hour:02d}_{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )

    if not scheduler_instance.running:
        scheduler_instance.start()

    logger.info("Scheduler rebuilt | enabled=%s | times=%s", autopost_enabled, schedule_raw)


@admin_only
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        "Бот работает.\n\n"
        "/gen [тема] — сгенерировать пост\n"
        "/publish [id] — опубликовать пост\n"
        "/last — последний пост\n"
        "/schedule — показать расписание\n"
        "/set_schedule 09:00,14:00,19:00 — поменять расписание\n"
        "/autopost_on — включить автопостинг\n"
        "/autopost_off — выключить автопостинг\n"
        "/test_channel — тестовый пост в тестовый канал\n"
        "/test_post [id] — тест конкретного поста\n"
        "/topic_add текст — добавить тему\n"
        "/topics — показать темы\n"
        "/partners — показать партнёрские ссылки\n"
        "/partner_add name | keywords | url — добавить партнёрскую ссылку\n"
        "/set_tourjin https://t.me/TourJin_bot — обновить ссылку\n"
        "/set_test_channel @my_test_channel — обновить тестовый канал"
    )


@admin_only
async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_cmd(update, context)


@admin_only
async def gen_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    topic = " ".join(context.args).strip() if context.args else ""

    if not topic:
        topic, source_type = get_next_topic()
    else:
        source_type = "manual"

    await safe_reply(update, f"Генерирую пост по теме: {topic}")

    post_id = await create_post_record(
        topic=topic,
        source_type=source_type,
        goal="trust",
        post_type="expert",
    )
    row = get_post(post_id)

    await safe_reply(
        update,
        f"Пост создан.\n\n{format_post_card(row)}\n\n"
        f"Для публикации: /publish {post_id}\n"
        f"Для теста: /test_post {post_id}"
    )


@admin_only
async def publish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Пример: /publish 12")
        return

    try:
        post_id = int(context.args[0])
    except ValueError:
        await safe_reply(update, "ID должен быть числом. Пример: /publish 12")
        return

    row = get_post(post_id)
    if not row:
        await safe_reply(update, f"Пост с ID {post_id} не найден.")
        return

    await publish_post_record(context.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
    await safe_reply(update, f"Пост {post_id} опубликован.")


@admin_only
async def last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    row = get_last_post()
    await safe_reply(update, format_post_card(row))


@admin_only
async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    enabled = "включен" if get_setting("autopost_enabled", "1") == "1" else "выключен"
    schedule_raw = get_setting("post_times", DEFAULT_POST_TIMES or "09:00,14:00,19:00")
    await safe_reply(
        update,
        f"Автопостинг: {enabled}\n"
        f"Время публикаций (UTC+3): {schedule_raw}\n\n"
        f"Изменить: /set_schedule 08:30,13:00,18:45"
    )


@admin_only
async def set_schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = " ".join(context.args).strip()
    if not raw:
        await safe_reply(update, "Пример: /set_schedule 08:30,13:00,18:45")
        return

    try:
        parse_schedule(raw)
    except Exception as exc:
        await safe_reply(update, f"Ошибка расписания: {exc}")
        return

    set_setting("post_times", raw)
    rebuild_scheduler(context.application)
    await safe_reply(update, f"Расписание обновлено (UTC+3): {raw}")


@admin_only
async def autopost_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("autopost_enabled", "1")
    rebuild_scheduler(context.application)
    await safe_reply(update, "Автопостинг включен.")


@admin_only
async def autopost_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_setting("autopost_enabled", "0")
    rebuild_scheduler(context.application)
    await safe_reply(update, "Автопостинг выключен.")


@admin_only
async def test_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    test_channel_id = get_setting("test_channel_id", TEST_CHANNEL_ID).strip()
    if not test_channel_id:
        await safe_reply(update, "Не задан TEST_CHANNEL_ID или test_channel_id в настройках.")
        return

    row = get_last_post()
    if not row:
        post_id = await create_post_record(
            topic="Тестовый пост для проверки канала",
            source_type="test",
            goal="test",
            post_type="test",
        )
        row = get_post(post_id)

    try:
        await publish_post_record(context.bot, test_channel_id, row, mark_status="tested")
        await safe_reply(update, f"Тестовая публикация отправлена в {test_channel_id}.")
    except Forbidden:
        await safe_reply(
            update,
            "Не удалось отправить в тестовый канал: бот не состоит в канале или у него нет прав публикации."
        )


@admin_only
async def test_post_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Пример: /test_post 12")
        return

    try:
        post_id = int(context.args[0])
    except ValueError:
        await safe_reply(update, "ID должен быть числом. Пример: /test_post 12")
        return

    row = get_post(post_id)
    if not row:
        await safe_reply(update, f"Пост с ID {post_id} не найден.")
        return

    test_channel_id = get_setting("test_channel_id", TEST_CHANNEL_ID).strip()
    if not test_channel_id:
        await safe_reply(update, "Не задан тестовый канал.")
        return

    try:
        await publish_post_record(context.bot, test_channel_id, row, mark_status="tested")
        await safe_reply(update, f"Пост {post_id} отправлен в тестовый канал.")
    except Forbidden:
        await safe_reply(update, "Бот не может публиковать в тестовый канал. Проверьте членство и права.")


@admin_only
async def topics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, topic_text, source_type, used FROM topics ORDER BY id DESC LIMIT 10"
        ).fetchall()

    if not rows:
        await safe_reply(update, "Тем пока нет.")
        return

    text = "Последние темы:\n\n"
    for row in rows:
        text += (
            f"{row['id']}. {row['topic_text']} | "
            f"источник={row['source_type']} | used={row['used']}\n"
        )
    text += "\nДобавить: /topic_add Ваша тема"
    await safe_reply(update, text)


@admin_only
async def topic_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    topic = " ".join(context.args).strip()
    if not topic:
        await safe_reply(update, "Пример: /topic_add Как выбрать тур в Турцию летом")
        return

    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO topics(topic_text, source_type, source_name, used, created_at)
            VALUES (?, 'manual', 'admin', 0, ?)
            """,
            (topic, now_iso()),
        )
        conn.commit()

    await safe_reply(update, f"Тема добавлена: {topic}")


@admin_only
async def partners_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, name, keywords, url, marker, is_active FROM partners ORDER BY id DESC"
        ).fetchall()

    if not rows:
        await safe_reply(update, "Партнёрских ссылок пока нет.")
        return

    text = "Партнёрские ссылки:\n\n"
    for row in rows:
        text += (
            f"{row['id']}. {row['name']}\n"
            f"keywords: {row['keywords']}\n"
            f"url: {row['url']}\n"
            f"marker: {row['marker']}\n"
            f"active: {row['is_active']}\n\n"
        )
    text += "Добавить: /partner_add name | keywords | url"
    await safe_reply(update, text)


@admin_only
async def partner_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.replace("/partner_add", "", 1).strip()
    parts = [p.strip() for p in raw.split("|")]

    if len(parts) < 3:
        await safe_reply(update, "Пример: /partner_add TourJin | тур,отпуск,отель | https://t.me/TourJin_bot")
        return

    name, keywords, url = parts[0], parts[1], parts[2]

    with closing(db()) as conn:
        conn.execute("""
            INSERT INTO partners(name, keywords, url, marker, is_active)
            VALUES (?, ?, ?, '', 1)
        """, (name, keywords, url))
        conn.commit()

    await safe_reply(update, f"Партнёрская ссылка добавлена: {name}")


@admin_only
async def set_tourjin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Пример: /set_tourjin https://t.me/TourJin_bot")
        return

    url = context.args[0].strip()
    set_setting("tourjin_bot_url", url)
    set_setting("default_ref_url", url)
    await safe_reply(update, f"Ссылка TourJin обновлена: {url}")


@admin_only
async def set_test_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Пример: /set_test_channel @my_test_channel")
        return

    channel = context.args[0].strip()
    set_setting("test_channel_id", channel)
    await safe_reply(update, f"Тестовый канал обновлён: {channel}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled exception", exc_info=context.error)

    if isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла ошибка. Она записана в логи.",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не удалось отправить сообщение об ошибке пользователю")


async def post_init(application: Application) -> None:
    rebuild_scheduler(application)
    logger.info("Bot starting...")


def build_application() -> Application:
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("gen", gen_cmd))
    app.add_handler(CommandHandler("publish", publish_cmd))
    app.add_handler(CommandHandler("last", last_cmd))
    app.add_handler(CommandHandler("schedule", schedule_cmd))
    app.add_handler(CommandHandler("set_schedule", set_schedule_cmd))
    app.add_handler(CommandHandler("autopost_on", autopost_on_cmd))
    app.add_handler(CommandHandler("autopost_off", autopost_off_cmd))
    app.add_handler(CommandHandler("test_channel", test_channel_cmd))
    app.add_handler(CommandHandler("test_post", test_post_cmd))
    app.add_handler(CommandHandler("topics", topics_cmd))
    app.add_handler(CommandHandler("topic_add", topic_add_cmd))
    app.add_handler(CommandHandler("partners", partners_cmd))
    app.add_handler(CommandHandler("partner_add", partner_add_cmd))
    app.add_handler(CommandHandler("set_tourjin", set_tourjin_cmd))
    app.add_handler(CommandHandler("set_test_channel", set_test_channel_cmd))

    app.add_error_handler(error_handler)
    return app


def main():
    init_db()
    app = build_application()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
