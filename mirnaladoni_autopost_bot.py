import asyncio
import logging
import os
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Tuple

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

TELEGRAM_ADMIN_ID = int(TELEGRAM_ADMIN_ID_RAW) if TELEGRAM_ADMIN_ID_RAW else None

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHANNEL_ID:
    raise RuntimeError("Не задан TELEGRAM_CHANNEL_ID")
if not OPENAI_API_KEY:
    raise RuntimeError("Не задан OPENAI_API_KEY")

client = AsyncOpenAI(api_key=OPENAI_API_KEY)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bot.db"
PHOTOS_DIR = BASE_DIR / "photos"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("mirnaladoni_bot")

scheduler_instance: Optional[AsyncIOScheduler] = None


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
        "post_times": os.getenv("POST_TIMES", "09:00,14:00,19:00"),
        "default_ref_url": "https://t.me/TourJin_bot",
        "tourjin_bot_url": "https://t.me/TourJin_bot",
        "test_channel_id": TEST_CHANNEL_ID,
        "photo_mode": "local",
        "topic_mode": "mixed",
    }
    for k, v in defaults.items():
        if get_setting(k, "") == "":
            set_setting(k, v)


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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def normalize_text(s: str) -> str:
    return (s or "").lower().strip()


def extract_keywords(text: str) -> List[str]:
    raw = normalize_text(text)
    tokens = []
    for part in raw.replace("\n", " ").replace(",", " ").replace(".", " ").split():
        part = part.strip()
        if len(part) >= 3:
            tokens.append(part)
    return list(dict.fromkeys(tokens))


def choose_referral_url(topic: str, content: str) -> str:
    combined = f"{topic} {content}".lower()

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT name, keywords, url, marker FROM partners WHERE is_active=1 ORDER BY id ASC"
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


def choose_photo_for_topic(topic: str) -> Optional[Path]:
    if not PHOTOS_DIR.exists():
        return None

    topic_lower = normalize_text(topic)

    candidates: List[Path] = []
    for p in PHOTOS_DIR.rglob("*"):
        if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
            name = p.stem.lower()
            if any(word in name for word in extract_keywords(topic_lower)):
                candidates.append(p)

    if candidates:
        return random.choice(candidates)

    default_dir = PHOTOS_DIR / "default"
    if default_dir.exists():
        defaults = [
            p for p in default_dir.iterdir()
            if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
        ]
        if defaults:
            return random.choice(defaults)

    all_images = [
        p for p in PHOTOS_DIR.rglob("*")
        if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
    ]
    return random.choice(all_images) if all_images else None


def format_post_text(content: str, referral_url: str) -> str:
    cta = (
        "\n\n"
        "Хочешь подобрать вариант быстрее и без лишней суеты?\n"
        f"Попробуй: {referral_url}"
    )
    return f"{content.strip()}{cta}"


def parse_schedule(raw: str) -> List[Tuple[int, int]]:
    result = []
    items = [item.strip() for item in raw.split(",") if item.strip()]
    for item in items:
        hour_str, minute_str = item.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
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
    status: str = "draft",
) -> int:
    with closing(db()) as conn:
        cur = conn.execute("""
            INSERT INTO posts(
                topic, theme_source, goal, post_type, content,
                referral_url, photo_path, status, created_at, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            topic,
            theme_source,
            goal,
            post_type,
            content,
            referral_url,
            photo_path,
            status,
            now_iso(),
            None,
        ))
        conn.commit()
        return cur.lastrowid


def update_post_status(post_id: int, status: str):
    with closing(db()) as conn:
        published_at = now_iso() if status == "published" else None
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
Ты пишешь Telegram-пост для travel-канала "Мир на ладони".

Тема: {topic}

Требования:
- язык: русский
- стиль: живой, экспертный, доверительный
- объём: 900–1400 символов
- без воды и канцелярита
- без эмодзи-перегруза
- сделать пост полезным, а не абстрактным
- в конце мягкий призыв к действию
- не вставляй markdown-звёздочки
- не выдумывай цены и факты, если они не даны
- можно упомянуть помощника для подбора туров: {tourjin_url}
- текст должен быть оригинальным, не похожим на копипаст

Структура:
1. сильный заход
2. польза/советы
3. короткий вывод
4. мягкий CTA
""".strip()

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.9,
        messages=[
            {"role": "system", "content": "Ты сильный редактор travel-контента для Telegram."},
            {"role": "user", "content": prompt},
        ],
    )

    text = response.choices[0].message.content or ""
    text = text.strip()
    if not text:
        raise ValueError("OpenAI вернул пустой текст")
    return text


async def generate_post_with_retry(topic: str, retries: int = 3, delay: int = 4) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return await generate_post_via_openai(topic)
        except Exception as e:
            last_error = e
            logger.exception("Ошибка генерации поста | topic=%s | attempt=%s/%s", topic, attempt, retries)
            if attempt < retries:
                await asyncio.sleep(delay)
    raise last_error


async def publish_post_record(bot, channel_id: str, row, mark_status: str = "published"):
    text = format_post_text(row["content"], row["referral_url"] or get_setting("default_ref_url"))

    photo_path = row["photo_path"]
    if photo_path and Path(photo_path).exists():
        with open(photo_path, "rb") as f:
            await bot.send_photo(chat_id=channel_id, photo=f, caption=text[:1024])
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
        f"Опубликован: {'да' if row['status'] == 'published' else 'нет'}\n"
        f"Реф-ссылка: {row['referral_url'] or '-'}\n\n"
        f"{row['content']}"
    )


def rebuild_scheduler(app: Application):
    global scheduler_instance

    if scheduler_instance:
        try:
            scheduler_instance.remove_all_jobs()
            scheduler_instance.shutdown(wait=False)
        except Exception:
            logger.exception("Не удалось корректно остановить старый scheduler")

    scheduler_instance = AsyncIOScheduler(timezone="Europe/Berlin")

    autopost_enabled = get_setting("autopost_enabled", "1") == "1"
    schedule_raw = get_setting("post_times", "09:00,14:00,19:00")

    if autopost_enabled:
        for hour, minute in parse_schedule(schedule_raw):
            scheduler_instance.add_job(
                scheduled_autopost_job,
                "cron",
                hour=hour,
                minute=minute,
                args=[app],
                id=f"autopost_{hour:02d}_{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )

    scheduler_instance.start()
    logger.info("Scheduler rebuilt | enabled=%s | times=%s", autopost_enabled, schedule_raw)


async def scheduled_autopost_job(app: Application):
    if get_setting("autopost_enabled", "1") != "1":
        logger.info("Автопостинг отключён, задача пропущена")
        return

    topic, source_type = get_next_topic()
    content = await generate_post_with_retry(topic)
    referral_url = choose_referral_url(topic, content)
    photo_path = choose_photo_for_topic(topic)
    post_id = save_post(
        topic=topic,
        theme_source=source_type,
        goal="trust",
        post_type="auto",
        content=content,
        referral_url=referral_url,
        photo_path=str(photo_path) if photo_path else None,
        status="draft",
    )
    row = get_post(post_id)
    await publish_post_record(app.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
    logger.info("Автопост опубликован | id=%s | topic=%s", post_id, topic)


@admin_only
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        "Бот работает.\n\n"
        "Главные команды:\n"
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
        "/set_tourjin https://t.me/TourJin_bot — обновить ссылку"
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

    content = await generate_post_with_retry(topic)
    referral_url = choose_referral_url(topic, content)
    photo_path = choose_photo_for_topic(topic)

    post_id = save_post(
        topic=topic,
        theme_source=source_type,
        goal="trust",
        post_type="expert",
        content=content,
        referral_url=referral_url,
        photo_path=str(photo_path) if photo_path else None,
        status="draft",
    )

    row = get_post(post_id)
    msg = (
        f"Пост создан.\n\n"
        f"{format_post_card(row)}\n\n"
        f"Для публикации: /publish {post_id}\n"
        f"Для теста: /test_post {post_id}"
    )
    await safe_reply(update, msg)


@admin_only
async def publish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await safe_reply(update, "Пример: /publish 12")
        return

    post_id = int(context.args[0])
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
    schedule_raw = get_setting("post_times", "09:00,14:00,19:00")
    await safe_reply(
        update,
        f"Автопостинг: {enabled}\nВремя публикаций: {schedule_raw}\n\n"
        f"Изменить: /set_schedule 09:00,14:00,19:00"
    )


@admin_only
async def set_schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = " ".join(context.args).strip()
    if not raw:
        await safe_reply(update, "Пример: /set_schedule 08:30,13:00,18:45")
        return

    parse_schedule(raw)
    set_setting("post_times", raw)
    rebuild_scheduler(context.application)
    await safe_reply(update, f"Расписание обновлено: {raw}")


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
        topic = "Тестовый пост для проверки канала"
        content = await generate_post_with_retry(topic)
        referral_url = choose_referral_url(topic, content)
        photo_path = choose_photo_for_topic(topic)

        post_id = save_post(
            topic=topic,
            theme_source="test",
            goal="test",
            post_type="test",
            content=content,
            referral_url=referral_url,
            photo_path=str(photo_path) if photo_path else None,
            status="draft",
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

    post_id = int(context.args[0])
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
        await safe_reply(
            update,
            "Бот не может публиковать в тестовый канал. Проверьте членство и права."
        )


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
                text="Произошла ошибка. Она записана в логи. Меню возвращено.",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не удалось отправить сообщение об ошибке пользователю")


def build_application() -> Application:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

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
    rebuild_scheduler(app)
    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
