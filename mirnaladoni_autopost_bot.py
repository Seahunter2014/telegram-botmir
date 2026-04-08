import asyncio
import logging
import os
import random
import sqlite3
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from dotenv import load_dotenv
from openai import AsyncOpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

# =========================
# Config
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "@NadoTurKrd")
TELEGRAM_ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0") or 0)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
POST_TIMES = os.getenv("POST_TIMES", "09:00,14:00,19:00")
DB_PATH = os.getenv("DB_PATH", "mirnaladoni_bot.db")
DEFAULT_MARKER = os.getenv("DEFAULT_MARKER", "98526")
CHANNEL_NAME = os.getenv("CHANNEL_NAME", "МирНаЛадони")
BOT_NAME = os.getenv("BOT_NAME", "Турджин")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")
AUTOPOST_ENABLED = os.getenv("AUTOPOST_ENABLED", "true").lower() == "true"

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
if not TELEGRAM_ADMIN_ID:
    raise RuntimeError("TELEGRAM_ADMIN_ID not set")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set")

client = AsyncOpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("mirnaladoni_bot")

# =========================
# Affiliate services from user file
# =========================
SERVICES = {
    "hotels": [
        {
            "name": "Островок",
            "url": "REPLACE_OSTROVOK_URL",
            "anchors": ["подобрать жильё", "посмотреть варианты проживания"],
        },
        {
            "name": "Отелло",
            "url": "https://otello.tp.st/lG6I9cfN",
            "anchors": ["найти отель", "посмотреть хорошие варианты"],
        },
        {
            "name": "Яндекс.Трэвел",
            "url": "https://yandex.tp.st/y94GSOah",
            "anchors": ["сравнить варианты размещения", "посмотреть жильё и билеты"],
        },
    ],
    "flights": [
        {
            "name": "Aviasales",
            "url": "https://aviasales.tp.st/hYipm2Ac",
            "anchors": ["посмотреть билеты", "проверить цены на перелёт"],
        }
    ],
    "lounges": [
        {
            "name": "VIP Залы",
            "url": "https://vip-zal.tp.st/VUTiM7FJ",
            "anchors": ["забронировать лаунж", "посмотреть доступные лаунжи"],
        }
    ],
    "tours": [
        {
            "name": "Onlinetours",
            "url": "https://onlinetours.tp.st/Um2ycow9",
            "anchors": ["подобрать тур онлайн", "посмотреть пакетные туры"],
        },
        {
            "name": "YouTravel",
            "url": "https://youtravel.tp.st/l1ay1eTX",
            "anchors": ["найти авторский тур", "посмотреть необычные маршруты"],
        },
        {
            "name": "Level.travel",
            "url": "https://level.tp.st/T6gVHplj",
            "anchors": ["сравнить туры", "посмотреть варианты тура"],
        },
        {
            "name": "Travelata",
            "url": "https://travelata.tp.st/O6m2Lg6H",
            "anchors": ["найти горящий тур", "посмотреть спецпредложения"],
        },
    ],
    "transport": [
        {
            "name": "Tutu.ru",
            "url": "https://tutu.tp.st/dZglLc7q",
            "anchors": ["посмотреть билеты на поезд или автобус", "сравнить варианты переезда"],
        },
        {
            "name": "Rail Europe",
            "url": "https://raileurope.tp.st/nWODZ4nI",
            "anchors": ["подобрать ж/д маршрут по Европе", "посмотреть европейские поезда"],
        },
    ],
    "insurance": [
        {
            "name": "TurStrahovka",
            "url": "https://ektatraveling.tp.st/5dN3sgg2",
            "anchors": ["оформить страховку", "сравнить полисы для поездки"],
        },
        {
            "name": "Cherehapa",
            "url": "https://cherehapa.tp.st/RIsddc4I",
            "anchors": ["проверить страховку", "подобрать страховой полис"],
        },
    ],
    "transfers": [
        {
            "name": "KiwiTaxi",
            "url": "https://kiwitaxi.tp.st/Ven9kvYz",
            "anchors": ["заказать трансфер", "заранее забронировать поездку из аэропорта"],
        },
        {
            "name": "GetTransfer",
            "url": "https://gettransfer.tp.st/XelV3vEQ",
            "anchors": ["посмотреть трансферы", "подобрать трансфер по маршруту"],
        },
    ],
    "car_rental": [
        {
            "name": "Qeeq",
            "url": "https://qeeq.tp.st/b1kQ7KBc",
            "anchors": ["посмотреть аренду авто", "сравнить прокат машин"],
        },
        {
            "name": "Localrent",
            "url": "https://localrent.tp.st/Q77W1ZWX",
            "anchors": ["найти авто у местных компаний", "подобрать машину на месте"],
        },
        {
            "name": "BikesBooking",
            "url": "https://bikesbooking.tp.st/eMN1TXvi",
            "anchors": ["взять байк или скутер", "посмотреть аренду двухколёсного транспорта"],
        },
    ],
    "excursions": [
        {
            "name": "Tripster",
            "url": "https://tripster.tp.st/FPqFOATh",
            "anchors": ["забронировать экскурсию", "посмотреть авторские прогулки"],
        },
        {
            "name": "Sputnik8",
            "url": "https://sputnik8.tp.st/FQZC0UxF",
            "anchors": ["найти экскурсии и активности", "посмотреть городские впечатления"],
        },
    ],
    "events": [
        {
            "name": "TicketNetwork",
            "url": "https://ticketnetwork.tp.st/evSOqzXe",
            "anchors": ["посмотреть билеты на события", "найти концерты и шоу"],
        },
        {
            "name": "Tiqets",
            "url": "https://tiqets.tp.st/Pe22NnHd",
            "anchors": ["купить билеты без очереди", "посмотреть музеи и парки"],
        },
    ],
}

CATEGORY_HINTS = {
    "hotels": ["отел", "гостин", "жиль", "апарт", "ночев", "прожив"],
    "flights": ["перелет", "авиабил", "рейс", "самолет", "аэропорт"],
    "lounges": ["лаунж", "бизнес-зал", "бизнес зал", "зал ожидания"],
    "tours": ["тур", "путев", "все включено", "горящ", "курорт"],
    "transport": ["поезд", "жд", "автобус", "маршрут", "rail"],
    "insurance": ["страхов", "полис", "медицина", "виз", "безопас"],
    "transfers": ["трансфер", "такси", "из аэропорта", "до отеля"],
    "car_rental": ["аренда авто", "машин", "скутер", "байк", "прокат"],
    "excursions": ["экскурс", "гид", "маршрут", "что посмотреть", "прогулк"],
    "events": ["событ", "концерт", "музей", "парк", "шоу", "билет"],
}

TOPIC_POOLS = {
    "value": [
        "5 ошибок при планировании отдыха в Турции",
        "Как сэкономить на поездке в Стамбул без потери впечатлений",
        "Что проверить перед поездкой в Таиланд",
        "Где летом комфортно отдыхать недорого",
        "Как выбрать жильё в популярном туристическом городе",
        "Что взять с собой в короткое путешествие на 3–4 дня",
    ],
    "engagement": [
        "В какой город Европы вы бы улетели на длинные выходные и почему",
        "Какой отдых вы выбираете: море, горы или город",
        "Самая недооценённая страна для короткого отпуска",
        "Какой ваш главный travel-фейл, который сейчас вспоминается с улыбкой",
    ],
    "expert": [
        "Как выбирать дату вылета, чтобы не переплачивать",
        "Когда пакетный тур выгоднее самостоятельной поездки",
        "Когда аренда авто реально оправдана в путешествии",
        "На чём чаще всего теряют деньги туристы перед поездкой",
    ],
    "trust": [
        "Почему хорошие поездки начинаются не с покупки билета",
        "Как не испортить отпуск из-за спешки в бронированиях",
        "Зачем заранее собирать план поездки, даже если любите спонтанность",
    ],
    "selling": [
        "Как найти выгодный тур на ближайшие даты",
        "Где смотреть перелёт и жильё, если хочется уложиться в бюджет",
        "Как быстро собрать поездку под ключ без лишней переплаты",
        "Что проверить перед бронированием экскурсии или трансфера",
    ],
    "experimental": [
        "Нестандартный маршрут выходного дня за пределами популярных направлений",
        "Мини-гид по городу, который часто недооценивают туристы",
        "Как превратить обычную поездку в запоминающееся путешествие",
    ],
}

LAST_ANCHORS_LIMIT = 5


# =========================
# Database
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            topic TEXT NOT NULL,
            post_type TEXT NOT NULL,
            goal TEXT NOT NULL,
            content TEXT NOT NULL,
            category TEXT,
            service_name TEXT,
            anchor_text TEXT,
            final_link TEXT,
            published INTEGER NOT NULL DEFAULT 0,
            published_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def save_setting(key: str, value: str) -> None:
    conn = db()
    conn.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


def get_setting(key: str, default: str = "") -> str:
    conn = db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def save_post(topic: str, post_type: str, goal: str, content: str, service_meta: dict | None) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO posts(created_at, topic, post_type, goal, content, category, service_name, anchor_text, final_link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.utcnow().isoformat(),
            topic,
            post_type,
            goal,
            content,
            service_meta.get("category") if service_meta else None,
            service_meta.get("name") if service_meta else None,
            service_meta.get("anchor") if service_meta else None,
            service_meta.get("final_link") if service_meta else None,
        ),
    )
    post_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(post_id)


def mark_published(post_id: int) -> None:
    conn = db()
    conn.execute(
        "UPDATE posts SET published = 1, published_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), post_id),
    )
    conn.commit()
    conn.close()


def get_post(post_id: int):
    conn = db()
    row = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    conn.close()
    return row


def get_last_post():
    conn = db()
    row = conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row


def get_recent_anchors(limit: int = LAST_ANCHORS_LIMIT) -> list[str]:
    conn = db()
    rows = conn.execute(
        "SELECT anchor_text FROM posts WHERE anchor_text IS NOT NULL ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [r["anchor_text"] for r in rows if r["anchor_text"]]


# =========================
# Helpers
# =========================
def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == TELEGRAM_ADMIN_ID)


def add_marker(url: str, marker: str = DEFAULT_MARKER) -> str:
    if url.startswith("REPLACE_"):
        return url
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["marker"] = marker
    new_query = urlencode(query)
    return urlunparse(parsed._replace(query=new_query))


def choose_post_type() -> str:
    roll = random.randint(1, 100)
    if roll <= 35:
        return "value"
    if roll <= 55:
        return "engagement"
    if roll <= 70:
        return "expert"
    if roll <= 80:
        return "trust"
    if roll <= 90:
        return "selling"
    return "experimental"


def choose_goal(post_type: str) -> str:
    mapping = {
        "value": "reach",
        "engagement": "engagement",
        "expert": "trust",
        "trust": "trust",
        "selling": "sale",
        "experimental": random.choice(["reach", "engagement", "click"]),
    }
    return mapping[post_type]


def choose_topic(post_type: str) -> str:
    return random.choice(TOPIC_POOLS[post_type])


def detect_category(topic: str, goal: str, post_type: str) -> str | None:
    text = f"{topic} {goal} {post_type}".lower()
    scores = {cat: 0 for cat in SERVICES.keys()}
    for category, hints in CATEGORY_HINTS.items():
        for hint in hints:
            if hint in text:
                scores[category] = scores.get(category, 0) + 1
    best_cat, best_score = max(scores.items(), key=lambda x: x[1])
    if best_score > 0:
        return best_cat
    if post_type == "selling" or goal in {"sale", "click"}:
        return random.choice(["tours", "flights", "hotels", "excursions"])
    return None


def choose_service(topic: str, goal: str, post_type: str) -> dict | None:
    category = detect_category(topic, goal, post_type)
    if not category:
        return None
    candidates = SERVICES.get(category, [])
    if not candidates:
        return None
    recent_anchors = set(get_recent_anchors())
    random.shuffle(candidates)
    for service in candidates:
        anchors = [a for a in service["anchors"] if a not in recent_anchors] or service["anchors"]
        anchor = random.choice(anchors)
        return {
            "category": category,
            "name": service["name"],
            "url": service["url"],
            "anchor": anchor,
            "final_link": add_marker(service["url"]),
        }
    return None


async def generate_post(topic: str, post_type: str, goal: str) -> tuple[str, dict | None]:
    service = None
    if post_type == "selling" or goal in {"sale", "click"}:
        service = choose_service(topic, goal, post_type)
    elif post_type in {"value", "expert", "experimental"} and random.random() < 0.35:
        service = choose_service(topic, goal, post_type)
    elif post_type in {"engagement", "trust"} and random.random() < 0.15:
        service = choose_service(topic, goal, post_type)

    monetization_block = "Не вставляй ссылок."
    if service:
        monetization_block = (
            f"Добавь ровно одну markdown-ссылку вида [{service['anchor']}]({service['final_link']}) "
            f"в естественную фразу по смыслу. Не упоминай название сервиса {service['name']}. "
            f"Не пиши, что ссылка партнерская. Ссылка должна смотреться нативно."
        )

    prompt = f"""
Ты редактор и growth-маркетолог Telegram-канала {CHANNEL_NAME} про путешествия.
Пиши только на русском языке.

Сделай один готовый Telegram-пост.

Тема: {topic}
Тип поста: {post_type}
Цель: {goal}

Обязательные правила:
- стиль живой, умный, полезный, уверенный
- никакого канцелярита
- никакой шаблонной AI-воды
- никаких хэштегов
- длина 900–1400 символов
- структура: сильный хук -> польза -> конкретика -> мягкий CTA
- можно 1–2 эмодзи, но умеренно
- можно нативно упомянуть канал {CHANNEL_NAME}
- можно упомянуть бота {BOT_NAME}, если это реально уместно
- НЕ упоминай бренды сервисов бронирования, билетов, экскурсий и т.п.
- не пиши, что это реклама
- не используй кликбейт уровня мусорного паблика

Монетизация:
{monetization_block}

Верни только готовый markdown-текст поста. Без пояснений.
"""

    response = await client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.9,
        messages=[
            {"role": "system", "content": "Ты сильный редактор Telegram и travel-копирайтер."},
            {"role": "user", "content": prompt},
        ],
    )
    text = response.choices[0].message.content.strip()
    return text, service


async def autopost_job(app: Application) -> None:
    post_type = choose_post_type()
    goal = choose_goal(post_type)
    topic = choose_topic(post_type)
    logger.info("Autopost started | type=%s | goal=%s | topic=%s", post_type, goal, topic)
    content, service = await generate_post(topic, post_type, goal)
    post_id = save_post(topic, post_type, goal, content, service)
    await app.bot.send_message(
        chat_id=TELEGRAM_CHANNEL_ID,
        text=content,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    mark_published(post_id)
    logger.info("Autopost published | post_id=%s", post_id)
    try:
        await app.bot.send_message(
            chat_id=TELEGRAM_ADMIN_ID,
            text=f"Автопост опубликован. ID: {post_id}\nТема: {topic}\nТип: {post_type}",
        )
    except Exception as exc:
        logger.warning("Cannot notify admin: %s", exc)


# =========================
# Telegram commands
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    await update.message.reply_text(
        "Бот запущен.\n\n"
        "/gen [тема] — сгенерировать пост\n"
        "/publish [id] — опубликовать пост вручную\n"
        "/last — показать последний пост\n"
        "/schedule — показать расписание\n"
        "/autopost_on — включить автопостинг\n"
        "/autopost_off — выключить автопостинг\n"
        "/test_channel — тест публикации в канал"
    )


async def gen_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    topic = " ".join(context.args).strip() or choose_topic("selling")
    post_type = choose_post_type()
    goal = choose_goal(post_type)
    await update.message.reply_text("Генерирую пост...")
    content, service = await generate_post(topic, post_type, goal)
    post_id = save_post(topic, post_type, goal, content, service)
    meta = [f"ID: {post_id}", f"Тип: {post_type}", f"Цель: {goal}"]
    if service:
        meta.append(f"Категория: {service['category']}")
        meta.append(f"Сервис: {service['name']} (внутренне)")
    await update.message.reply_text(
        content[:3900] + "\n\n" + "\n".join(meta),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


async def publish_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Пример: /publish 12")
        return
    post = get_post(int(context.args[0]))
    if not post:
        await update.message.reply_text("Пост не найден.")
        return
    await context.bot.send_message(
        chat_id=TELEGRAM_CHANNEL_ID,
        text=post["content"],
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    mark_published(post["id"])
    await update.message.reply_text(f"Пост {post['id']} опубликован в канал.")


async def last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    post = get_last_post()
    if not post:
        await update.message.reply_text("Постов пока нет.")
        return
    await update.message.reply_text(
        post["content"][:3900] + f"\n\nID: {post['id']}\nОпубликован: {'да' if post['published'] else 'нет'}",
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    status = get_setting("autopost_enabled", "true")
    await update.message.reply_text(
        f"Автопостинг: {'включен' if status == 'true' else 'выключен'}\n"
        f"Время публикаций: {POST_TIMES}\n"
        f"Канал: {TELEGRAM_CHANNEL_ID}"
    )


async def autopost_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    save_setting("autopost_enabled", "true")
    await update.message.reply_text("Автопостинг включен.")


async def autopost_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    save_setting("autopost_enabled", "false")
    await update.message.reply_text("Автопостинг выключен.")


async def test_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("Доступ только для администратора.")
        return
    await context.bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text="Тест публикации от бота МирНаЛадони ✅")
    await update.message.reply_text("Тест отправлен в канал.")


# =========================
# Scheduler
# =========================
def create_scheduler(app: Application) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    for time_str in [t.strip() for t in POST_TIMES.split(",") if t.strip()]:
        hour, minute = time_str.split(":")

        async def job_wrapper(application: Application = app):
            if get_setting("autopost_enabled", "true" if AUTOPOST_ENABLED else "false") != "true":
                logger.info("Autopost skipped: disabled")
                return
            try:
                await autopost_job(application)
            except Exception as exc:
                logger.exception("Autopost failed: %s", exc)
                try:
                    await application.bot.send_message(
                        chat_id=TELEGRAM_ADMIN_ID,
                        text=f"Ошибка автопостинга: {exc}",
                    )
                except Exception:
                    pass

        scheduler.add_job(job_wrapper, "cron", hour=int(hour), minute=int(minute), misfire_grace_time=300)

    return scheduler


async def post_init(app: Application) -> None:
    init_db()
    save_setting("autopost_enabled", "true" if AUTOPOST_ENABLED else "false")
    scheduler = create_scheduler(app)
    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logger.info("Scheduler started | times=%s", POST_TIMES)


async def main() -> None:
    init_db()
    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("gen", gen_cmd))
    application.add_handler(CommandHandler("publish", publish_cmd))
    application.add_handler(CommandHandler("last", last_cmd))
    application.add_handler(CommandHandler("schedule", schedule_cmd))
    application.add_handler(CommandHandler("autopost_on", autopost_on_cmd))
    application.add_handler(CommandHandler("autopost_off", autopost_off_cmd))
    application.add_handler(CommandHandler("test_channel", test_channel_cmd))

    logger.info("Bot starting...")
    await application.run_polling(close_loop=False)


if __name__ == "__main__":
    asyncio.run(main())
