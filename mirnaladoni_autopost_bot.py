import asyncio
import html
import logging
import os
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, ReplyKeyboardMarkup
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

APP_VERSION = "travel-matrix-v3-2026-04-11"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID", "").strip()
TELEGRAM_ADMIN_ID_RAW = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "").strip()
DEFAULT_POST_TIMES = os.getenv("POST_TIMES", "09:00,14:00,19:00").strip()

BOT_TZ = timezone(timedelta(hours=3))
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
CACHE_DIR = PHOTOS_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("mirnaladoni_bot")

scheduler_instance: Optional[AsyncIOScheduler] = None

CONTENT_PLAN = {
    "value": 35,
    "engagement": 20,
    "expert": 15,
    "trust": 10,
    "selling": 15,
    "experimental": 5,
}

SERVICES: List[Dict[str, Any]] = [
    {
        "key": "ostrovok",
        "url": "https://ostrovok.tp.st/yHBoZUg7",
        "groups": ["hotels"],
        "anchor_options": ["варианты отелей", "подходящее жильё", "варианты проживания"],
    },
    {
        "key": "aviasales",
        "url": "https://aviasales.tp.st/hYipm2Ac",
        "groups": ["flights"],
        "anchor_options": ["авиабилеты", "билеты на перелёт", "варианты перелёта"],
    },
    {
        "key": "onlinetours",
        "url": "https://onlinetours.tp.st/Um2ycow9",
        "groups": ["tours"],
        "anchor_options": ["пакетные туры", "варианты тура", "туры по направлению"],
    },
    {
        "key": "travelata",
        "url": "https://travelata.tp.st/O6m2Lg6H",
        "groups": ["tours"],
        "anchor_options": ["горящие туры", "туры со скидкой", "интересные туры"],
    },
    {
        "key": "tutu",
        "url": "https://tutu.tp.st/dZglLc7q",
        "groups": ["ground_transport"],
        "anchor_options": ["билеты на транспорт", "варианты переезда", "билеты на поезд или автобус"],
    },
    {
        "key": "rail_europe",
        "url": "https://raileurope.tp.st/nWODZ4nI",
        "groups": ["rail_europe"],
        "anchor_options": ["жд билеты по Европе", "поезда по Европе", "европейские маршруты"],
    },
    {
        "key": "cherehapa",
        "url": "https://cherehapa.tp.st/RIsddc4I",
        "groups": ["insurance"],
        "anchor_options": ["страховку для поездки", "подходящий страховой полис", "варианты страховки"],
    },
    {
        "key": "kiwitaxi",
        "url": "https://kiwitaxi.tp.st/Ven9kvYz",
        "groups": ["transfer"],
        "anchor_options": ["трансфер из аэропорта", "варианты трансфера", "поездку из аэропорта"],
    },
    {
        "key": "localrent",
        "url": "https://localrent.tp.st/Q77W1ZWX",
        "groups": ["car_rental"],
        "anchor_options": ["аренду авто", "машину для поездки", "варианты проката авто"],
    },
    {
        "key": "bikesbooking",
        "url": "https://bikesbooking.tp.st/eMN1TXvi",
        "groups": ["bike_rental"],
        "anchor_options": ["прокат байка или скутера", "двухколёсный транспорт", "прокат скутера"],
    },
    {
        "key": "sputnik8",
        "url": "https://sputnik8.tp.st/FQZC0UxF",
        "groups": ["excursions"],
        "anchor_options": ["экскурсии по месту", "варианты экскурсий", "что посмотреть на месте"],
    },
    {
        "key": "ticketnetwork",
        "url": "https://ticketnetwork.tp.st/evSOqzXe",
        "groups": ["events"],
        "anchor_options": ["билеты на мероприятия", "события по направлению", "интересные мероприятия"],
    },
    {
        "key": "trip_cruises",
        "url": "https://www.trip.com/t/OjTmFMTwFU2",
        "groups": ["cruises"],
        "anchor_options": ["варианты круизов", "круизы по направлению", "подходящий круиз"],
    },
    {
        "key": "vip_zal",
        "url": "https://vip-zal.tp.st/VUTiM7FJ",
        "groups": ["airport"],
        "anchor_options": ["доступ в лаунж", "варианты бизнес-залов", "лаунж-залы в аэропорту"],
    },
]

TOPIC_GROUP_RULES = {
    "hotels": ["отель", "отели", "гостиница", "гостиницы", "проживание", "жилье", "жильё", "апартаменты"],
    "flights": ["билет", "билеты", "перелет", "перелёт", "авиабилеты", "самолет", "самолёт", "рейс"],
    "tours": ["тур", "туры", "путевка", "путёвка", "горящий тур", "горящие туры", "all inclusive"],
    "insurance": ["страховка", "страхование", "полис", "страховой"],
    "transfer": ["трансфер", "аэропорт", "из аэропорта", "в аэропорт"],
    "car_rental": ["аренда авто", "машина", "прокат авто", "арендовать машину"],
    "bike_rental": ["байк", "скутер", "мотоцикл", "мопед", "багги"],
    "excursions": ["экскурсия", "экскурсии", "гид", "достопримечательности", "что посмотреть", "прогулка"],
    "events": ["концерт", "шоу", "мероприятие", "событие", "матч", "спектакль"],
    "cruises": ["круиз", "круизы", "лайнер"],
    "ground_transport": ["поезд", "поезда", "автобус", "автобусы", "электричка", "жд"],
    "rail_europe": ["европа", "поезд по европе", "поезда европы"],
    "airport": ["лаунж", "бизнес-зал", "пересадка", "долгая пересадка"],
}

STYLE_VARIANTS = {
    "value": ["полезный список", "антиошибка", "разбор одной проблемы"],
    "engagement": ["вопрос с выбором", "спорный тезис", "контраст двух подходов"],
    "expert": ["экспертный нюанс", "разбор типичной ошибки", "неочевидимый критерий выбора"],
    "trust": ["личное наблюдение", "человеческий вывод", "спокойная история-мысль"],
    "selling": ["проблема → решение", "потери времени/денег → как избежать", "когда стоит посмотреть варианты заранее"],
    "experimental": ["неочевидный ракурс", "мини-гипотеза", "парадокс в travel-теме"],
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {col[1] for col in cols}
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


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
                monetization_service TEXT,
                photo_path TEXT,
                photo_credit TEXT,
                photo_source_url TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL,
                published_at TEXT
            )
        """)

        ensure_column(conn, "posts", "referral_url", "TEXT")
        ensure_column(conn, "posts", "monetization_service", "TEXT")
        ensure_column(conn, "posts", "photo_path", "TEXT")
        ensure_column(conn, "posts", "photo_credit", "TEXT")
        ensure_column(conn, "posts", "photo_source_url", "TEXT")
        ensure_column(conn, "posts", "status", "TEXT NOT NULL DEFAULT 'draft'")
        ensure_column(conn, "posts", "published_at", "TEXT")

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
            CREATE TABLE IF NOT EXISTS content_stats(
                post_type TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.commit()

    ensure_default_settings()
    ensure_default_topics()
    ensure_default_content_stats()


def ensure_default_content_stats():
    with closing(db()) as conn:
        for post_type in CONTENT_PLAN.keys():
            conn.execute(
                """
                INSERT INTO content_stats(post_type, count)
                VALUES(?, 0)
                ON CONFLICT(post_type) DO NOTHING
                """,
                (post_type,),
            )
        conn.commit()


def increment_post_type_stat(post_type: str):
    with closing(db()) as conn:
        conn.execute(
            "UPDATE content_stats SET count = count + 1 WHERE post_type = ?",
            (post_type,),
        )
        conn.commit()


def get_content_stats() -> Dict[str, int]:
    with closing(db()) as conn:
        rows = conn.execute("SELECT post_type, count FROM content_stats").fetchall()
    stats = {row["post_type"]: row["count"] for row in rows}
    for post_type in CONTENT_PLAN.keys():
        stats.setdefault(post_type, 0)
    return stats


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
        "test_channel_id": TEST_CHANNEL_ID,
        "photo_attribution_mode": "0",
        "pexels_orientation": "landscape",
        "pexels_size": "large",
        "pexels_per_page": "10",
    }
    for key, value in defaults.items():
        if get_setting(key, "") == "":
            set_setting(key, value)


def ensure_default_topics():
    default_topics = [
        "5 ошибок при бронировании отпуска",
        "Как выбрать идеальный отель без разочарований",
        "Когда лучше покупать билеты",
        "Что проверить перед поездкой за границу",
        "Как не переплатить за отдых",
        "Как выбрать страховку в поездку",
        "Что важно знать про трансфер из аэропорта",
        "Где туристы чаще всего теряют деньги в отпуске",
        "Как не испортить первый день отдыха",
        "Куда поехать на море недорого",
        "Как сэкономить на экскурсиях",
        "Семейный отдых: что важно предусмотреть заранее",
        "Почему дешёвый тур иногда выходит дороже",
        "Как не ошибиться с отелем в первый раз",
        "Нужна ли страховка, если летишь всего на неделю",
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
            ["/gen", "/last", "/version"],
            ["/schedule", "/autopost_on", "/autopost_off"],
            ["/test_channel", "/topics", "/menu"],
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


def escape_html_text(text: str) -> str:
    return html.escape(text or "", quote=False)


def slugify(text: str, max_len: int = 60) -> str:
    text = text.lower().strip()
    text = text.replace("ё", "e")
    text = re.sub(r"[^a-zA-Zа-яА-Я0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:max_len] or "photo"


def cleanup_post_text(text: str) -> str:
    text = text.replace("***", "").replace("**", "").replace("__", "")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"^([^\w\s])(\d)", r"\1 \2", text)
    return text.strip()


def looks_incomplete(text: str) -> bool:
    stripped = text.strip()
    if len(stripped) < 340:
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


def get_recent_posts(limit: int = 7):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT id, topic, post_type, monetization_service, content FROM posts ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()


def choose_balanced_post_type() -> str:
    stats = get_content_stats()
    total = sum(stats.values())
    recent = get_recent_posts(5)
    recent_types = [row["post_type"] for row in recent if row["post_type"]]

    deficits = {}
    if total == 0:
        for k, v in CONTENT_PLAN.items():
            deficits[k] = v
    else:
        for post_type, target_pct in CONTENT_PLAN.items():
            actual_pct = (stats[post_type] / total) * 100 if total else 0
            deficits[post_type] = target_pct - actual_pct

    for ptype in deficits:
        if recent_types and recent_types[0] == ptype:
            deficits[ptype] -= 20
        if recent_types.count(ptype) >= 2:
            deficits[ptype] -= 10

    max_score = max(deficits.values())
    candidates = [ptype for ptype, score in deficits.items() if score == max_score]
    return random.choice(candidates)


def should_insert_monetization(post_type: str, recent_posts) -> bool:
    if post_type in {"engagement", "trust"}:
        return False

    recent_with_links = sum(1 for row in recent_posts[:3] if row["monetization_service"])
    if recent_with_links >= 1:
        return False

    if post_type == "selling":
        return True
    if post_type == "value":
        return random.random() < 0.35
    if post_type == "expert":
        return random.random() < 0.35
    if post_type == "experimental":
        return random.random() < 0.20
    return False


def choose_discussion_cta(post_type: str, recent_posts) -> str:
    ctas = {
        "engagement": [
            "А вы бы что выбрали?",
            "У кого было иначе?",
            "Или я один так думаю?",
            "Что у вас обычно срабатывает лучше?",
        ],
        "value": [
            "Сохрани, чтобы не потерять.",
            "Что бы ты сюда добавил?",
            "У тебя было что-то похожее?",
        ],
        "expert": [
            "Вы с этим согласны?",
            "У кого был другой опыт?",
            "Что для вас здесь решает всё?",
        ],
        "trust": [
            "У вас было что-то похожее?",
            "Кто тоже через это проходил?",
            "У кого такая история уже случалась?",
        ],
        "selling": [
            "Если тема актуальна — лучше сохранить пост заранее.",
            "Такое обычно вспоминают слишком поздно, поэтому лучше сохранить.",
            "Отправь тому, кто как раз планирует поездку.",
        ],
        "experimental": [
            "Как вам такой формат?",
            "Продолжать такие разборы?",
            "Такой угол полезен или спорно?",
        ],
    }

    recent_text = " ".join([(row["content"] or "")[-120:] for row in recent_posts[:3]])
    variants = ctas.get(post_type, ctas["value"])
    filtered = [cta for cta in variants if cta not in recent_text]
    return random.choice(filtered or variants)


def topic_groups(topic: str, content: str = "") -> List[str]:
    combined = f"{topic} {content}".lower()
    groups = []
    for group, keywords in TOPIC_GROUP_RULES.items():
        if any(kw in combined for kw in keywords):
            groups.append(group)
    return groups


def choose_service(topic: str, content: str, post_type: str, recent_posts) -> Optional[Dict[str, Any]]:
    if not should_insert_monetization(post_type, recent_posts):
        return None

    groups = topic_groups(topic, content)
    if not groups:
        return None

    candidates = [s for s in SERVICES if any(g in s["groups"] for g in groups)]
    if not candidates:
        return None

    last_service = recent_posts[0]["monetization_service"] if recent_posts and recent_posts[0]["monetization_service"] else ""
    non_repeated = [s for s in candidates if s["key"] != last_service]
    if non_repeated:
        candidates = non_repeated

    return random.choice(candidates)


def choose_anchor_text(service: Dict[str, Any]) -> str:
    return random.choice(service["anchor_options"])


def build_native_link_paragraph(service: Dict[str, Any], post_type: str) -> str:
    url = service["url"]
    anchor = escape_html_text(choose_anchor_text(service))

    soft_templates = [
        f'Если хочется спокойно сравнить варианты — можно посмотреть <a href="{url}">{anchor}</a>.',
        f'Обычно в таких случаях удобнее заранее проверить <a href="{url}">{anchor}</a>.',
        f'Чтобы не тратить время на хаотичный поиск, можно открыть <a href="{url}">{anchor}</a>.',
    ]

    selling_templates = [
        f'Когда вопрос уже актуален, проще сразу посмотреть <a href="{url}">{anchor}</a>.',
        f'В такой ситуации логичнее заранее открыть <a href="{url}">{anchor}</a>, чем потом выбирать в спешке.',
        f'Если не хочется терять время на лишние сравнения, можно быстро проверить <a href="{url}">{anchor}</a>.',
    ]

    return random.choice(selling_templates if post_type == "selling" else soft_templates)


def build_pexels_queries(topic: str) -> List[str]:
    topic_lower = normalize_text(topic)
    base = [
        topic_lower,
        f"{topic_lower} travel",
        f"{topic_lower} vacation",
        f"{topic_lower} tourism",
    ]

    mappings = [
        (["турция", "turkey"], ["turkey beach resort", "antalya resort"]),
        (["егип", "egypt"], ["egypt resort sea", "egypt beach vacation"]),
        (["таиланд", "thai", "thailand"], ["thailand beach resort", "phuket travel"]),
        (["отель", "hotel"], ["hotel room travel", "resort hotel"]),
        (["море", "пляж"], ["beach resort", "sea vacation"]),
        (["семей", "дет"], ["family vacation beach", "family travel"]),
        (["роман", "пара"], ["romantic vacation beach", "couple resort"]),
        (["билет", "самолет", "перелет"], ["airport travel", "airplane window travel"]),
        (["экскур", "достопримеч"], ["city tour travel", "tourist sightseeing"]),
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
    if not PEXELS_API_KEY:
        return None, None, None

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


def post_type_instructions(post_type: str, style_variant: str) -> str:
    mapping = {
        "value": f"Сделай {style_variant}. Дай конкретную пользу, но без ощущения учебника.",
        "engagement": f"Сделай {style_variant}. Главная цель — вызвать обсуждение или спор, а не объяснить всё до конца.",
        "expert": f"Сделай {style_variant}. Покажи нюанс, который обычно упускают.",
        "trust": f"Сделай {style_variant}. Тон спокойный и человеческий, как личное наблюдение.",
        "selling": f"Сделай {style_variant}. Мягко подведи к решению, но без ощущения рекламы.",
        "experimental": f"Сделай {style_variant}. Можно использовать необычный ракурс или нестандартную подачу.",
    }
    return mapping.get(post_type, mapping["value"])


async def generate_post_via_openai(topic: str, post_type: str, style_variant: str) -> str:
    style_seed = random.choice([
        "пиши как автор travel-канала, который реально летает и наблюдает",
        "пиши живо, как будто делишься выводом с подписчиком",
        "пиши легко, без ощущения статьи",
        "пиши так, чтобы текст хотелось дочитать в ленте Telegram",
    ])

    prompt = f"""
Ты пишешь Telegram-пост для travel-канала «Мир на ладони».

Тема: {topic}
Тип поста: {post_type}
Формат: {style_variant}

Требования:
- язык: русский
- стиль: живой, человеческий, как автор Telegram-канала
- {style_seed}
- умеренные эмодзи: 1–2, максимум 3
- никакого канцелярита, штампов и AI-воды
- не упоминай бренды сервисов
- не вставляй ссылки
- не делай прямых продаж
- длина: 520–740 символов
- структура:
  1) короткий заголовок
  2) короткий заход
  3) 2–3 коротких смысловых блока
  4) короткий вывод
- каждый блок короткий и читаемый с телефона
- допускаются разные типы подзаголовков
- не используй markdown со звёздочками
- текст должен быть законченным и не ощущаться шаблонным

Инструкция:
{post_type_instructions(post_type, style_variant)}

Верни только готовый текст поста.
""".strip()

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=1.0,
        max_tokens=850,
        messages=[
            {"role": "system", "content": "Ты сильный growth-редактор Telegram-канала про путешествия."},
            {"role": "user", "content": prompt},
        ],
    )

    text = (response.choices[0].message.content or "").strip()
    if not text:
        raise ValueError("OpenAI вернул пустой текст")

    return cleanup_post_text(text)


async def generate_post_with_retry(topic: str, post_type: str, style_variant: str, retries: int = 3, delay: int = 4) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            text = await generate_post_via_openai(topic, post_type, style_variant)
            if looks_incomplete(text):
                raise ValueError("Сгенерированный текст выглядит незавершённым")
            return text
        except Exception as exc:
            last_error = exc
            logger.exception(
                "Ошибка генерации поста | topic=%s | type=%s | style=%s | attempt=%s/%s",
                topic,
                post_type,
                style_variant,
                attempt,
                retries,
            )
            if attempt < retries:
                await asyncio.sleep(delay)

    raise last_error


def convert_plain_text_to_html(post_text: str) -> str:
    text = (post_text or "").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not parts:
        return ""

    html_blocks = []
    title = escape_html_text(parts[0])
    html_blocks.append(f"<b>{title}</b>")

    subheading_pattern = re.compile(
        r"^(Ошибка\s*\d+|Совет\s*\d+|Шаг\s*\d+|Пункт\s*\d+|Момент\s*\d+|Что важно|На что смотреть|Что проверить|Почему это важно)\s*[:.-]?\s*(.*)$",
        re.IGNORECASE,
    )

    for block in parts[1:]:
        escaped = escape_html_text(block)
        lines = [line.strip() for line in escaped.split("\n") if line.strip()]

        if len(lines) == 1:
            m = subheading_pattern.match(html.unescape(lines[0]))
            if m:
                head = escape_html_text(m.group(1))
                tail = escape_html_text(m.group(2))
                html_blocks.append(f"<b>{head}</b>\n{tail}".strip())
            else:
                html_blocks.append(lines[0])
            continue

        first_line = lines[0]
        m = subheading_pattern.match(html.unescape(first_line))
        if m:
            head = escape_html_text(m.group(1))
            tail = escape_html_text(m.group(2))
            rest = "\n".join(lines[1:])
            block_html = f"<b>{head}</b>"
            if tail:
                block_html += f"\n{tail}"
            if rest:
                block_html += f"\n{rest}"
            html_blocks.append(block_html.strip())
        else:
            html_blocks.append("\n".join(lines))

    return "\n\n".join(html_blocks).strip()


def build_closing_blocks(post_type: str, recent_posts) -> List[str]:
    closing_map = {
        "value": [
            "Сохрани, чтобы не потерять.",
            "Что бы ты сюда добавил?",
            "У тебя было что-то похожее?",
        ],
        "engagement": [
            "А вы бы что выбрали?",
            "У кого было иначе?",
            "Или я один так думаю?",
        ],
        "expert": [
            "Вы с этим согласны?",
            "У кого был другой опыт?",
            "Что для вас здесь решает всё?",
        ],
        "trust": [
            "У вас было что-то похожее?",
            "Кто тоже через это проходил?",
            "У кого такая история уже случалась?",
        ],
        "selling": [
            "Если тема актуальна — лучше сохранить пост заранее.",
            "Такое обычно вспоминают слишком поздно, поэтому лучше сохранить.",
            "Отправь тому, кто как раз планирует поездку.",
        ],
        "experimental": [
            "Как вам такой формат?",
            "Продолжать такие разборы?",
            "Такой угол полезен или спорно?",
        ],
    }

    recent_text = " ".join([(row["content"] or "")[-120:] for row in recent_posts[:3]])
    variants = [c for c in closing_map.get(post_type, closing_map["value"]) if c not in recent_text]
    base = variants or closing_map.get(post_type, closing_map["value"])
    count = 1 if post_type in {"selling", "trust"} else random.choice([1, 2])
    return random.sample(base, k=min(count, len(base)))


def format_post_text(
    content: str,
    post_type: str,
    service: Optional[Dict[str, Any]],
    recent_posts,
    photo_credit: Optional[str],
    photo_source_url: Optional[str],
) -> str:
    base_html = convert_plain_text_to_html(content)
    blocks = []

    if service:
        blocks.append(build_native_link_paragraph(service, post_type))

    for closing in build_closing_blocks(post_type, recent_posts):
        blocks.append(escape_html_text(closing))

    credit_block = ""
    if get_setting("photo_attribution_mode", "0") == "1" and photo_credit:
        credit_text = escape_html_text(photo_credit)
        credit_block = f"\n\n{credit_text}"
        if photo_source_url:
            credit_block += f'\n<a href="{photo_source_url}">Источник фото</a>'

    ending_html = "\n\n".join(blocks).strip()

    plain_base = re.sub(r"<[^>]+>", "", base_html)
    plain_ending = re.sub(r"<[^>]+>", "", ending_html)
    plain_credit = re.sub(r"<[^>]+>", "", credit_block)

    max_base_len = 1024 - len(plain_ending) - len(plain_credit) - 4
    trimmed_plain_base = trim_to_limit(plain_base, max(260, max_base_len))
    base_html = convert_plain_text_to_html(trimmed_plain_base)

    final_html = f"{base_html}\n\n{ending_html}{credit_block}".strip() if ending_html else f"{base_html}{credit_block}".strip()

    final_plain = re.sub(r"<[^>]+>", "", final_html)
    if len(final_plain) > 1024:
        overflow = len(final_plain) - 1024
        trimmed_plain_base = trim_to_limit(trimmed_plain_base, max(220, len(trimmed_plain_base) - overflow - 10))
        base_html = convert_plain_text_to_html(trimmed_plain_base)
        final_html = f"{base_html}\n\n{ending_html}{credit_block}".strip() if ending_html else f"{base_html}{credit_block}".strip()

    return final_html


def save_post(
    topic: str,
    theme_source: str,
    goal: str,
    post_type: str,
    content: str,
    referral_url: str,
    monetization_service: str,
    photo_path: Optional[str],
    photo_credit: Optional[str],
    photo_source_url: Optional[str],
    status: str = "draft",
) -> int:
    with closing(db()) as conn:
        cur = conn.execute("""
            INSERT INTO posts(
                topic, theme_source, goal, post_type, content,
                referral_url, monetization_service,
                photo_path, photo_credit, photo_source_url,
                status, created_at, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            topic,
            theme_source,
            goal,
            post_type,
            content,
            referral_url,
            monetization_service,
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


def get_recent_posts(limit: int = 7):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT id, topic, post_type, monetization_service, content FROM posts ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()


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
        "5 ошибок при бронировании отпуска",
        "Как выбрать идеальный отель без разочарований",
        "Когда лучше покупать билеты",
        "Что проверить перед поездкой за границу",
        "Как не переплатить за отдых",
    ])
    return fallback, "fallback"


def format_post_card(row) -> str:
    if not row:
        return "Постов пока нет."

    return (
        f"Версия: {APP_VERSION}\n"
        f"ID: {row['id']}\n"
        f"Тема: {row['topic'] or '-'}\n"
        f"Источник темы: {row['theme_source'] or '-'}\n"
        f"Тип: {row['post_type'] or '-'}\n"
        f"Цель: {row['goal'] or '-'}\n"
        f"Статус: {row['status']}\n"
        f"Сервис: {row['monetization_service'] or '-'}\n"
        f"Реф-ссылка: {row['referral_url'] or '-'}\n"
        f"Фото: {'да' if row['photo_path'] else 'нет'}\n\n"
        f"{row['content']}"
    )


async def create_post_record(topic: str, source_type: str, goal: str, forced_post_type: Optional[str] = None) -> int:
    recent_posts = get_recent_posts(7)
    post_type = forced_post_type or choose_balanced_post_type()

    style_candidates = STYLE_VARIANTS.get(post_type, ["полезный список"])
    recent_text = " ".join([(row["content"] or "")[:80] for row in recent_posts[:3]])
    available_styles = [s for s in style_candidates if s not in recent_text]
    style_variant = random.choice(available_styles or style_candidates)

    content = await generate_post_with_retry(topic, post_type, style_variant)
    service = choose_service(topic, content, post_type, recent_posts)

    referral_url = service["url"] if service else ""
    monetization_service = service["key"] if service else ""

    logger.info(
        "POST_DECISION | version=%s | topic=%s | type=%s | style=%s | service=%s | url=%s",
        APP_VERSION,
        topic,
        post_type,
        style_variant,
        monetization_service or "none",
        referral_url or "none",
    )

    photo_path, photo_credit, photo_source_url = await fetch_pexels_photo(topic)

    post_id = save_post(
        topic=topic,
        theme_source=source_type,
        goal=goal,
        post_type=post_type,
        content=content,
        referral_url=referral_url,
        monetization_service=monetization_service,
        photo_path=photo_path,
        photo_credit=photo_credit,
        photo_source_url=photo_source_url,
        status="draft",
    )

    increment_post_type_stat(post_type)
    return post_id


async def publish_post_record(bot, channel_id: str, row, mark_status: str = "published"):
    recent_posts = get_recent_posts(7)
    service = None
    if row["monetization_service"]:
        for s in SERVICES:
            if s["key"] == row["monetization_service"]:
                service = s
                break

    text = format_post_text(
        content=row["content"],
        post_type=row["post_type"] or "value",
        service=service,
        recent_posts=recent_posts,
        photo_credit=row["photo_credit"],
        photo_source_url=row["photo_source_url"],
    )

    photo_path = row["photo_path"]
    if photo_path and Path(photo_path).exists():
        with open(photo_path, "rb") as photo_file:
            await bot.send_photo(
                chat_id=channel_id,
                photo=photo_file,
                caption=text,
                parse_mode="HTML",
            )
    else:
        await bot.send_message(
            chat_id=channel_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=False,
        )

    update_post_status(row["id"], mark_status)


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


async def scheduled_autopost_job(app: Application):
    if get_setting("autopost_enabled", "1") != "1":
        logger.info("Автопостинг отключён, задача пропущена")
        return

    logger.info("Запуск scheduled_autopost_job")
    topic, source_type = get_next_topic()
    post_id = await create_post_record(
        topic=topic,
        source_type=source_type,
        goal="autopost",
        forced_post_type=None,
    )
    row = get_post(post_id)
    await publish_post_record(app.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
    logger.info(
        "Автопост опубликован | version=%s | id=%s | topic=%s | type=%s | service=%s",
        APP_VERSION,
        post_id,
        topic,
        row["post_type"],
        row["monetization_service"],
    )


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

    logger.info("Scheduler rebuilt | version=%s | enabled=%s | times=%s", APP_VERSION, autopost_enabled, schedule_raw)


@admin_only
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        f"Бот работает.\nВерсия: {APP_VERSION}\n\n"
        "/gen [тема] — сгенерировать пост\n"
        "/publish [id] — опубликовать пост\n"
        "/last — последний пост\n"
        "/version — показать активную версию\n"
        "/schedule — показать расписание\n"
        "/set_schedule 09:00,14:00,19:00 — поменять расписание\n"
        "/autopost_on — включить автопостинг\n"
        "/autopost_off — выключить автопостинг\n"
        "/test_channel — тестовый пост в тестовый канал\n"
        "/test_post [id] — тест конкретного поста\n"
        "/topic_add текст — добавить тему\n"
        "/topics — показать темы\n"
        "/set_test_channel @my_test_channel — обновить тестовый канал"
    )


@admin_only
async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update, f"Активная версия: {APP_VERSION}")


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
        goal="manual",
        forced_post_type=None,
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
    stats = get_content_stats()
    stats_text = ", ".join([f"{k}: {v}" for k, v in stats.items()])
    await safe_reply(
        update,
        f"Версия: {APP_VERSION}\n"
        f"Автопостинг: {enabled}\n"
        f"Время публикаций (UTC+3): {schedule_raw}\n"
        f"Статистика типов: {stats_text}"
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
            forced_post_type="experimental",
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
                text=f"Произошла ошибка. Версия: {APP_VERSION}",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не удалось отправить сообщение об ошибке пользователю")


async def post_init(application: Application) -> None:
    rebuild_scheduler(application)
    logger.info("Bot starting | version=%s", APP_VERSION)


def build_application() -> Application:
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
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
    app.add_handler(CommandHandler("set_test_channel", set_test_channel_cmd))

    app.add_error_handler(error_handler)
    return app


def main():
    init_db()
    app = build_application()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
