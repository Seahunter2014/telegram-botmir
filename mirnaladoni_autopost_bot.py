import asyncio
import html
import logging
import os
import random
import re
import sqlite3
import ssl
import urllib.request
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

APP_VERSION = "travel-matrix-v12-signal-engine"

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

CONTENT_MODES: Dict[str, int] = {
    "offer": 35,
    "useful": 25,
    "idea": 20,
    "engagement": 20,
}

MODE_TEMPLATE_WEIGHTS: Dict[str, Dict[str, int]] = {
    "offer": {
        "selling": 40,
        "selection": 25,
        "seasonal": 10,
        "short": 5,
        "special_low_price_map": 10,
        "special_hot_tours": 5,
        "special_hotels_discount": 5,
    },
    "useful": {
        "useful": 40,
        "mistake": 25,
        "expert": 20,
        "selection": 10,
        "short": 5,
    },
    "idea": {
        "trust": 20,
        "seasonal": 25,
        "case": 20,
        "selection": 15,
        "selling": 10,
        "short": 10,
    },
    "engagement": {
        "engagement": 45,
        "mini_poll": 30,
        "provocation": 15,
        "short": 10,
    },
}

SIGNAL_KIND_KEYWORDS: Dict[str, List[str]] = {
    "offer": ["скид", "дешев", "акци", "распродаж", "горящ", "цена", "билет", "тур", "отель", "перелет"],
    "useful": ["как", "ошиб", "почему", "что проверить", "совет", "правил", "документ", "страхов", "багаж", "виза", "внж", "вид на жительство", "шенген", "паспорт", "границ"],
    "engagement": ["что выбрать", "вы бы", "опрос", "вопрос", "угадай", "голос", "или"],
    "idea": ["куда поехать", "мест", "страна", "город", "курорт", "маршрут", "фестиваль", "интересн", "пляж", "водопад", "парк", "ресторан", "кафе", "арктик", "кругосвет"],
}

DEFAULT_SIGNAL_SOURCES = "\n".join([
    "telegram_public|MirNaLadoni|https://t.me/s/NadoTurKrd",
    "telegram_public|Planet Earth|https://t.me/s/Planet_Earth",
    "telegram_public|travelpics|https://t.me/s/travelpics",
    "telegram_public|wonderful_globe|https://t.me/s/wonderful_globe",
    "telegram_public|Amazing World and Travel|https://t.me/s/amazingworldandtravel",
    "telegram_public|nomadslens|https://t.me/s/nomadslens",
    "telegram_public|globetrekker|https://t.me/s/globetrekker",
    "telegram_public|travelata|https://t.me/s/travelata",
    "telegram_public|leveltravel|https://t.me/s/leveltravel",
    "telegram_public|aviasales|https://t.me/s/aviasales",
    "telegram_public|tripmydream|https://t.me/s/tripmydream",
    "telegram_public|klooktravelsg|https://t.me/s/klooktravelsg",
    "web_list|RIA Tourism|https://ria.ru/tourism/",
    "web_list|ATOR outbound|https://www.atorus.ru/taxonomy/term/7",
    "web_list|ATOR domestic|https://www.atorus.ru/vnetrenniy-turizm",
])

BLOCKED_SIGNAL_SOURCE_NAMES = {
    "tripmydream",
}

BLOCKED_SIGNAL_KEYWORDS = [
    "украин",
    "україн",
    "ukraine",
    "украина",
    "україна",
    "украинский",
    "український",
    "киев",
    "київ",
    "kyiv",
    "львов",
    "львів",
    "lviv",
    "одесса",
    "одеса",
    "odesa",
    "харьков",
    "харків",
    "kharkiv",
    "укрзалізниця",
    "укрзализныця",
    "укрзализниця",
]

WHITELIST_THEME_KEYWORDS = {
    "visa": [
        "виза", "визы", "внж", "вид на жительство", "шенген", "въезд", "правила въезда",
        "безвиз", "паспорт", "документы", "для граждан рф", "россиян", "гражданам рф",
    ],
    "locations": [
        "пляж", "пляжи", "водопад", "водопады", "парк", "парки", "парк аттракционов",
        "аттракцион", "ресторан", "кафе", "локаци", "курорт", "остров", "бухта",
        "необычный тур", "арктика", "кругосвет", "сафари", "яхт", "круиз",
    ],
    "events": [
        "чемпионат", "евро", "world cup", "кубок", "лига чемпионов", "матч", "футбол",
        "концерт", "звезда", "фестиваль", "выставка", "шоу", "формула 1", "гран-при",
    ],
}

WHITELIST_DESTINATION_KEYWORDS = [
    "дубай", "оаэ", "турция", "стамбул", "анталья", "армения", "ереван", "грузия", "тбилиси",
    "батуми", "азия", "таиланд", "тайланд", "тай", "пхукет", "банкок", "вьетнам", "бали",
    "индонезия", "малайзия", "сингапур", "китай", "япония", "корея", "европа", "италия",
    "франция", "испания", "германия", "португалия", "греция", "сербия", "черногория",
    "америка", "сша", "нью-йорк", "лос-анджелес", "майами",
]

POST_TEMPLATES: Dict[str, int] = {
    "useful": 12,
    "mistake": 12,
    "selection": 10,
    "engagement": 10,
    "provocation": 5,
    "trust": 8,
    "expert": 10,
    "seasonal": 8,
    "case": 7,
    "selling": 12,
    "mini_poll": 3,
    "short": 3,
}

SPECIAL_TEMPLATES = {
    "special_low_price_map",
    "special_hot_tours",
    "special_hotels_discount",
}

TEMPLATE_LENGTH_CLASS = {
    "short": "short",
    "mini_poll": "short",
    "engagement": "short",
    "provocation": "short",
    "useful": "medium",
    "mistake": "medium",
    "trust": "medium",
    "seasonal": "medium",
    "case": "medium",
    "selection": "long",
    "expert": "long",
    "selling": "long",
    "special_low_price_map": "medium",
    "special_hot_tours": "medium",
    "special_hotels_discount": "medium",
}

LENGTH_RANGES = {
    "short": (350, 650),
    "medium": (650, 1100),
    "long": (900, 1450),
}

NO_LINK_TEMPLATES = set()
OPTIONAL_LINK_TEMPLATES = {"useful", "selection", "expert", "mistake", "seasonal", "case", "short", "engagement", "provocation", "trust", "mini_poll"}
FORCED_LINK_TEMPLATES = {"selling", "special_low_price_map", "special_hot_tours", "special_hotels_discount"}

SERVICES: List[Dict[str, Any]] = [
    {
        "key": "ostrovok",
        "name": "Ostrovok",
        "category": "hotels",
        "url": "https://ostrovok.tp.st/yHBoZUg7",
        "keywords": ["отель", "отели", "гостиница", "гостиницы", "жилье", "жильё", "проживание", "апартаменты"],
        "anchor_options": ["отели и цены", "варианты проживания", "подходящее жильё"],
        "priority": 10,
        "is_active": True,
    },
    {
        "key": "yandex_travel",
        "name": "Yandex Travel",
        "category": "general_travel",
        "url": "https://yandex.tp.st/y94GSOah",
        "keywords": ["путешествие", "поездка", "отдых", "маршрут", "направление"],
        "anchor_options": ["варианты поездки", "варианты отдыха", "удобные варианты"],
        "priority": 3,
        "is_active": True,
    },
    {
        "key": "aviasales",
        "name": "Aviasales",
        "category": "flights",
        "url": "https://aviasales.tp.st/hYipm2Ac",
        "keywords": ["билет", "билеты", "авиабилет", "авиабилеты", "самолет", "самолёт", "перелет", "перелёт", "рейс"],
        "anchor_options": ["авиабилеты", "варианты перелёта", "билеты на перелёт"],
        "priority": 10,
        "is_active": True,
    },
    {
        "key": "vip_zal",
        "name": "VIP Zal",
        "category": "airport",
        "url": "https://vip-zal.tp.st/VUTiM7FJ",
        "keywords": ["лаунж", "бизнес-зал", "бизнес зал", "зал ожидания", "аэропорт", "долгая пересадка", "пересадка"],
        "anchor_options": ["доступ в лаунж", "варианты бизнес-залов", "лаунж-залы в аэропорту"],
        "priority": 6,
        "is_active": True,
    },
    {
        "key": "onlinetours",
        "name": "Onlinetours",
        "category": "tours",
        "url": "https://onlinetours.tp.st/Um2ycow9",
        "keywords": ["тур", "туры", "путевка", "путёвка", "пакетный тур", "all inclusive"],
        "anchor_options": ["лучшие туры", "готовые туры", "поездки под ключ"],
        "priority": 9,
        "is_active": True,
    },
    {
        "key": "travelata",
        "name": "Travelata",
        "category": "tours",
        "url": "https://travelata.tp.st/O6m2Lg6H",
        "keywords": ["горящий тур", "горящие туры", "тур", "туры", "спецпредложения"],
        "anchor_options": ["лучшие туры", "туры со скидкой", "готовые туры"],
        "priority": 9,
        "is_active": True,
    },
    {
        "key": "tutu",
        "name": "Tutu",
        "category": "ground_transport",
        "url": "https://tutu.tp.st/dZglLc7q",
        "keywords": ["поезд", "поезда", "автобус", "автобусы", "электричка", "жд", "переезд"],
        "anchor_options": ["билеты на транспорт", "варианты переезда", "билеты на поезд или автобус"],
        "priority": 8,
        "is_active": True,
    },
    {
        "key": "rail_europe",
        "name": "Rail Europe",
        "category": "rail_europe",
        "url": "https://raileurope.tp.st/nWODZ4nI",
        "keywords": ["европа", "поезд по европе", "поезда европы", "железная дорога европа"],
        "anchor_options": ["жд билеты по Европе", "поезда по Европе", "европейские маршруты"],
        "priority": 7,
        "is_active": True,
    },
    {
        "key": "cherehapa",
        "name": "Cherehapa",
        "category": "insurance",
        "url": "https://cherehapa.tp.st/RIsddc4I",
        "keywords": ["страховка", "страхование", "полис", "страховой"],
        "anchor_options": ["страховку для поездки", "варианты страховки", "подходящий страховой полис"],
        "priority": 10,
        "is_active": True,
    },
    {
        "key": "kiwitaxi",
        "name": "KiwiTaxi",
        "category": "transfer",
        "url": "https://kiwitaxi.tp.st/Ven9kvYz",
        "keywords": ["трансфер", "из аэропорта", "в аэропорт", "после прилёта", "поздний прилёт"],
        "anchor_options": ["трансфер из аэропорта", "варианты трансфера", "поездку из аэропорта"],
        "priority": 9,
        "is_active": True,
    },
    {
        "key": "localrent",
        "name": "Localrent",
        "category": "car_rental",
        "url": "https://localrent.tp.st/Q77W1ZWX",
        "keywords": ["аренда авто", "машина", "авто", "прокат авто", "арендовать машину"],
        "anchor_options": ["аренду авто", "варианты проката авто", "машину для поездки"],
        "priority": 9,
        "is_active": True,
    },
    {
        "key": "bikesbooking",
        "name": "BikesBooking",
        "category": "bike_rental",
        "url": "https://bikesbooking.tp.st/eMN1TXvi",
        "keywords": ["байк", "скутер", "мотоцикл", "мопед", "багги"],
        "anchor_options": ["прокат байка или скутера", "двухколёсный транспорт", "прокат скутера"],
        "priority": 7,
        "is_active": True,
    },
    {
        "key": "sputnik8",
        "name": "Sputnik8",
        "category": "excursions",
        "url": "https://sputnik8.tp.st/FQZC0UxF",
        "keywords": ["экскурсия", "экскурсии", "гид", "что посмотреть", "достопримечательности", "прогулка"],
        "anchor_options": ["экскурсии по месту", "что посмотреть на месте", "варианты экскурсий"],
        "priority": 9,
        "is_active": True,
    },
    {
        "key": "ticketnetwork",
        "name": "TicketNetwork",
        "category": "events",
        "url": "https://ticketnetwork.tp.st/evSOqzXe",
        "keywords": ["концерт", "шоу", "мероприятие", "событие", "матч", "спектакль", "выставка", "фестиваль"],
        "anchor_options": ["билеты на мероприятия", "события по направлению", "интересные мероприятия"],
        "priority": 7,
        "is_active": True,
    },
    {
        "key": "trip_cruises",
        "name": "Trip",
        "category": "cruises",
        "url": "https://www.trip.com/t/OjTmFMTwFU2",
        "keywords": ["круиз", "круизы", "лайнер"],
        "anchor_options": ["варианты круизов", "круизы по направлению", "подходящий круиз"],
        "priority": 6,
        "is_active": True,
    },
    {
        "key": "tourjin_bot",
        "name": "TourJin Bot",
        "category": "general_bot",
        "url": "https://t.me/TourJin_bot",
        "keywords": ["куда поехать", "что выбрать", "общий подбор", "подобрать поездку", "подобрать отдых"],
        "anchor_options": ["удобный подбор поездки", "варианты под запрос", "идеи поездки"],
        "priority": 2,
        "is_active": True,
    },
]

TOPIC_GROUP_RULES = {
    "hotels": ["отель", "отели", "гостиница", "гостиницы", "жилье", "жильё", "проживание", "апартаменты"],
    "flights": ["билет", "билеты", "авиабилет", "авиабилеты", "перелет", "перелёт", "самолет", "самолёт", "рейс"],
    "tours": ["тур", "туры", "путевка", "путёвка", "горящий тур", "горящие туры", "all inclusive"],
    "insurance": ["страховка", "страхование", "полис", "страховой"],
    "transfer": ["трансфер", "из аэропорта", "в аэропорт", "поздний прилёт", "после прилёта"],
    "car_rental": ["аренда авто", "машина", "прокат авто", "арендовать машину"],
    "bike_rental": ["байк", "скутер", "мотоцикл", "мопед", "багги"],
    "excursions": ["экскурсия", "экскурсии", "гид", "что посмотреть", "достопримечательности", "прогулка"],
    "events": ["концерт", "шоу", "мероприятие", "событие", "матч", "спектакль", "выставка", "фестиваль"],
    "cruises": ["круиз", "круизы", "лайнер"],
    "ground_transport": ["поезд", "поезда", "автобус", "автобусы", "электричка", "жд", "переезд"],
    "rail_europe": ["европа", "поезд по европе", "поезда европы"],
    "airport": ["лаунж", "бизнес-зал", "зал ожидания", "пересадка", "аэропорт"],
    "general_bot": ["куда поехать", "что выбрать", "подобрать поездку", "подобрать отдых"],
    "general_travel": ["поездка", "отдых", "маршрут", "направление", "путешествие"],
}

CTA_CLASSES = {
    "save": [
        "Сохрани, чтобы не потерять.",
        "Лучше оставить себе, чем потом искать заново.",
        "Оставь себе — такие вещи обычно вспоминают в последний момент.",
        "Сохрани пост, чтобы вернуться к нему перед поездкой.",
    ],
    "comment": [
        "Что бы ты сюда добавил?",
        "У тебя было что-то похожее?",
        "А у тебя как обычно происходит?",
        "С каким пунктом ты бы поспорил?",
    ],
    "choice": [
        "А вы бы что выбрали?",
        "Для вас важнее цена или комфорт?",
        "Ты бы выбрал экономию или удобство?",
        "А какой вариант ближе тебе?",
    ],
    "share": [
        "Отправь тому, с кем давно хочется куда-нибудь выбраться.",
        "Поделись с тем, кому сейчас особенно нужны идеи для поездки.",
        "Перешли другу, с которым вы все откладываете отпуск.",
        "Сохрани или отправь тому, кто любит находить классные варианты раньше других.",
    ],
    "subscribe": [
        "Читайте канал, здесь мы регулярно собираем идеи, сервисы и лайфхаки для путешествий.",
        "Оставайтесь с нами: впереди еще много маршрутов, находок и полезных сервисов.",
        "Подписывайтесь, если любите путешествия с удовольствием, а не в спешке.",
        "В канале еще будет много красивых и полезных идей для ваших поездок.",
    ],
    "soft_sell": [
        "Если идея вам откликается, можно спокойно сравнить разные варианты и выбрать свой формат поездки.",
        "Понравилось направление? Самое время посмотреть, какие варианты путешествия сейчас доступны.",
        "Когда начинаешь готовиться заранее, поездка обычно получается и спокойнее, и приятнее.",
        "Иногда один удачно найденный вариант уже задает настроение всему будущему отпуску.",
    ],
    "none": [
        "Путешествие всегда начинается с хорошей идеи, а дальше уже складываются детали.",
        "Именно из таких деталей потом и рождаются самые теплые впечатления.",
        "Чем спокойнее подготовка, тем больше удовольствия от самой поездки.",
        "Пусть каждое путешествие приносит не суету, а радость и яркие эмоции.",
    ],
}

BRAND_SIGNATURE_HTML = 'С любовью, <a href="https://t.me/NadoTurKrd">Мир на ладони</a>'

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
                content_mode TEXT,
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
        ensure_column(conn, "posts", "content_mode", "TEXT")
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS content_signals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                summary TEXT DEFAULT '',
                url TEXT DEFAULT '',
                source_name TEXT DEFAULT '',
                source_type TEXT DEFAULT '',
                signal_kind TEXT DEFAULT 'idea',
                score INTEGER DEFAULT 0,
                used INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
    ensure_default_settings()
    ensure_default_topics()
    ensure_default_content_stats()

def ensure_default_content_stats():
    with closing(db()) as conn:
        for post_type in POST_TEMPLATES.keys():
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
    if post_type in SPECIAL_TEMPLATES:
        return
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO content_stats(post_type, count)
            VALUES(?, 1)
            ON CONFLICT(post_type) DO UPDATE SET count = count + 1
            """,
            (post_type,),
        )
        conn.commit()

def get_content_stats() -> Dict[str, int]:
    stats = {post_type: 0 for post_type in POST_TEMPLATES.keys()}
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT post_type
            FROM posts
            WHERE post_type NOT IN ('special_low_price_map', 'special_hot_tours', 'special_hotels_discount')
            ORDER BY id DESC
            LIMIT 60
            """
        ).fetchall()
    for row in rows:
        post_type = row["post_type"]
        if post_type in stats:
            stats[post_type] += 1
    return stats

def get_mode_weights() -> Dict[str, int]:
    return {
        "offer": int(get_setting("mode_offer_weight", "35") or "35"),
        "useful": int(get_setting("mode_useful_weight", "25") or "25"),
        "idea": int(get_setting("mode_idea_weight", "20") or "20"),
        "engagement": int(get_setting("mode_engagement_weight", "20") or "20"),
    }

def get_content_mode_stats() -> Dict[str, int]:
    stats = {mode: 0 for mode in CONTENT_MODES.keys()}
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT content_mode
            FROM posts
            WHERE content_mode IS NOT NULL AND content_mode != ''
            ORDER BY id DESC
            LIMIT 60
            """
        ).fetchall()
    for row in rows:
        mode = row["content_mode"]
        if mode in stats:
            stats[mode] += 1
    return stats

def choose_content_mode() -> str:
    weights = get_mode_weights()
    stats = get_content_mode_stats()
    total = sum(stats.values())
    if total == 0:
        pool: List[str] = []
        for mode, weight in weights.items():
            pool.extend([mode] * max(1, weight))
        return random.choice(pool)
    deficits: Dict[str, float] = {}
    recent = get_recent_posts(8)
    recent_modes = [row["content_mode"] for row in recent if row["content_mode"] in weights]
    for mode, target_pct in weights.items():
        actual_pct = (stats.get(mode, 0) / total) * 100 if total else 0
        deficits[mode] = target_pct - actual_pct
        if recent_modes and recent_modes[0] == mode:
            deficits[mode] -= 20
        if recent_modes.count(mode) >= 2:
            deficits[mode] -= 12
    best = max(deficits.values())
    candidates = [mode for mode, score in deficits.items() if score == best]
    return random.choice(candidates)

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
        "signals_enabled": "1",
        "signals_refresh_hours": "8",
        "signal_sources": DEFAULT_SIGNAL_SOURCES,
        "mode_offer_weight": "35",
        "mode_useful_weight": "25",
        "mode_idea_weight": "20",
        "mode_engagement_weight": "20",
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
        "Почему маленькие приморские города иногда лучше раскрученных курортов",
        "Что особенно цепляет в Стамбуле весной",
        "Куда уехать на 3–4 дня, если хочется сменить картинку",
        "Как одна ошибка с районом проживания портит всю поездку",
        "Какие форматы отдыха люди чаще всего недооценивают",
        "Что важнее в отпуске: экономия, комфорт или впечатления",
        "Почему межсезонье часто выгоднее пикового периода",
        "Когда аренда авто действительно делает поездку удобнее",
        "В каких случаях трансфер из аэропорта — это не роскошь, а спокойствие",
        "Как выставка, матч или концерт могут стать поводом для поездки",
        "Что выбрать: отель в центре или жильё подальше, но тише",
        "Какие расходы в путешествии чаще всего недооценивают",
        "Почему спонтанные поездки иногда получаются ярче запланированных",
        "Как не прогадать с коротким городским путешествием",
        "Что люди зря игнорируют при выборе пакетного тура",
        "Почему красивое место ещё не всегда равно хорошему отдыху",
        "Какие вопросы стоит задать себе до покупки билетов",
        "Когда горы лучше моря, а когда наоборот",
        "Как собрать поездку так, чтобы не уставать с первого дня",
        "Какие признаки выдают неудачный вариант проживания ещё до бронирования",
        "Почему путешествие ради события иногда запоминается сильнее, чем пляжный отдых",
    ]
    with closing(db()) as conn:
        existing_rows = conn.execute("SELECT topic_text FROM topics").fetchall()
        existing_topics = {row["topic_text"].strip().lower() for row in existing_rows}
        inserted = 0
        for topic in default_topics:
            normalized = topic.strip().lower()
            if normalized in existing_topics:
                continue
            conn.execute(
                """
                INSERT INTO topics(topic_text, source_type, source_name, used, created_at)
                VALUES(?, 'default', 'system', 0, ?)
                """,
                (topic, now_iso()),
            )
            inserted += 1
        if inserted:
            conn.commit()
            logger.info("Добавлены новые стандартные темы: %s", inserted)

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
    text = re.sub(r"^\s*Заголовок\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*Title\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"^([^\w\s])(\d)", r"\1 \2", text)
    text = re.sub(r"\n\s*Заголовок\s*:\s*", "\n", text, flags=re.IGNORECASE)
    return text.strip()

def looks_incomplete(text: str) -> bool:
    stripped = text.strip()
    if len(stripped) < 260:
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
    if last_break > max_len * 0.55:
        shortened = shortened[:last_break + 1].rstrip()
    return shortened.rstrip(" ,;:-") + "…"

def get_recent_posts(limit: int = 7):
    with closing(db()) as conn:
        return conn.execute(
            """
            SELECT id, topic, post_type, content_mode, monetization_service, content, created_at
            FROM posts
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

def get_last_post():
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 1").fetchone()

def get_post(post_id: int):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM posts WHERE id=?", (post_id,)).fetchone()

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
        "Идея для путешествия, которая может вдохновить на ближайший отпуск",
        "Место, ради которого хочется собрать маршрут уже сегодня",
        "Событие, под которое можно придумать яркую поездку",
        "Необычный формат отдыха, который дарит настоящие впечатления",
        "Направление, куда приятно поехать без суеты и с интересом",
    ])
    return fallback, "fallback"

def fetch_url(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0 Safari/537.36"},
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=25) as resp:
        return resp.read().decode("utf-8", errors="replace")

def strip_html_tags(text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def normalize_url(url: str) -> str:
    return url.strip()

def classify_signal_kind(title: str, summary: str = "") -> str:
    haystack = normalize_text(f"{title} {summary}")
    scores: Dict[str, int] = {}
    for kind, keywords in SIGNAL_KIND_KEYWORDS.items():
        score = 0
        for kw in keywords:
            if kw in haystack:
                score += 1
        if score:
            scores[kind] = score
    if not scores:
        return "idea"
    return max(scores.items(), key=lambda x: x[1])[0]

def parse_signal_sources(raw: str) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    for line in [x.strip() for x in raw.splitlines() if x.strip()]:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 3:
            continue
        result.append({"type": parts[0], "name": parts[1], "url": parts[2]})
    return result

def contains_blocked_signal_content(title: str, summary: str, source_name: str = "") -> bool:
    haystack = normalize_text(f"{title} {summary} {source_name}")
    source_norm = normalize_text(source_name)
    if source_norm in BLOCKED_SIGNAL_SOURCE_NAMES:
        return True
    if any(keyword in haystack for keyword in BLOCKED_SIGNAL_KEYWORDS):
        return True
    if re.search(r"[іїєґІЇЄҐ]", f"{title} {summary}"):
        return True
    return False

def signal_whitelist_bonus(title: str, summary: str, source_name: str = "") -> int:
    haystack = normalize_text(f"{title} {summary} {source_name}")
    bonus = 0
    matched_groups = 0
    for keywords in WHITELIST_THEME_KEYWORDS.values():
        if any(keyword in haystack for keyword in keywords):
            bonus += 3
            matched_groups += 1
    if any(keyword in haystack for keyword in WHITELIST_DESTINATION_KEYWORDS):
        bonus += 2
    if "для граждан рф" in haystack or "гражданам рф" in haystack or "россиян" in haystack:
        bonus += 3
    if matched_groups >= 2:
        bonus += 2
    return bonus

def purge_blocked_signals() -> int:
    removed = 0
    with closing(db()) as conn:
        rows = conn.execute("SELECT id, title, summary, source_name FROM content_signals").fetchall()
        for row in rows:
            if contains_blocked_signal_content(row["title"] or "", row["summary"] or "", row["source_name"] or ""):
                conn.execute("DELETE FROM content_signals WHERE id=?", (row["id"],))
                removed += 1
        conn.commit()
    return removed

def save_signal(title: str, summary: str, url: str, source_name: str, source_type: str, signal_kind: str, score: int = 0) -> bool:
    normalized_url = normalize_url(url)
    normalized_title = title.strip()
    if not normalized_title:
        return False
    if contains_blocked_signal_content(normalized_title, summary.strip(), source_name):
        return False
    with closing(db()) as conn:
        exists = conn.execute(
            """
            SELECT id FROM content_signals
            WHERE title=? AND source_name=? AND COALESCE(url,'')=?
            LIMIT 1
            """,
            (normalized_title, source_name, normalized_url),
        ).fetchone()
        if exists:
            return False
        conn.execute(
            """
            INSERT INTO content_signals(title, summary, url, source_name, source_type, signal_kind, score, used, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (normalized_title, summary.strip(), normalized_url, source_name, source_type, signal_kind, score, now_iso()),
        )
        conn.commit()
        return True

def discover_signals_from_web_list(name: str, url: str) -> int:
    html_text = fetch_url(url)
    headlines = re.findall(r"<h[1-3][^>]*>(.*?)</h[1-3]>", html_text, re.I | re.S)
    links = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html_text, re.I | re.S)
    saved = 0
    for raw in headlines[:10]:
        title = strip_html_tags(raw)
        if len(title) >= 18:
            kind = classify_signal_kind(title, "")
            score = 5 + signal_whitelist_bonus(title, "", name)
            if save_signal(title, "", url, name, "web_list", kind, score=score):
                saved += 1
    for href, raw_text in links[:30]:
        text = strip_html_tags(raw_text)
        if len(text) < 18:
            continue
        final_url = href if href.startswith("http") else url
        kind = classify_signal_kind(text, "")
        score = 3 + signal_whitelist_bonus(text, "", name)
        if save_signal(text, "", final_url, name, "web_list", kind, score=score):
            saved += 1
    return saved

def discover_signals_from_public_telegram(name: str, url: str) -> int:
    html_text = fetch_url(url)
    posts = re.findall(
        r'<div class="tgme_widget_message_text js-message_text"[^>]*>([\s\S]*?)</div>[\s\S]*?<time datetime="([^"]+)"',
        html_text,
        re.I,
    )
    saved = 0
    source_name_norm = name.lower()
    saved_per_source = 0
    for raw_text, _dt in posts[:12]:
        text = strip_html_tags(raw_text)
        if len(text) < 35:
            continue
        title = text.split(".")[0][:140].strip(" -")
        summary = text[:350]
        if contains_blocked_signal_content(title, summary, name):
            continue
        kind = classify_signal_kind(title, summary)
        base_score = 6
        if source_name_norm in {"travelata", "leveltravel", "aviasales", "tripmydream", "klooktravelsg"}:
            base_score = 9
        elif source_name_norm in {"planet earth", "travelpics", "wonderful_globe", "amazing world and travel", "nomadslens", "globetrekker"}:
            base_score = 7
        if source_name_norm == "klooktravelsg":
            base_score = 5
        elif source_name_norm == "aviasales":
            base_score = 8
        elif source_name_norm in {"travelata", "leveltravel"}:
            base_score = 8
        elif source_name_norm in {"planet earth", "travelpics", "wonderful_globe", "amazing world and travel", "nomadslens", "globetrekker"}:
            base_score = 6
        final_score = base_score + signal_whitelist_bonus(title, summary, name)
        if saved_per_source >= 4:
            break
        if save_signal(title or summary[:120], summary, url, name, "telegram_public", kind, score=final_score):
            saved += 1
            saved_per_source += 1
    return saved

def discover_public_signals(force: bool = False) -> int:
    if get_setting("signals_enabled", "1") != "1":
        return 0
    purge_blocked_signals()
    refresh_hours = int(get_setting("signals_refresh_hours", "8") or "8")
    last_run_raw = get_setting("signals_last_refresh_at", "")
    if not force and last_run_raw:
        try:
            last_run = datetime.fromisoformat(last_run_raw)
            if datetime.now(timezone.utc) - last_run < timedelta(hours=refresh_hours):
                return 0
        except Exception:
            pass
    sources = parse_signal_sources(get_setting("signal_sources", DEFAULT_SIGNAL_SOURCES))
    inserted = 0
    for source in sources:
        try:
            if source["type"] == "telegram_public":
                inserted += discover_signals_from_public_telegram(source["name"], source["url"])
            elif source["type"] == "web_list":
                inserted += discover_signals_from_web_list(source["name"], source["url"])
        except Exception:
            logger.exception("Ошибка сбора сигналов | source=%s | url=%s", source["name"], source["url"])
    set_setting("signals_last_refresh_at", now_iso())
    return inserted

def get_next_signal(preferred_kind: Optional[str] = None) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        if preferred_kind:
            row = conn.execute(
                """
                SELECT * FROM content_signals
                WHERE used=0 AND signal_kind=?
                ORDER BY score DESC, id DESC
                LIMIT 1
                """,
                (preferred_kind,),
            ).fetchone()
            if row:
                conn.execute("UPDATE content_signals SET used=1 WHERE id=?", (row["id"],))
                conn.commit()
                return row
        row = conn.execute(
            """
            SELECT * FROM content_signals
            WHERE used=0
            ORDER BY score DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            conn.execute("UPDATE content_signals SET used=1 WHERE id=?", (row["id"],))
            conn.commit()
            return row
    return None

def list_signals(limit: int = 15, only_unused: bool = False) -> List[sqlite3.Row]:
    with closing(db()) as conn:
        if only_unused:
            return conn.execute(
                """
                SELECT * FROM content_signals
                WHERE used=0
                ORDER BY score DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return conn.execute(
            """
            SELECT * FROM content_signals
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

def reset_signals(only_used: bool = False) -> int:
    with closing(db()) as conn:
        if only_used:
            cur = conn.execute("UPDATE content_signals SET used=0 WHERE used=1")
        else:
            cur = conn.execute("UPDATE content_signals SET used=0")
        conn.commit()
        return cur.rowcount

def clear_signals() -> int:
    with closing(db()) as conn:
        cur = conn.execute("DELETE FROM content_signals")
        conn.commit()
        return cur.rowcount

def get_next_content_input() -> Dict[str, str]:
    discover_public_signals(force=False)
    content_mode = choose_content_mode()
    signal_kind_map = {
        "offer": "offer",
        "useful": "useful",
        "idea": "idea",
        "engagement": "engagement",
    }
    signal = get_next_signal(signal_kind_map.get(content_mode))
    if signal:
        return {
            "topic": signal["title"],
            "source_type": signal["source_type"] or "signal",
            "goal": "autopost",
            "content_mode": content_mode,
            "signal_title": signal["title"] or "",
            "signal_summary": signal["summary"] or "",
            "signal_url": signal["url"] or "",
            "signal_kind": signal["signal_kind"] or content_mode,
            "signal_source_name": signal["source_name"] or "",
        }
    topic, source_type = get_next_topic()
    return {
        "topic": topic,
        "source_type": source_type,
        "goal": "autopost",
        "content_mode": content_mode,
        "signal_title": "",
        "signal_summary": "",
        "signal_url": "",
        "signal_kind": "",
        "signal_source_name": "",
    }

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
    return sorted(result)

def get_today_post_slot(now_dt: datetime, schedule_raw: str) -> int:
    times = parse_schedule(schedule_raw)
    current_minutes = now_dt.hour * 60 + now_dt.minute
    for idx, (h, m) in enumerate(times, start=1):
        if h * 60 + m == current_minutes:
            return idx
    passed = [1 for h, m in times if h * 60 + m <= current_minutes]
    if passed:
        return len(passed)
    return 1

def template_family(template: str) -> str:
    families = {
        "useful": "practical",
        "mistake": "practical",
        "expert": "practical",
        "selection": "comparison",
        "selling": "comparison",
        "engagement": "engagement",
        "mini_poll": "engagement",
        "provocation": "engagement",
        "trust": "human",
        "case": "human",
        "seasonal": "situational",
        "short": "situational",
    }
    return families.get(template, template)

def choose_post_template(content_mode: Optional[str] = None) -> str:
    weights_map = MODE_TEMPLATE_WEIGHTS.get(content_mode or "", POST_TEMPLATES)
    stats = get_content_stats()
    total = sum(stats.values())
    recent = get_recent_posts(8)
    recent_types = [row["post_type"] for row in recent if row["post_type"] and row["post_type"] in POST_TEMPLATES]
    recent_families = [template_family(t) for t in recent_types]
    if total == 0:
        weighted = []
        for template, weight in weights_map.items():
            weighted.extend([template] * weight)
        return random.choice(weighted)
    deficits = {}
    target_total = sum(weights_map.values()) or 1
    for template, target_weight in weights_map.items():
        target_pct = (target_weight / target_total) * 100
        actual_pct = (stats.get(template, 0) / total) * 100 if total else 0
        deficits[template] = target_pct - actual_pct
    for template in list(deficits.keys()):
        if recent_types and recent_types[0] == template:
            deficits[template] -= 35
        if recent_types.count(template) >= 2:
            deficits[template] -= 18
        if recent_families and recent_families[0] == template_family(template):
            deficits[template] -= 10
        if template == "selling" and recent_types and recent_types[0] == "selling":
            deficits[template] -= 40
    best = max(deficits.values())
    candidates = [k for k, v in deficits.items() if v == best]
    return random.choice(candidates)

def get_length_range(template: str) -> Tuple[int, int]:
    length_class = TEMPLATE_LENGTH_CLASS.get(template, "medium")
    return LENGTH_RANGES[length_class]

def recent_posts_have_links(recent_posts, lookback: int = 1) -> bool:
    return any((row["monetization_service"] or "").strip() for row in recent_posts[:lookback])

def recent_tourjin_count(recent_posts, lookback: int = 4) -> int:
    return sum(1 for row in recent_posts[:lookback] if (row["monetization_service"] or "") == "tourjin_bot")

def topic_groups(topic: str, content: str = "") -> List[str]:
    topic_text = normalize_text(topic)
    content_text = normalize_text(content)
    scores: Dict[str, int] = {}
    for group, keywords in TOPIC_GROUP_RULES.items():
        score = 0
        for kw in keywords:
            if kw in topic_text:
                score += 3
            if kw in content_text:
                score += 1
        if score > 0:
            scores[group] = score
    for service in SERVICES:
        if not service["is_active"]:
            continue
        service_score = 0
        for kw in service["keywords"]:
            if kw in topic_text:
                service_score += 2
            if kw in content_text:
                service_score += 1
        if service_score > 0:
            scores[service["category"]] = scores.get(service["category"], 0) + service_score
    if not scores:
        return []
    max_score = max(scores.values())
    winners = [group for group, score in scores.items() if score == max_score]
    return winners

def build_service_candidates(groups: List[str]) -> List[Dict[str, Any]]:
    matches = [s for s in SERVICES if s["is_active"] and s["category"] in groups]
    matches.sort(key=lambda x: x["priority"], reverse=True)
    return matches

def should_insert_links(template: str, recent_posts, groups: Optional[List[str]] = None) -> bool:
    groups = groups or []
    if template in NO_LINK_TEMPLATES:
        return False
    if recent_posts_have_links(recent_posts, lookback=1):
        return False
    if template in FORCED_LINK_TEMPLATES:
        return True
    strong = [g for g in groups if g not in {"general_bot", "general_travel"}]
    if groups and strong:
        if template in {"engagement", "provocation", "mini_poll"}:
            return random.random() < 0.35
        if template in {"trust", "short"}:
            return random.random() < 0.55
        return True
    if template in OPTIONAL_LINK_TEMPLATES:
        return random.random() < 0.45
    return False

def choose_services(topic: str, content: str, template: str, recent_posts) -> List[Dict[str, Any]]:
    groups = topic_groups(topic, content)
    if not should_insert_links(template, recent_posts, groups):
        return []
    if not groups:
        return []
    candidates = build_service_candidates(groups)
    if not candidates:
        return []
    last_service = (recent_posts[0]["monetization_service"] or "") if recent_posts else ""
    filtered = [s for s in candidates if s["key"] != last_service]
    if filtered:
        candidates = filtered
    final_candidates = []
    for s in candidates:
        if s["key"] == "tourjin_bot":
            if "general_bot" not in groups and "general_travel" not in groups:
                continue
            if recent_tourjin_count(recent_posts, lookback=4) >= 1:
                continue
        final_candidates.append(s)
    if final_candidates:
        candidates = final_candidates
    if not candidates:
        return []
    max_links = 3 if template in {"selection", "selling", "seasonal", "case"} else 2
    chosen: List[Dict[str, Any]] = [candidates[0]]
    used_categories = {candidates[0]["category"]}
    if template in {"selection", "selling", "seasonal", "case"} and len(candidates) > 1:
        for candidate in candidates[1:]:
            if candidate["key"] == chosen[0]["key"]:
                continue
            if candidate["category"] in used_categories:
                continue
            chosen.append(candidate)
            used_categories.add(candidate["category"])
            if len(chosen) >= max_links:
                break
    return chosen[:max_links]

def choose_cta_class(template: str, recent_posts) -> str:
    preferred = {
        "selling": ["soft_sell", "share", "subscribe"],
        "engagement": ["choice", "comment"],
        "mini_poll": ["choice"],
        "provocation": ["comment", "choice"],
        "trust": ["none", "comment"],
        "expert": ["comment", "save", "none"],
        "selection": ["choice", "share", "save"],
        "useful": ["save", "comment", "share", "none"],
        "mistake": ["save", "comment", "none"],
        "seasonal": ["save", "soft_sell", "subscribe"],
        "case": ["comment", "share", "none"],
        "short": ["save", "none", "comment"],
        "special_low_price_map": ["soft_sell", "share"],
        "special_hot_tours": ["soft_sell", "share"],
        "special_hotels_discount": ["soft_sell", "share"],
    }
    recent_text = " ".join((row["content"] or "")[-200:] for row in recent_posts[:4])
    options = preferred.get(template, ["save", "comment", "none"])
    filtered_classes = []
    for cta_class in options:
        phrases = CTA_CLASSES.get(cta_class, [])
        if not any(phrase in recent_text for phrase in phrases):
            filtered_classes.append(cta_class)
    return random.choice(filtered_classes or options)

def build_cta(template: str, recent_posts) -> str:
    cta_class = choose_cta_class(template, recent_posts)
    options = CTA_CLASSES[cta_class]
    recent_text = " ".join((row["content"] or "")[-200:] for row in recent_posts[:4])
    filtered = [o for o in options if o not in recent_text]
    return random.choice(filtered or options)

def choose_anchor_text(service: Dict[str, Any]) -> str:
    return random.choice(service["anchor_options"])

def build_brand_hashtags(content: str, template: str, services: List[Dict[str, Any]]) -> str:
    base_tags = ["#МирНаЛадони", "#ПутешествиеМечты", "#Турджин"]
    haystack = normalize_text(content)
    extra_tags: List[str] = []

    keyword_map = [
        ("авиабилет", "#Авиабилеты"),
        ("перел", "#ДешевыеПерелеты"),
        ("отел", "#Отели"),
        ("жиль", "#ОтдыхСКомфортом"),
        ("тур", "#ГотовыеТуры"),
        ("море", "#ОтдыхУМоря"),
        ("пляж", "#ПляжныйОтдых"),
        ("егип", "#Египет"),
        ("турци", "#Турция"),
        ("дубай", "#ОАЭ"),
        ("виза", "#ЛайфхакиДляПутешествий"),
        ("страхов", "#БезопасноеПутешествие"),
        ("экскурс", "#ИдеиДляПутешествий"),
        ("выходн", "#ИдеиНаУикенд"),
        ("уикенд", "#ИдеиНаУикенд"),
        ("событи", "#КудаПоехать"),
        ("маршрут", "#МаршрутыМечты"),
    ]
    for needle, tag in keyword_map:
        if needle in haystack and tag not in extra_tags:
            extra_tags.append(tag)
        if len(extra_tags) >= 2:
            break

    if len(extra_tags) < 2:
        category_map = {
            "tours": "#ГотовыеТуры",
            "flights": "#Авиабилеты",
            "hotels": "#Отели",
            "excursions": "#ИдеиДляПутешествий",
            "insurance": "#ЛайфхакиДляПутешествий",
            "general_travel": "#КудаПоехать",
            "general_bot": "#ПланируемОтпуск",
        }
        for service in services:
            tag = category_map.get(service.get("category", ""))
            if tag and tag not in extra_tags:
                extra_tags.append(tag)
            if len(extra_tags) >= 2:
                break

    if len(extra_tags) < 2:
        fallback_by_template = {
            "selling": ["#ПланируемОтпуск", "#КудаПоехать"],
            "selection": ["#КудаПоехать", "#ИдеиДляПутешествий"],
            "seasonal": ["#ИдеиДляПутешествий", "#ПланируемОтпуск"],
            "useful": ["#ЛайфхакиДляПутешествий", "#ПланируемОтпуск"],
            "mistake": ["#ЛайфхакиДляПутешествий", "#ПутешествиеБезСтресса"],
        }
        for tag in fallback_by_template.get(template, ["#КудаПоехать", "#ПланируемОтпуск"]):
            if tag not in extra_tags:
                extra_tags.append(tag)
            if len(extra_tags) >= 2:
                break

    return " ".join(base_tags + extra_tags[:2])

def build_native_link_paragraph(service: Dict[str, Any], template: str) -> str:
    url = service["url"]
    anchor = escape_html_text(choose_anchor_text(service))
    base_templates = {
        "hotels": [
            f'Если захочется собрать поездку под свой вкус и бюджет, можно спокойно посмотреть <a href="{url}">{anchor}</a>.',
            f'Когда хочется больше комфорта и меньше хаоса в поиске, удобно заранее открыть <a href="{url}">{anchor}</a>.',
        ],
        "tours": [
            f'Понравилась идея? Можно спокойно сравнить <a href="{url}">{anchor}</a> на нужные даты и направление.',
            f'Если хочется упростить подготовку, посмотрите <a href="{url}">{anchor}</a> и выберите свой вариант поездки.',
        ],
        "flights": [
            f'Для старта путешествия удобно заранее проверить <a href="{url}">{anchor}</a> и понять вилку цен.',
            f'Иногда именно удачный перелёт задаёт настроение всей поездке — посмотрите <a href="{url}">{anchor}</a>.',
        ],
        "insurance": [
            f'Чтобы путешествовать спокойнее, перед вылетом стоит заранее посмотреть <a href="{url}">{anchor}</a>.',
            f'Для уверенности в дороге можно заранее выбрать <a href="{url}">{anchor}</a>.',
        ],
        "excursions": [
            f'Чтобы впечатлений в поездке было больше, заранее посмотрите <a href="{url}">{anchor}</a>.',
            f'Если хочется наполнить маршрут эмоциями, удобно заранее выбрать <a href="{url}">{anchor}</a>.',
        ],
        "transfer": [
            f'Чтобы после прилёта всё было спокойно и без суеты, можно заранее посмотреть <a href="{url}">{anchor}</a>.',
            f'Если не хочется решать логистику на месте, удобно заранее открыть <a href="{url}">{anchor}</a>.',
        ],
        "car_rental": [
            f'Для свободы в маршруте удобно заранее сравнить <a href="{url}">{anchor}</a>.',
            f'Если хочется увидеть больше за одну поездку, посмотрите <a href="{url}">{anchor}</a>.',
        ],
        "ground_transport": [
            f'Для спокойного маршрута без спешки удобно заранее проверить <a href="{url}">{anchor}</a>.',
            f'Если поездка состоит из нескольких переездов, заранее посмотрите <a href="{url}">{anchor}</a>.',
        ],
        "events": [
            f'Если хочется поехать под конкретное событие, заранее посмотрите <a href="{url}">{anchor}</a>.',
            f'Такие поездки особенно приятно собирать, когда под рукой уже есть <a href="{url}">{anchor}</a>.',
        ],
        "airport": [
            f'Если впереди длинная дорога или пересадка, заранее посмотрите <a href="{url}">{anchor}</a>.',
            f'Для более комфортного пути удобно заранее проверить <a href="{url}">{anchor}</a>.',
        ],
        "general_bot": [
            f'Если хочется быстро собрать варианты под свой запрос, можно открыть <a href="{url}">{anchor}</a>.',
            f'Когда нужна отправная точка для поездки, удобно посмотреть <a href="{url}">{anchor}</a>.',
        ],
        "general_travel": [
            f'Если хотите сравнить разные сценарии путешествия, можно спокойно открыть <a href="{url}">{anchor}</a>.',
            f'Когда хочется увидеть общую картину поездки без спешки, посмотрите <a href="{url}">{anchor}</a>.',
        ],
        "cruises": [
            f'Если вам близок такой формат отдыха, заранее посмотрите <a href="{url}">{anchor}</a>.',
            f'Такие маршруты особенно удобно сравнивать через <a href="{url}">{anchor}</a>.',
        ],
        "rail_europe": [
            f'Если собираете маршрут по Европе, удобно заранее открыть <a href="{url}">{anchor}</a>.',
            f'Такие переезды приятнее планировать, когда под рукой уже есть <a href="{url}">{anchor}</a>.',
        ],
    }
    category = service["category"]
    options = base_templates.get(category, [
        f'Если тема актуальна, удобно заранее проверить <a href="{url}">{anchor}</a>.',
        f'Чтобы не тратить время на хаотичный поиск, можно открыть <a href="{url}">{anchor}</a>.',
    ])
    if template == "selling":
        options = [
            f'Понравилась идея? Начните подготовку спокойно: посмотрите <a href="{url}">{anchor}</a> и сравните варианты.',
            f'Если хочется перейти от мечты к плану, удобно начать с <a href="{url}">{anchor}</a>.',
        ] + options
    return random.choice(options)

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
        (["концерт", "фестиваль", "матч", "выставка"], ["concert crowd travel", "festival city", "stadium travel"]),
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

def build_template_instruction(template: str) -> str:
    mapping = {
        "useful": "Сделай полезный практический пост с конкретной пользой и без воды.",
        "mistake": "Сделай пост в формате ошибка / антиошибка: что люди делают не так и как исправить.",
        "selection": "Сделай пост в формате подборки / рейтинга / сравнения вариантов.",
        "engagement": "Сделай вовлекающий вопрос, который вызывает комментарии и обсуждение.",
        "provocation": "Сделай провокационный, но не токсичный пост со спорным тезисом.",
        "trust": "Сделай доверительный пост как личное наблюдение автора.",
        "expert": "Сделай экспертный разбор с неочевидимым нюансом.",
        "seasonal": "Сделай сезонный / ситуативный пост, привязанный к моменту.",
        "case": "Сделай мини-кейс или короткую историю ситуации.",
        "selling": "Сделай мягко продающий пост без прямой рекламы, через проблему и решение.",
        "mini_poll": "Сделай мини-анкету или быстрый опрос.",
        "short": "Сделай короткий ситуативный пост в 2–4 абзаца.",
        "special_low_price_map": "Сделай пост на тему карты низких цен на авиабилеты. Это должен быть полезный, понятный и живой пост.",
        "special_hot_tours": "Сделай пост на тему горящих туров. Это должен быть живой и удобный для Telegram пост.",
        "special_hotels_discount": "Сделай пост на тему отелей со скидкой до 80%.",
    }
    return mapping.get(template, "Сделай живой travel-пост.")

def template_forbidden_rules(template: str) -> str:
    if template in NO_LINK_TEMPLATES:
        return "Не намекай на сервисы и не пиши про ботов или ссылки."
    return "Не упоминай бренды сервисов и не вставляй ссылки сам."

def template_structure_hint(template: str) -> str:
    if template in {"short", "mini_poll", "engagement", "provocation"}:
        return "Структура: сильный заход, 2–3 коротких абзаца, живая концовка."
    if template in {"selection", "expert", "selling"}:
        return "Структура: яркий заход, 2–4 смысловых блока, человеческий финал."
    return "Структура: живой заход, 2–4 смысловых блока, теплая концовка."

async def generate_post_via_openai(
    topic: str,
    template: str,
    content_mode: str = "useful",
    signal_title: str = "",
    signal_summary: str = "",
    signal_url: str = "",
    signal_source_name: str = "",
    signal_kind: str = "",
) -> str:
    length_min, length_max = get_length_range(template)
    style_seed = random.choice([
        "пиши так, будто это авторский Telegram-канал, а не статья",
        "пиши легко, с ощущением живого travel-канала",
        "пиши так, чтобы текст хотелось дочитать с телефона",
        "пиши как человек, который реально любит путешествия, а не как справочник",
    ])
    mode_instruction = {
        "offer": "Фокус на конкретное предложение, выгоду, цифры, сроки, маршрут, бюджет или повод перейти по ссылке. Не пиши абстрактно.",
        "useful": "Фокус на конкретной пользе, ошибках, проверках, советах и применимости на практике.",
        "idea": "Фокус на интересном месте, идее поездки, эмоции, необычном формате отдыха или маршруте.",
        "engagement": "Фокус на реакции аудитории: вопрос, выбор, спорный тезис, мини-опрос или викторина.",
    }.get(content_mode, "Пиши как сильный Telegram-пост про путешествия.")
    signal_block = ""
    if signal_title or signal_summary or signal_url:
        signal_block = f"""
Сигнал / источник:
- название сигнала: {signal_title or topic}
- краткое содержание: {signal_summary or 'нет'}
- источник: {signal_source_name or 'нет'}
- ссылка на источник: {signal_url or 'нет'}
- тип сигнала: {signal_kind or content_mode}
""".strip()
    prompt = f"""
Ты пишешь Telegram-пост для travel-канала «Мир на ладони».

Тема: {topic}
Шаблон: {template}
Контент-режим: {content_mode}

Требования:
- язык: русский
- стиль: живой, человеческий, читаемый с телефона
- {style_seed}
- без канцелярита, штампов, AI-воды
- не пиши слово "Заголовок" или "Title" в начале поста
- 1–2 эмодзи, максимум 3
- длина текста: {length_min}-{length_max} символов
- {template_structure_hint(template)}
- текст должен быть законченным
- не используй markdown со звёздочками
- делай абзацы короткими
- не делай одинаковый шаблонный тон
- не превращай текст в скучную памятку
- избегай сухих фраз вроде "вопрос уже актуален", "тема горит", "проще заранее посмотреть"
- если обещаешь список, количество пунктов в тексте должно точно совпадать с цифрой в заголовке
- если даёшь подборку событий, мест или советов, не обрывай список на середине
- финал делай тёплым и человеческим: вдохнови, предложи обсудить планы и мягко подведи к путешествию
- если тема про место, формат отдыха или атмосферу — добавь ощущение живого интереса
- если тема про пользу — дай конкретику, а не общие слова
- {mode_instruction}
- {template_forbidden_rules(template)}

Инструкция:
{build_template_instruction(template)}

{signal_block}

Верни только готовый текст поста.
""".strip()
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=1.05,
        max_tokens=900,
        messages=[
            {"role": "system", "content": "Ты сильный редактор Telegram-канала про путешествия, рост охватов и нативную монетизацию."},
            {"role": "user", "content": prompt},
        ],
    )
    text = (response.choices[0].message.content or "").strip()
    if not text:
        raise ValueError("OpenAI вернул пустой текст")
    return cleanup_post_text(text)

async def generate_post_with_retry(
    topic: str,
    template: str,
    content_mode: str = "useful",
    signal_title: str = "",
    signal_summary: str = "",
    signal_url: str = "",
    signal_source_name: str = "",
    signal_kind: str = "",
    retries: int = 3,
    delay: int = 4,
) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            text = await generate_post_via_openai(
                topic=topic,
                template=template,
                content_mode=content_mode,
                signal_title=signal_title,
                signal_summary=signal_summary,
                signal_url=signal_url,
                signal_source_name=signal_source_name,
                signal_kind=signal_kind,
            )
            if looks_incomplete(text):
                raise ValueError("Сгенерированный текст выглядит незавершённым")
            return text
        except Exception as exc:
            last_error = exc
            logger.exception(
                "Ошибка генерации поста | topic=%s | template=%s | attempt=%s/%s",
                topic, template, attempt, retries,
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

def services_from_row(row) -> List[Dict[str, Any]]:
    services: List[Dict[str, Any]] = []
    if not row or not row["referral_url"]:
        return services
    urls = [u.strip() for u in (row["referral_url"] or "").split(",") if u.strip()]
    if (row["monetization_service"] or "").startswith("special_"):
        special = {
            "template": row["post_type"],
            "url": urls[0] if urls else "",
            "service_key": row["monetization_service"],
        }
        return [make_special_service(special)] if urls else []
    for url in urls[:3]:
        matched = next((s for s in SERVICES if s["url"] == url), None)
        if matched:
            services.append(matched)
    return services

def render_post_preview(row) -> str:
    if not row:
        return ""
    recent_posts = get_recent_posts(7)
    return format_post_text(
        content=row["content"] or "",
        template=row["post_type"] or "useful",
        services=services_from_row(row),
        recent_posts=recent_posts,
        photo_credit=row["photo_credit"],
        photo_source_url=row["photo_source_url"],
        max_plain_len=4096,
    )

def build_link_blocks(services: List[Dict[str, Any]], template: str) -> List[str]:
    return [build_native_link_paragraph(service, template) for service in services[:3]]

def service_button_text(service: Dict[str, Any]) -> str:
    category = service.get("category", "")
    mapping = {
        "tours": "Лучшие туры",
        "flights": "Доступные билеты",
        "hotels": "Забронировать отель",
        "insurance": "Страховка",
        "excursions": "Что посмотреть",
        "transfer": "Трансфер",
        "car_rental": "Аренда авто",
        "events": "Билеты на события",
        "general_travel": "Варианты поездки",
        "general_bot": "Подобрать поездку",
    }
    return mapping.get(category, "Полезная ссылка")

def build_inline_buttons(services: List[Dict[str, Any]], post_id: int) -> Optional[InlineKeyboardMarkup]:
    rows: List[List[InlineKeyboardButton]] = []
    service_buttons: List[InlineKeyboardButton] = []
    used_urls = set()
    for service in services[:2]:
        url = service.get("url", "").strip()
        if not url or url in used_urls:
            continue
        used_urls.add(url)
        service_buttons.append(InlineKeyboardButton(service_button_text(service), url=url))
    if service_buttons:
        rows.append(service_buttons)
    if post_id % 10 == 0:
        rows.append([InlineKeyboardButton("ТурДжин", url="https://t.me/TourJin_bot")])
    else:
        rows.append([InlineKeyboardButton("Подписаться на канал", url="https://t.me/NadoTurKrd")])
    return InlineKeyboardMarkup(rows) if rows else None

def format_post_text(
    content: str,
    template: str,
    services: List[Dict[str, Any]],
    recent_posts,
    photo_credit: Optional[str],
    photo_source_url: Optional[str],
    max_plain_len: int = 4096,
) -> str:
    base_html = convert_plain_text_to_html(content)
    blocks = []
    blocks.extend(build_link_blocks(services, template))
    cta_text = build_cta(template, recent_posts)
    if cta_text:
        blocks.append(escape_html_text(cta_text))
    blocks.append(escape_html_text("Делитесь в комментариях, какие идеи и планы у вас на ближайший отпуск или уикенд."))
    blocks.append(BRAND_SIGNATURE_HTML)
    blocks.append(escape_html_text(build_brand_hashtags(content, template, services)))
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
    max_base_len = max_plain_len - len(plain_ending) - len(plain_credit) - 4
    trimmed_plain_base = trim_to_limit(plain_base, max(180, max_base_len))
    base_html = convert_plain_text_to_html(trimmed_plain_base)
    final_html = f"{base_html}\n\n{ending_html}{credit_block}".strip() if ending_html else f"{base_html}{credit_block}".strip()
    final_plain = re.sub(r"<[^>]+>", "", final_html)
    if len(final_plain) > max_plain_len:
        overflow = len(final_plain) - max_plain_len
        trimmed_plain_base = trim_to_limit(trimmed_plain_base, max(160, len(trimmed_plain_base) - overflow - 10))
        base_html = convert_plain_text_to_html(trimmed_plain_base)
        final_html = f"{base_html}\n\n{ending_html}{credit_block}".strip() if ending_html else f"{base_html}{credit_block}".strip()
    return final_html

def save_post(
    topic: str,
    theme_source: str,
    goal: str,
    post_type: str,
    content_mode: str,
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
                topic, theme_source, goal, post_type, content_mode, content,
                referral_url, monetization_service,
                photo_path, photo_credit, photo_source_url,
                status, created_at, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            topic, theme_source, goal, post_type, content_mode, content,
            referral_url, monetization_service,
            photo_path, photo_credit, photo_source_url,
            status, now_iso(), None,
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

def build_special_post(now_dt: datetime, slot_index: int, total_slots: int) -> Optional[Dict[str, str]]:
    weekday = now_dt.weekday()
    if weekday == 1 and slot_index == 2:
        return {
            "template": "special_low_price_map",
            "topic": "Карта низких цен на авиабилеты",
            "url": "https://aviasales.tp.st/05TrktsG?erid=2VtzqwmJxgb",
            "service_key": "special_low_price_map",
        }
    if weekday == 3 and slot_index == total_slots:
        return {
            "template": "special_hot_tours",
            "topic": "Горящие туры",
            "url": "https://travelata.tp.st/42HvBmFJ?erid=2VtzqwyVPEu",
            "service_key": "special_hot_tours",
        }
    if weekday == 4 and slot_index == 2:
        return {
            "template": "special_hotels_discount",
            "topic": "Отели со скидкой до 80%",
            "url": "https://www.trip.com/t/OdWcYTrlHU2",
            "service_key": "special_hotels_discount",
        }
    return None

def make_special_service(special: Dict[str, str]) -> Dict[str, Any]:
    if special["template"] == "special_low_price_map":
        return {
            "key": special["service_key"],
            "name": "Low Price Map",
            "category": "flights",
            "url": special["url"],
            "anchor_options": ["карту низких цен", "низкие цены на авиабилеты", "дешёвые варианты перелёта"],
        }
    if special["template"] == "special_hot_tours":
        return {
            "key": special["service_key"],
            "name": "Hot Tours",
            "category": "tours",
            "url": special["url"],
            "anchor_options": ["горящие туры", "варианты горящих туров", "туры по акции"],
        }
    return {
        "key": special["service_key"],
        "name": "Hotel Discounts",
        "category": "hotels",
        "url": special["url"],
        "anchor_options": ["отели со скидкой", "варианты отелей со скидкой", "отели по акции"],
    }

async def create_post_record(
    topic: str,
    source_type: str,
    goal: str,
    content_mode: Optional[str] = None,
    signal_title: str = "",
    signal_summary: str = "",
    signal_url: str = "",
    signal_source_name: str = "",
    signal_kind: str = "",
    forced_post_type: Optional[str] = None,
    forced_services: Optional[List[Dict[str, Any]]] = None,
) -> int:
    recent_posts = get_recent_posts(7)
    content_mode = content_mode or choose_content_mode()
    template = forced_post_type or choose_post_template(content_mode)
    content = await generate_post_with_retry(
        topic=topic,
        template=template,
        content_mode=content_mode,
        signal_title=signal_title,
        signal_summary=signal_summary,
        signal_url=signal_url,
        signal_source_name=signal_source_name,
        signal_kind=signal_kind,
    )
    services = forced_services if forced_services is not None else choose_services(topic, content, template, recent_posts)
    referral_url = ",".join([s["url"] for s in services[:3]]) if services else ""
    monetization_service = services[0]["key"] if services else ""
    logger.info(
        "POST_DECISION | version=%s | topic=%s | mode=%s | template=%s | service=%s | urls=%s | source=%s",
        APP_VERSION, topic, content_mode, template, monetization_service or "none", referral_url or "none", source_type,
    )
    photo_path, photo_credit, photo_source_url = await fetch_pexels_photo(topic)
    post_id = save_post(
        topic=topic, theme_source=source_type, goal=goal, post_type=template, content_mode=content_mode, content=content,
        referral_url=referral_url, monetization_service=monetization_service,
        photo_path=photo_path, photo_credit=photo_credit, photo_source_url=photo_source_url,
        status="draft",
    )
    increment_post_type_stat(template)
    return post_id

async def publish_post_record(bot, channel_id: str, row, mark_status: str = "published"):
    recent_posts = get_recent_posts(7)
    services: List[Dict[str, Any]] = []
    if row["referral_url"]:
        urls = [u.strip() for u in (row["referral_url"] or "").split(",") if u.strip()]
        if (row["monetization_service"] or "").startswith("special_"):
            special = {
                "template": row["post_type"],
                "url": urls[0] if urls else "",
                "service_key": row["monetization_service"],
            }
            services = [make_special_service(special)] if urls else []
        else:
            for url in urls[:3]:
                matched = next((s for s in SERVICES if s["url"] == url), None)
                if matched:
                    services.append(matched)
    text = format_post_text(
        content=row["content"],
        template=row["post_type"] or "useful",
        services=services,
        recent_posts=recent_posts,
        photo_credit=row["photo_credit"],
        photo_source_url=row["photo_source_url"],
        max_plain_len=4096,
    )
    reply_markup = build_inline_buttons(services, row["id"])
    photo_path = row["photo_path"]
    plain_text_len = len(re.sub(r"<[^>]+>", "", text))
    if photo_path and Path(photo_path).exists():
        if plain_text_len <= 1024:
            with open(photo_path, "rb") as photo_file:
                await bot.send_photo(chat_id=channel_id, photo=photo_file, caption=text, parse_mode="HTML", reply_markup=reply_markup)
        else:
            with open(photo_path, "rb") as photo_file:
                await bot.send_photo(chat_id=channel_id, photo=photo_file)
            await bot.send_message(chat_id=channel_id, text=text, parse_mode="HTML", disable_web_page_preview=False, reply_markup=reply_markup)
    else:
        await bot.send_message(chat_id=channel_id, text=text, parse_mode="HTML", disable_web_page_preview=False, reply_markup=reply_markup)
    update_post_status(row["id"], mark_status)

async def scheduled_autopost_job(app: Application):
    if get_setting("autopost_enabled", "1") != "1":
        logger.info("Автопостинг отключён, задача пропущена")
        return
    schedule_raw = get_setting("post_times", DEFAULT_POST_TIMES or "09:00,14:00,19:00")
    now_dt = datetime.now(BOT_TZ)
    times = parse_schedule(schedule_raw)
    slot_index = get_today_post_slot(now_dt, schedule_raw)
    total_slots = len(times)
    special = build_special_post(now_dt, slot_index, total_slots)
    if special:
        post_id = await create_post_record(
            topic=special["topic"],
            source_type="special_schedule",
            goal="autopost",
            content_mode="offer",
            forced_post_type=special["template"],
            forced_services=[make_special_service(special)],
        )
        row = get_post(post_id)
        await publish_post_record(app.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
        logger.info("Спецпост опубликован | version=%s | template=%s | topic=%s", APP_VERSION, special["template"], special["topic"])
        return
    item = get_next_content_input()
    post_id = await create_post_record(
        topic=item["topic"],
        source_type=item["source_type"],
        goal=item["goal"],
        content_mode=item["content_mode"],
        signal_title=item["signal_title"],
        signal_summary=item["signal_summary"],
        signal_url=item["signal_url"],
        signal_source_name=item["signal_source_name"],
        signal_kind=item["signal_kind"],
        forced_post_type=None,
    )
    row = get_post(post_id)
    await publish_post_record(app.bot, TELEGRAM_CHANNEL_ID, row, mark_status="published")
    logger.info(
        "Автопост опубликован | version=%s | id=%s | topic=%s | type=%s | service=%s",
        APP_VERSION, post_id, item["topic"], row["post_type"], row["monetization_service"],
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
        item = get_next_content_input()
        topic = item["topic"]
        source_type = item["source_type"]
        content_mode = item["content_mode"]
        signal_title = item["signal_title"]
        signal_summary = item["signal_summary"]
        signal_url = item["signal_url"]
        signal_source_name = item["signal_source_name"]
        signal_kind = item["signal_kind"]
    else:
        source_type = "manual"
        content_mode = "idea"
        signal_title = ""
        signal_summary = ""
        signal_url = ""
        signal_source_name = ""
        signal_kind = ""
    await safe_reply(update, f"Генерирую пост по теме: {topic}")
    post_id = await create_post_record(
        topic=topic,
        source_type=source_type,
        goal="manual",
        content_mode=content_mode,
        signal_title=signal_title,
        signal_summary=signal_summary,
        signal_url=signal_url,
        signal_source_name=signal_source_name,
        signal_kind=signal_kind,
        forced_post_type=None,
    )
    row = get_post(post_id)
    preview = render_post_preview(row)
    await safe_reply(
        update,
        f"Пост создан.\n\n{format_post_card(row)}\n\nФинальный вид при публикации:\n\n{preview}\n\nДля публикации: /publish {post_id}\nДля теста: /test_post {post_id}"
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
        f"Скользящая статистика типов: {stats_text}"
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
            forced_post_type="short",
        )
        row = get_post(post_id)
    try:
        await publish_post_record(context.bot, test_channel_id, row, mark_status="tested")
        await safe_reply(update, f"Тестовая публикация отправлена в {test_channel_id}.")
    except Forbidden:
        await safe_reply(update, "Не удалось отправить в тестовый канал: бот не состоит в канале или у него нет прав публикации.")

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
        rows = conn.execute("SELECT id, topic_text, source_type, used FROM topics ORDER BY id DESC LIMIT 15").fetchall()
    if not rows:
        await safe_reply(update, "Тем пока нет.")
        return
    text = "Последние темы:\n\n"
    for row in rows:
        text += f"{row['id']}. {row['topic_text']} | источник={row['source_type']} | used={row['used']}\n"
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

@admin_only
async def refresh_signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update, "Обновляю сигналы из публичных источников...")
    removed = purge_blocked_signals()
    inserted = discover_public_signals(force=True)
    rows = list_signals(limit=10, only_unused=True)
    text = f"Готово. Добавлено/обновлено сигналов: {inserted}\nУдалено нежелательных сигналов: {removed}\n\n"
    if not rows:
        text += "Сигналов пока нет."
    else:
        text += "Последние сигналы:\n"
        for row in rows:
            text += f"\n{row['id']}. [{row['signal_kind']}] {row['title']}\nИсточник: {row['source_name']}\nScore: {row['score']}\nUsed: {row['used']}\n"
    await safe_reply(update, text[:3900])

@admin_only
async def signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    only_unused = bool(context.args and context.args[0].lower() in {"unused", "new", "0"})
    rows = list_signals(limit=15, only_unused=only_unused)
    if not rows:
        await safe_reply(update, "Сигналов пока нет. Используйте /refresh_signals")
        return
    title = "Неиспользованные сигналы" if only_unused else "Последние сигналы"
    text = title + ":\n"
    for row in rows:
        text += f"\n{row['id']}. [{row['signal_kind']}] {row['title']}\nИсточник: {row['source_name']}\nScore: {row['score']} | Used: {row['used']}\n"
    text += "\nКоманды: /refresh_signals, /signals unused, /gen_signal, /reset_signals"
    await safe_reply(update, text[:3900])

@admin_only
async def reset_signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = (context.args[0].lower() if context.args else "used")
    if mode == "all":
        count = reset_signals(only_used=False)
        await safe_reply(update, f"Сброшены флаги used у всех сигналов: {count}")
        return
    if mode == "clear":
        count = clear_signals()
        await safe_reply(update, f"Удалено сигналов: {count}")
        return
    count = reset_signals(only_used=True)
    await safe_reply(update, f"Сброшены только использованные сигналы: {count}\n\nДля полного сброса: /reset_signals all\nДля удаления всех: /reset_signals clear")

@admin_only
async def gen_signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    preferred_mode = context.args[0].strip().lower() if context.args else ""
    item = get_next_content_input()
    if preferred_mode in {"offer", "useful", "idea", "engagement"}:
        signal = get_next_signal(preferred_mode)
        if signal:
            item = {
                "topic": signal["title"],
                "source_type": signal["source_type"] or "signal",
                "goal": "manual",
                "content_mode": preferred_mode,
                "signal_title": signal["title"] or "",
                "signal_summary": signal["summary"] or "",
                "signal_url": signal["url"] or "",
                "signal_kind": signal["signal_kind"] or preferred_mode,
                "signal_source_name": signal["source_name"] or "",
            }
    await safe_reply(update, f"Генерирую пост из сигнала.\nТема: {item['topic']}\nРежим: {item['content_mode']}")
    post_id = await create_post_record(
        topic=item["topic"],
        source_type=item["source_type"],
        goal="manual",
        content_mode=item["content_mode"],
        signal_title=item["signal_title"],
        signal_summary=item["signal_summary"],
        signal_url=item["signal_url"],
        signal_source_name=item["signal_source_name"],
        signal_kind=item["signal_kind"],
        forced_post_type=None,
    )
    row = get_post(post_id)
    await safe_reply(update, f"Пост из сигнала создан.\n\n{format_post_card(row)}\n\nДля публикации: /publish {post_id}\nДля теста: /test_post {post_id}")

@admin_only
async def run_once_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update, "Запускаю один полный цикл автопоста в тестовом режиме...")
    item = get_next_content_input()
    post_id = await create_post_record(
        topic=item["topic"],
        source_type=item["source_type"],
        goal="manual",
        content_mode=item["content_mode"],
        signal_title=item["signal_title"],
        signal_summary=item["signal_summary"],
        signal_url=item["signal_url"],
        signal_source_name=item["signal_source_name"],
        signal_kind=item["signal_kind"],
        forced_post_type=None,
    )
    row = get_post(post_id)
    await safe_reply(update, f"Цикл выполнен.\n\n{format_post_card(row)}\n\nДля теста в канал: /test_post {post_id}")

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
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
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
    app.add_handler(CommandHandler("refresh_signals", refresh_signals_cmd))
    app.add_handler(CommandHandler("signals", signals_cmd))
    app.add_handler(CommandHandler("reset_signals", reset_signals_cmd))
    app.add_handler(CommandHandler("gen_signal", gen_signal_cmd))
    app.add_handler(CommandHandler("run_once", run_once_cmd))
    app.add_error_handler(error_handler)
    return app

def main():
    init_db()
    app = build_application()
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
