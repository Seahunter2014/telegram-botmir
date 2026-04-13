# ====== НОВАЯ СИСТЕМА ШАБЛОНОВ ======

POST_TEMPLATES = {
    "value": 12,
    "mistake": 12,
    "list": 10,
    "engagement": 10,
    "provocation": 5,
    "trust": 8,
    "expert": 10,
    "seasonal": 8,
    "case": 7,
    "selling": 12,
    "poll": 3,
    "short": 3,
}

NO_LINK_TYPES = {"engagement", "provocation", "trust", "poll"}
FORCE_LINK_TYPES = {"selling", "list", "value", "expert", "mistake", "seasonal"}

CTA_TYPES = [
    "save",
    "comment",
    "choice",
    "share",
    "subscribe",
    "soft_sell",
    "no_cta",
]

# ====== ВЫБОР ТИПА ПОСТА ======

def choose_post_template():
    total = sum(POST_TEMPLATES.values())
    rnd = random.randint(1, total)

    cumulative = 0
    for key, weight in POST_TEMPLATES.items():
        cumulative += weight
        if rnd <= cumulative:
            return key
    return "value"


# ====== ДЛИНА ======

def get_length_range(template):
    if template in {"short", "poll", "engagement"}:
        return (300, 550)
    elif template in {"list", "expert", "selling"}:
        return (700, 950)
    else:
        return (550, 850)


# ====== CTA ======

def choose_cta(template):
    mapping = {
        "engagement": ["choice", "comment"],
        "provocation": ["comment"],
        "trust": ["no_cta"],
        "poll": ["choice"],
        "selling": ["soft_sell", "share"],
    }
    return random.choice(mapping.get(template, CTA_TYPES))


def build_cta(cta_type):
    ctas = {
        "save": ["Сохрани, чтобы не потерять."],
        "comment": ["Что думаешь по этому поводу?"],
        "choice": ["А ты бы что выбрал?"],
        "share": ["Отправь тому, кому это пригодится."],
        "subscribe": ["Подпишись, если тема откликается."],
        "soft_sell": ["Если тема актуальна — лучше проверить заранее."],
        "no_cta": [""],
    }
    return random.choice(ctas[cta_type])


# ====== ЛОГИКА ССЫЛОК ======

def should_add_link(template, recent_posts):
    if template in NO_LINK_TYPES:
        return False

    last_links = sum(1 for p in recent_posts[:2] if p["monetization_service"])
    if last_links >= 1:
        return False

    return template in FORCE_LINK_TYPES or random.random() < 0.4


# ====== СПЕЦПОСТЫ ======

def check_special_post(now_dt, post_index):
    weekday = now_dt.weekday()

    # вторник
    if weekday == 1 and post_index == 2:
        return {
            "type": "special_aviasales",
            "url": "https://aviasales.tp.st/05TrktsG?erid=2VtzqwmJxgb"
        }

    # четверг
    if weekday == 3 and post_index == 3:
        return {
            "type": "special_tours",
            "url": "https://travelata.tp.st/42HvBmFJ?erid=2VtzqwyVPEu"
        }

    # пятница
    if weekday == 4 and post_index == 2:
        return {
            "type": "special_hotels",
            "url": "https://www.trip.com/t/OdWcYTrlHU2"
        }

    return None


# ====== PROMPT ======

async def generate_post_via_openai(topic, template):

    length_min, length_max = get_length_range(template)

    prompt = f"""
Ты пишешь пост для Telegram travel-канала.

Тип: {template}
Тема: {topic}

Требования:
- живой стиль
- не сухо
- 1–2 эмодзи
- длина {length_min}-{length_max}
- без ссылок
- без рекламы
- структура:
  заголовок
  короткий заход
  2-3 блока
  вывод

Сделай текст НЕ шаблонным.
"""

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=1.0,
    )

    return cleanup_post_text(response.choices[0].message.content)


# ====== СБОРКА ФИНАЛА ======

def format_post_text(content, template, service, recent_posts):

    base = convert_plain_text_to_html(content)

    blocks = []

    if service:
        blocks.append(build_native_link_paragraph(service, template))

    cta = build_cta(choose_cta(template))
    if cta:
        blocks.append(cta)

    return base + "\n\n" + "\n\n".join(blocks)
