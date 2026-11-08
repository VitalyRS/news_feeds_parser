#!/usr/bin/env python3
"""
Telegram-бот с long-polling для обработки inline-кнопок.

Функции:
1. Слушает callback_query от inline-кнопок (отправленных telegram_notifier.py)
2. При нажатии "📝 Сгенерировать ТГ-пост" → показывает кнопки выбора ноутбука (до 4 мес.)
3. При выборе ноутбука → отправляет editorial_angles в NotebookLM RAG (chat.ask)
4. Полученный RAG-ответ прогоняет через AnyModel LLM для финальной полировки
5. Отправляет готовый ТГ-пост пользователю
6. Команда /top — отправляет ТОП оценённых новостей с кнопками из БД

Команды:
  /start                — приветствие и справка
  /top                  — интерактивный выбор (кол-во + дата)
  /top 20               — отправить ТОП-20 за сегодня
  /top 20 today         — то же самое
  /top 10 2026-09-02    — ТОП-10 за конкретную дату
  /top 15 yesterday     — ТОП-15 за вчера
  /status               — статус бота, ноутбуки, LLM

Запуск:
  python3 telegram_bot.py                    # Обычный режим
  python3 telegram_bot.py --verbose          # Расширенное логирование

Требования:
  - TELEGA_BOT в .env — токен бота
  - TELEGA_CHAT_ID в .env — ID чата (опционально)
  - NotebookLM авторизация: notebooklm login (разово)
  - ANYMODEL_KEY в .env — для пост-обработки
"""

import os
import sys
import json
import re
import time
import uuid
import sqlite3
import asyncio
import logging
import argparse
import threading
from datetime import datetime, timedelta, date, timezone
from typing import Optional, Dict, Any, List

import requests

# =================== КОНФИГУРАЦИЯ ===================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'telegram_bot.log')
ARTICLES_CONTEXT_FILE = os.path.join(SCRIPT_DIR, '.tg_bot_articles.json')
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')
QUERY_SESSIONS_FILE = os.path.join(SCRIPT_DIR, '.tg_query_sessions.json')
POST_QUEUE_FILE = os.path.join(SCRIPT_DIR, '.tg_post_queue.json')

# Хранилище состояний FSM в памяти: chat_id -> {"action": "waiting_ask_question" | "waiting_post_topic", ...}
USER_STATES: Dict[str, Dict[str, Any]] = {}

# Загрузка .env
def load_env_file():
    env_paths = [
        os.path.join(SCRIPT_DIR, '.env'),
        os.path.join(SCRIPT_DIR, 'analysis', '.env'),
    ]
    for env_path in env_paths:
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k not in os.environ:
                            os.environ[k] = v

load_env_file()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger("telegram_bot")

BOT_TOKEN = os.environ.get("TELEGA_BOT") or os.environ.get("TELEGRAM_BOT_TOKEN")
ANYMODEL_KEY = os.environ.get("ANYMODEL_KEY")
ANYMODEL_BASE_URL = os.environ.get("ANYMODEL_BASE_URL", "https://anymodel.org/v1").rstrip("/")
ANYMODEL_MODEL = os.environ.get("ANYMODEL_MODEL", "ag/gemini-2.5-flash")

# Каналы для публикаций и интервал очереди
TELEGRAM_CHANNEL = os.environ.get("TELEGRAM_CHANNEL", "@iberia_news").strip()
if not TELEGRAM_CHANNEL.startswith("@") and not TELEGRAM_CHANNEL.startswith("-100"):
    TELEGRAM_CHANNEL = f"@{TELEGRAM_CHANNEL}"

TELEGRAM_SECONDARY_CHANNEL = os.environ.get("TELEGRAM_SECONDARY_CHANNEL", "@noticia666").strip()
if TELEGRAM_SECONDARY_CHANNEL and not TELEGRAM_SECONDARY_CHANNEL.startswith("@") and not TELEGRAM_SECONDARY_CHANNEL.startswith("-100"):
    TELEGRAM_SECONDARY_CHANNEL = f"@{TELEGRAM_SECONDARY_CHANNEL}"

PUBLISH_INTERVAL_HOURS = int(os.environ.get("PUBLISH_INTERVAL_HOURS", "8"))

# 6 категорий публикации для маршрутизации (cat1 - cat6)
POST_CATEGORIES: Dict[str, Dict[str, str]] = {
    "cat1": {
        "code": "cat1",
        "num": "1",
        "emoji": "📰",
        "title": "1. 📰 Новости Испании",
        "btn": "1. 📰 Новости (cat1)",
        "desc": "Главные события Испании: политика, экономика, происшествия, общество, скандалы, Россия в СМИ Испании."
    },
    "cat2": {
        "code": "cat2",
        "num": "2",
        "emoji": "📑",
        "title": "2. 📑 Документы и гражданство",
        "btn": "2. 📑 Документы (cat2)",
        "desc": "ВНЖ, визы, NIE/TIE, продление документов, гражданство, налоги, соцвопросы и админпроцедуры."
    },
    "cat3": {
        "code": "cat3",
        "num": "3",
        "emoji": "💡",
        "title": "3. 💡 Гайды, советы и лайфхаки",
        "btn": "3. 💡 Гайды (cat3)",
        "desc": "Практические инструкции: банки, связь, транспорт, покупки, экономия, сервисы и бытовые вопросы."
    },
    "cat4": {
        "code": "cat4",
        "num": "4",
        "emoji": "🇪🇸",
        "title": "4. 🇪🇸 Учим испанский",
        "btn": "4. 🇪🇸 Испанский (cat4)",
        "desc": "Изучение испанского языка: слова, грамматика, разговорная практика, подготовка к DELE A2 и CCSE."
    },
    "cat5": {
        "code": "cat5",
        "num": "5",
        "emoji": "📍",
        "title": "5. 📍 Куда сходить и путешествия",
        "btn": "5. 📍 Куда сходить (cat5)",
        "desc": "Интересные места, рестораны, концерты, выставки, праздники, бесплатные мероприятия, маршруты и поездки."
    },
    "cat6": {
        "code": "cat6",
        "num": "6",
        "emoji": "👨‍👩‍👧",
        "title": "6. 👨‍👩‍👧 Семья и здоровье",
        "btn": "6. 👨‍👩‍👧 Семья/Здоровье (cat6)",
        "desc": "Медицина, врачи, страховки, школы, детские сады, кружки, спорт, семейный отдых и воспитание детей."
    },
}

# Максимальная глубина выбора ноутбуков (месяцев назад)
MAX_NOTEBOOK_MONTHS = 4

# =================== TELEGRAM API (ЧИСТЫЙ requests) ===================


def tg_api(method: str, **kwargs) -> Optional[dict]:
    """Вызывает метод Telegram Bot API."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    # Убираем None значения, чтобы не отправлять 'null' в Telegram API (вызывает 400 Bad Request)
    payload = {k: v for k, v in kwargs.items() if v is not None}
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            if "message is not modified" in resp.text:
                log.debug("ℹ️ TG API: сообщение не изменено")
                return None
            log.error("❌ TG API %s ошибка (%d): %s", method, resp.status_code, resp.text[:300])
            return None
    except Exception as e:
        log.error("❌ TG API %s ошибка соединения: %s", method, e)
        return None


def answer_callback_query(callback_query_id: str, text: str = "", show_alert: bool = False):
    """Отвечает на callback_query (убирает "часики" на кнопке)."""
    tg_api("answerCallbackQuery",
           callback_query_id=callback_query_id,
           text=text[:200],
           show_alert=show_alert)


def send_message(chat_id: str, text: str, parse_mode: Optional[str] = "HTML",
                 reply_markup: Optional[dict] = None,
                 disable_web_page_preview: bool = False) -> Optional[dict]:
    """Отправляет сообщение в Telegram."""
    params = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        params["parse_mode"] = parse_mode
    if reply_markup:
        params["reply_markup"] = reply_markup
    return tg_api("sendMessage", **params)


def edit_message_text(chat_id: str, message_id: int, text: str,
                      parse_mode: Optional[str] = "HTML",
                      reply_markup: Optional[dict] = None,
                      disable_web_page_preview: bool = False):
    """Редактирует текст существующего сообщения."""
    params = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    if parse_mode:
        params["parse_mode"] = parse_mode
    if reply_markup:
        params["reply_markup"] = reply_markup
    if disable_web_page_preview:
        params["disable_web_page_preview"] = disable_web_page_preview
    return tg_api("editMessageText", **params)


def edit_message_reply_markup(chat_id: str, message_id: int,
                              reply_markup: Optional[dict] = None):
    """Редактирует кнопки существующего сообщения."""
    params = {
        "chat_id": chat_id,
        "message_id": message_id,
    }
    if reply_markup:
        params["reply_markup"] = reply_markup
    return tg_api("editMessageReplyMarkup", **params)


def send_long_message(chat_id: str, text: str, parse_mode: Optional[str] = "HTML",
                      reply_markup: Optional[dict] = None,
                      disable_web_page_preview: bool = True):
    """
    Отправляет сообщение в Telegram, безопасно разбивая на части,
    если длина превышает лимит Telegram (4096 символов).
    При ошибке parse_mode пробует отправить чистым текстом.
    Возвращает dict ответа Telegram API для последнего отправленного сообщения.
    """
    max_chunk = 3800
    if len(text) <= max_chunk:
        resp = send_message(chat_id, text, parse_mode=parse_mode,
                            reply_markup=reply_markup,
                            disable_web_page_preview=disable_web_page_preview)
        if not resp and parse_mode:
            resp = send_message(chat_id, text, parse_mode=None,
                                reply_markup=reply_markup,
                                disable_web_page_preview=disable_web_page_preview)
        return resp

    chunks = []
    lines = text.split("\n")
    current_chunk = ""
    for line in lines:
        if len(current_chunk) + len(line) + 1 > max_chunk:
            if current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = ""
            if len(line) > max_chunk:
                for i in range(0, len(line), max_chunk):
                    chunks.append(line[i:i + max_chunk])
            else:
                current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"
    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    last_resp = None
    for idx, ch in enumerate(chunks):
        markup = reply_markup if (idx == len(chunks) - 1) else None
        resp = send_message(chat_id, ch, parse_mode=parse_mode,
                            reply_markup=markup,
                            disable_web_page_preview=disable_web_page_preview)
        if not resp and parse_mode:
            resp = send_message(chat_id, ch, parse_mode=None,
                                reply_markup=markup,
                                disable_web_page_preview=disable_web_page_preview)
        last_resp = resp
        time.sleep(0.3)
    return last_resp


def escape_html(text: str) -> str:
    """Экранирует спецсимволы для parse_mode='HTML' в Telegram."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# =================== ОЧЕРЕДЬ ПУБЛИКАЦИИ В КАНАЛ (@iberia_news) ===================

QUEUE_LOCK = threading.Lock()


def load_queue_data() -> Dict[str, Any]:
    """Загружает очередь публикаций и черновики из .tg_post_queue.json."""
    if os.path.exists(POST_QUEUE_FILE):
        try:
            with open(POST_QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("queue", [])
                    data.setdefault("drafts", {})
                    data.setdefault("favorites", [])
                    return data
        except Exception as e:
            log.warning("⚠️ Ошибка чтения %s: %s", POST_QUEUE_FILE, e)
    return {
        "last_published_at": None,
        "queue": [],
        "drafts": {},
        "favorites": []
    }


def save_queue_data(data: Dict[str, Any]):
    """Атомарно сохраняет очередь публикаций в .tg_post_queue.json."""
    try:
        tmp_file = f"{POST_QUEUE_FILE}.tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, POST_QUEUE_FILE)
    except Exception as e:
        log.error("❌ Ошибка сохранения %s: %s", POST_QUEUE_FILE, e)


def parse_headline_options(text: str) -> tuple[List[Dict[str, Any]], str]:
    """
    Извлекает список вариантов заголовков из блока 💡 ВАРИАНТЫ ЗАГОЛОВКА: в начале поста.
    Возвращает (список_опций, тело_поста).
    """
    m = re.search(r'^(.*?)\n\s*[-_*=]{3,}\s*\n(.*)$', text or "", flags=re.DOTALL)
    if not m:
        head_block = (text or "")[:1500]
        body = text or ""
    else:
        head_block, body = m.group(1), m.group(2)

    options = []
    pat = re.compile(r'^(?:([1-4])️⃣|([1-4])[.)\]])\s*(?:[*_]*\[?([^\]:\n]+)\]?[*_]*[:\-—\s]*)?\s*(.+)$')
    for line in head_block.splitlines():
        line_s = line.strip()
        if not line_s:
            continue
        m_opt = pat.match(line_s)
        if m_opt:
            num = int(m_opt.group(1) or m_opt.group(2))
            cat = (m_opt.group(3) or '').strip('*_[] :—')
            title = (m_opt.group(4) or '').strip('*_\"\' ')
            short_cat = cat.split('/')[0].strip() if cat else f'Вариант {num}'
            if len(short_cat) > 14:
                short_cat = short_cat[:14]
            options.append({
                'idx': num,
                'category': cat,
                'short_category': short_cat,
                'title': title
            })
    return options, body


def apply_chosen_headline(text: str, chosen_title: str) -> str:
    """
    Заменяет заголовок в теле поста (первую строку после блока вариантов заголовков)
    на выбранный пользователем, сохраняя вводный тематический эмодзи и выделение **жирным**.
    """
    m = re.search(r'^(.*?)\n\s*[-_*=]{3,}\s*\n(.*)$', text or "", flags=re.DOTALL)
    if m:
        head_block = m.group(1)
        body = m.group(2)
    else:
        head_block = ''
        body = text or ""

    lines = body.strip().splitlines()
    if not lines:
        return text or ""

    first_line = lines[0].strip()
    emoji_match = re.match(r'^([\U00010000-\U0010ffff\u2600-\u27bf\u2300-\u23ff\u2b50\u203c-\u3299]+)\s*', first_line)
    leading_emoji = emoji_match.group(0) if emoji_match else ''
    if leading_emoji and not leading_emoji.endswith(' '):
        leading_emoji += ' '

    clean_title = chosen_title.strip('*_# ')
    new_first_line = f'{leading_emoji}**{clean_title}**'
    lines[0] = new_first_line
    new_body = '\n'.join(lines)

    if head_block:
        return f'{head_block}\n---\n{new_body}'
    return new_body


def extract_current_body_headline(text: str) -> str:
    """Извлекает текущий заголовок (первую непустую строку тела поста без эмодзи и **)."""
    _, body = parse_headline_options(text)
    lines = [ln.strip() for ln in body.strip().splitlines() if ln.strip()]
    if not lines:
        return ""
    first_line = lines[0]
    first_line = re.sub(
        r'^[\U00010000-\U0010ffff\u2600-\u27bf\u2300-\u23ff\u2b50\u203c-\u3299\ufe0f]+\s*',
        '',
        first_line
    )
    return first_line.strip('*_# «»"\'')


def strip_category_footer(text: str) -> str:
    """Удаляет служебную подпись категории вида '#dai breaking cat3', 'breaking cat3' или 'cat1' в самом конце текста."""
    if not text:
        return ""
    cleaned = re.sub(
        r'(?:\n|\s)+(?:(?:#dai\s+)?(?:breaking\s+)?cat[1-6](?:\s+#dai)?|#dai)\s*$',
        '',
        text.rstrip(),
        flags=re.IGNORECASE
    )
    return cleaned.rstrip()


def extract_category_from_text(text: str) -> Optional[str]:
    """Извлекает код категории (cat1..cat6) из конца текста, если он уже проставлен."""
    if not text:
        return None
    m = re.search(
        r'(?:^|\s)(?:#dai\s+)?(?:breaking\s+)?(cat[1-6])(?:\s+#dai)?\s*$',
        text.rstrip(),
        flags=re.IGNORECASE
    )
    return m.group(1).lower() if m else None


def apply_category_footer(text: str, category_code: str, is_breaking: bool = False) -> str:
    """
    Добавляет в самый конец текста служебную строку для вторичного канала (@noticia666):
    - '#dai breaking cat3' если is_breaking=True
    - '#dai cat3' если is_breaking=False
    """
    if not text:
        return ""
    cat = category_code if category_code in POST_CATEGORIES else "cat1"
    base = strip_category_footer(text)
    suffix = f"#dai breaking {cat}" if is_breaking else f"#dai {cat}"
    return f"{base}\n\n{suffix}"


def detect_post_category(text: str, title: str = "") -> str:
    """
    Автоматически определяет категорию поста (cat1..cat6) по рубрике, заголовку и содержанию текста.
    """
    combined = f"{title}\n{strip_headline_options_block(text)}".lower()
    t_low = (title or "").lower()

    # 1. Быстрые подсказки по названию рубрики
    if any(w in t_low for w in ["инструкции", "как сделать", "лайфхак"]):
        return "cat3"
    if any(w in t_low for w in ["детям и семьям"]):
        return "cat6"
    if any(w in t_low for w in [
        "для пар", "культура и классическая", "бесплатные мероприятия",
        "праздники и фестивали", "шоу и представления", "необычные места",
        "афиша на выходные", "куда поехать", "гастрономия и вино"
    ]):
        return "cat5"
    if any(w in t_low for w in ["законы, налоги и документы"]):
        return "cat2"

    # 2. Подсчёт весов по ключевым маркерам для cat1..cat6
    scores: Dict[str, int] = {"cat1": 1, "cat2": 0, "cat3": 0, "cat4": 0, "cat5": 0, "cat6": 0}

    kw_map = {
        "cat4": [
            ("dele", 5), ("ccse", 5), ("учим испанский", 5), ("испанский язык", 4),
            ("грамматик", 4), ("лексик", 4), ("словарный запас", 4), ("глагол", 3),
            ("произношени", 3), ("разговорная практика", 4), ("фразы на испанском", 4)
        ],
        "cat2": [
            ("внж", 4), ("виз", 3), ("nie", 4), ("tie", 4), ("гражданств", 4),
            ("extranjería", 4), ("экстранхери", 4), ("empadronamiento", 4), ("прописк", 3),
            ("налог", 3), ("irpf", 4), ("деклараци", 3), ("резидент", 3), ("легализац", 4),
            ("arraigo", 4), ("оседлост", 4), ("cita previa", 3), ("autónomo", 3),
            ("аутономо", 3), ("seguridad social", 3), ("пенси", 2), ("пособи", 2), ("субсиди", 2)
        ],
        "cat3": [
            ("пошаговый алгоритм", 4), ("инструкци", 3), ("лайфхак", 4), ("гайд", 4),
            ("как сэкономить", 4), ("как выбрать", 3), ("как открыть счет", 4), ("банковский счет", 3),
            ("тариф", 3), ("электроэнерги", 3), ("счет за свет", 4), ("оператор связи", 3),
            ("проездной", 3), ("abono", 3), ("обмен прав", 4), ("водительские права", 3),
            ("certificado digital", 4), ("электронная подпись", 4), ("cl@ve", 4),
            ("возврат товара", 4), ("права потребителей", 3), ("как не потерять", 3), ("как защититься", 3)
        ],
        "cat5": [
            ("куда сходить", 4), ("куда поехать", 4), ("афиша", 4), ("фестивал", 4),
            ("концерт", 3), ("выставк", 3), ("музе", 3), ("театр", 3), ("маршрут", 4),
            ("экскурси", 4), ("ресторан", 3), ("гастроном", 3), ("дегустац", 4),
            ("ярмарк", 3), ("праздник", 3), ("выходные", 2), ("вход свободн", 4),
            ("бесплатн", 2), ("замок", 3), ("путешеств", 3), ("достопримечательност", 4)
        ],
        "cat6": [
            ("медицин", 4), ("врач", 3), ("больниц", 3), ("госпитал", 3), ("поликлиник", 4),
            ("страховк", 3), ("здоровь", 3), ("лекарств", 3), ("аптек", 3), ("педиатр", 4),
            ("школ", 3), ("школьник", 3), ("colegio", 3), ("детский сад", 4), ("guardería", 4),
            ("кружк", 3), ("воспитани", 4), ("семейн", 2), ("беременнос", 4), ("роды", 3), ("вакцин", 3)
        ],
        "cat1": [
            ("политик", 3), ("правительств", 3), ("парламент", 3), ("суд ", 3), ("полици", 3),
            ("задержан", 3), ("арест", 3), ("преступлен", 3), ("мошеннич", 3), ("афер", 3),
            ("скандал", 3), ("происшестви", 3), ("забастовк", 3), ("протест", 3),
            ("экономик", 2), ("инфляци", 2), ("росси", 4), ("русск", 3), ("испанские сми", 3)
        ],
    }

    for cat_code, kw_list in kw_map.items():
        for kw, weight in kw_list:
            if kw in combined:
                scores[cat_code] += weight

    best_cat = max(scores.items(), key=lambda x: x[1])[0]
    if scores[best_cat] <= 2:
        return "cat1"
    return best_cat


def sync_initial_headline(text: str, title_hint: str = "") -> tuple[str, List[Dict[str, Any]], int]:
    """
    Синхронизирует заголовок в теле поста с одним из сгенерированных вариантов (1..4).
    Возвращает (синхронизированный_чистый_текст, options, selected_idx).
    """
    clean_base = strip_category_footer(text)
    options, _ = parse_headline_options(clean_base)
    if not options:
        return clean_base, [], 1

    curr_h = extract_current_body_headline(clean_base)
    norm_curr = re.sub(r'\W+', '', curr_h.lower())

    matched_opt = None
    if norm_curr:
        for opt in options:
            norm_opt = re.sub(r'\W+', '', opt.get("title", "").lower())
            if norm_opt and (norm_opt == norm_curr or norm_opt in norm_curr or norm_curr in norm_opt):
                matched_opt = opt
                break

    if matched_opt is None:
        matched_opt = options[0]

    synced_text = apply_chosen_headline(clean_base, matched_opt["title"])
    return strip_category_footer(synced_text), options, matched_opt["idx"]


def save_post_draft(
    text: str,
    title: str = "",
    chat_id: str = "",
    draft_id: Optional[str] = None,
    headline_options: Optional[List[Dict[str, Any]]] = None,
    selected_headline_idx: Optional[int] = None,
    post_msg_id: Optional[int] = None,
    category: Optional[str] = None
) -> str:
    """
    Сохраняет сгенерированный пост как черновик для привязки к inline-кнопкам.
    Автоматически извлекает варианты заголовков, синхронизирует активный заголовок
    и определяет категорию публикации (cat1..cat6) для второго канала (@noticia666).
    Очищает старые черновики (>48ч). Возвращает draft_id.
    """
    if not draft_id:
        draft_id = uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)

    raw_extracted_cat = extract_category_from_text(text)
    synced_text, parsed_options, detected_idx = sync_initial_headline(text, title_hint=title)
    text = synced_text
    if headline_options is None:
        headline_options = parsed_options

    with QUEUE_LOCK:
        data = load_queue_data()
        drafts = data.setdefault("drafts", {})
        existing = drafts.get(draft_id, {})
        sel_idx = (
            selected_headline_idx
            if selected_headline_idx is not None
            else existing.get("selected_headline_idx", detected_idx)
        )
        p_mid = post_msg_id if post_msg_id is not None else existing.get("post_msg_id")
        cat_code = (
            category
            or existing.get("category")
            or raw_extracted_cat
            or detect_post_category(text, title)
        )

        drafts[draft_id] = {
            "id": draft_id,
            "text": strip_category_footer(text),
            "title": (title or existing.get("title", ""))[:100],
            "chat_id": str(chat_id or existing.get("chat_id", "")),
            "created_at": existing.get("created_at", now.isoformat()),
            "headline_options": headline_options if headline_options else existing.get("headline_options", []),
            "selected_headline_idx": sel_idx,
            "category": cat_code,
            "post_msg_id": p_mid
        }
        # Чистим черновики старше 48 часов
        cleaned_drafts = {}
        for k, v in drafts.items():
            try:
                c_dt = datetime.fromisoformat(v.get("created_at", now.isoformat()))
                if (now - c_dt).total_seconds() < 172800:
                    cleaned_drafts[k] = v
            except Exception:
                pass
        data["drafts"] = cleaned_drafts
        save_queue_data(data)
    log.info("📝 Сохранён черновик поста [%s] (заголовков: %d, активный: %d, категория: %s, чат: %s, post_msg_id: %s)",
             draft_id, len(headline_options or []), sel_idx, cat_code, chat_id, p_mid)
    return draft_id


def clean_rag_citations(text: str) -> str:
    """
    Удаляет технические сноски и метки цитирования NotebookLM:
    [1], [1, 2], [10-12], [4, 13-15], [1–4], [source 1], [[suspicious link removed]] и т.п.,
    а также убирает лишние пробелы перед знаками препинания (" ." -> ".").
    """
    if not text:
        return ""
    cleaned = re.sub(r'\[\s*\d+(?:\s*[-–—,]\s*\d+)*\s*\]', '', text)
    cleaned = re.sub(r'\[source\s*\d+[^\]]*\]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\[+suspicious link removed\]+', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'[ \t]+([.,;:!?])', r'\1', cleaned)
    cleaned = re.sub(r'(?<=\S)[ \t]{2,}', ' ', cleaned)
    return cleaned.strip()


def clean_asterisks_formatting(text: str) -> str:
    """
    Удаляет одиночные звёздочки (*), которые ломают вёрстку в Telegram,
    но строго сохраняет двойные звёздочки **жирный текст**.
    1. Заменяет горизонтальные разделители *** на ---
    2. Убирает тройные звёздочки: ***жирный*** -> **жирный**
    3. Списки с одиночной звёздочкой в начале строк: '* пункт' -> '• пункт'
    4. Одиночные звёздочки курсива: *курсив* -> курсив (без звёздочек)
    5. Любые одиночные блуждающие звёздочки удаляет.
    """
    if not text:
        return ""
    # Разделители *** на отдельной строке
    text = re.sub(r'^(?:[ \t]*)\*{3,}(?:[ \t]*)$', '---', text, flags=re.MULTILINE)
    # ***жирный*** -> **жирный**
    text = re.sub(r'\*\*\*(.+?)\*\*\*', r'**\1**', text, flags=re.DOTALL)
    # Списки вида '* пункт' -> '• пункт'
    text = re.sub(r'^(?:[ \t]*)\*(?:[ \t]+)', '• ', text, flags=re.MULTILINE)
    # Одиночные звёздочки курсива: *курсив* -> курсив (не трогая **)
    text = re.sub(r'(?<!\*)\*([^\*\n]+?)\*(?!\*)', r'\1', text)
    # Любые оставшиеся одиночные звёздочки
    text = re.sub(r'(?<!\*)\*(?!\*)', '', text)
    # Убираем множественные пробелы
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text.strip()


def markdown_to_telegram_html(text: str) -> str:
    """
    Конвертирует стандартный Markdown от LLM в валидный HTML для Telegram Bot API:
    - **жирный** или __жирный__ -> <b>жирный</b>
    - [текст](url)              -> <a href="url">текст</a>
    - `код`                     -> <code>код</code>
    - ```код```                 -> <pre>код</pre>
    - # Заголовок               -> <b>Заголовок</b>
    - ~~зачеркнутый~~           -> <s>зачеркнутый</s>
    Одиночные звездочки (*) удаляются, чтобы не ломать верстку в Telegram.
    Экранирует сырые спецсимволы &, <, > перед разметкой.
    """
    if not text:
        return ""

    # 0. Очищаем одиночные звездочки (сохраняя **)
    text = clean_asterisks_formatting(text)

    # 1. Экранируем спецсимволы HTML
    res = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # 2. Блоки кода: ```code``` и `inline`
    res = re.sub(r'```(?:[a-zA-Z0-9_-]+)?\n?(.*?)```', r'<pre>\1</pre>', res, flags=re.DOTALL)
    res = re.sub(r'`([^`\n]+)`', r'<code>\1</code>', res)

    # 3. Ссылки: [текст](url) -> <a href="url">текст</a>
    res = re.sub(r'\[([^\]]+)\]\((https?://[^\)\s]+)\)', r'<a href="\2">\1</a>', res)

    # 4. Заголовки markdown: # Заголовок -> <b>Заголовок</b>
    res = re.sub(r'^(?:#{1,6})\s+(.+)$', r'<b>\1</b>', res, flags=re.MULTILINE)

    # 5. Жирный шрифт: **текст** и __текст__
    res = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', res, flags=re.DOTALL)
    res = re.sub(r'__(.+?)__', r'<b>\1</b>', res, flags=re.DOTALL)

    # 6. Зачёркнутый: ~~текст~~
    res = re.sub(r'~~(.+?)~~', r'<s>\1</s>', res, flags=re.DOTALL)

    # 7. Курсив через нижнее подчёркивание: _текст_
    res = re.sub(r'(?<![a-zA-Z0-9_])_([^_]+?)_(?![a-zA-Z0-9_])', r'<i>\1</i>', res)

    return res


def strip_headline_options_block(text: str) -> str:
    """Удаляет служебный черновой блок вариантов заголовка перед публикацией в канал."""
    if not text:
        return ""
    pattern = r'^(?:\s*💡?\s*[*_#\s]*ВАРИАНТЫ\s+ЗАГОЛОВК[^\n]*\n(?:[^\n]*\n)*?\s*[-_*=]{3,}\s*\n+)'
    cleaned = re.sub(pattern, '', text, flags=re.IGNORECASE)
    if cleaned != text:
        return cleaned.strip()
    pattern2 = r'^(?:\s*💡?\s*[*_#\s]*ВАРИАНТЫ\s+ЗАГОЛОВК[^\n]*\n(?:[^\n]*\n){2,6}\s*\n+)'
    cleaned2 = re.sub(pattern2, '', text, flags=re.IGNORECASE)
    return cleaned2.strip() if cleaned2.strip() else text


def _send_post_to_single_channel(channel_id: str, text: str) -> bool:
    """Отправляет отформатированный пост в один указанный Telegram-канал."""
    log.info("📢 Публикую пост в канал %s (длина: %d симв.)", channel_id, len(text))
    html_text = markdown_to_telegram_html(text)

    if len(html_text) > 3800:
        send_long_message(channel_id, html_text, parse_mode="HTML", disable_web_page_preview=False)
        return True

    resp = tg_api("sendMessage",
                  chat_id=channel_id,
                  text=html_text,
                  parse_mode="HTML",
                  disable_web_page_preview=False)
    if resp and resp.get("ok"):
        return True

    # Fallback: если Telegram API отклонил HTML, пробуем отправить со снятыми звёздочками
    log.warning("⚠️ Не удалось отправить пост в %s с parse_mode='HTML', пробую fallback без звездочек...", channel_id)
    clean_text = text.replace("**", "").replace("__", "")
    resp = tg_api("sendMessage",
                  chat_id=channel_id,
                  text=clean_text,
                  parse_mode=None,
                  disable_web_page_preview=False)
    return bool(resp and resp.get("ok"))


def publish_post_to_channel(text: str, category: Optional[str] = None, is_breaking: bool = False) -> bool:
    """
    Публикует пост в оба целевых Telegram-канала:
    1) TELEGRAM_CHANNEL (@iberia_news) — ЧИСТЫЙ пост (без breaking, без cat1..cat6, без #dai).
    2) TELEGRAM_SECONDARY_CHANNEL (@noticia666) — пост со служебным подвалом:
       '#dai cat1..cat6' (обычный) или '#dai breaking cat1..cat6' (срочный).
    """
    raw_body = strip_headline_options_block(text)
    cat_code = category or extract_category_from_text(raw_body) or detect_post_category(raw_body)
    clean_primary_text = strip_category_footer(raw_body)

    # 1. В @iberia_news публикуем чистый текст (без breaking, cat1..cat6 и #dai)
    primary_ok = _send_post_to_single_channel(TELEGRAM_CHANNEL, clean_primary_text)

    # 2. В @noticia666 публикуем текст с обязательным #dai и catX (плюс breaking, если срочно)
    if TELEGRAM_SECONDARY_CHANNEL and TELEGRAM_SECONDARY_CHANNEL != TELEGRAM_CHANNEL:
        secondary_text = apply_category_footer(clean_primary_text, cat_code, is_breaking=is_breaking)
        secondary_ok = _send_post_to_single_channel(TELEGRAM_SECONDARY_CHANNEL, secondary_text)
        if not secondary_ok:
            log.warning("⚠️ Не удалось опубликовать пост во второй канал %s", TELEGRAM_SECONDARY_CHANNEL)

    return primary_ok


def format_channels_html() -> str:
    """Возвращает HTML-ссылки на оба канала для информационных сообщений."""
    parts = []
    for ch in (TELEGRAM_CHANNEL, TELEGRAM_SECONDARY_CHANNEL):
        if not ch:
            continue
        if ch.startswith("@"):
            parts.append(f"<a href='https://t.me/{ch.lstrip('@')}'>{ch}</a>")
        else:
            parts.append(f"<b>{ch}</b>")
    return " и ".join(dict.fromkeys(parts))


def calculate_next_publish_time(data: Dict[str, Any]) -> datetime:
    """
    Вычисляет дату и время следующего выхода в очереди с интервалом PUBLISH_INTERVAL_HOURS.
    - Если в очереди есть ожидающие посты: берётся время последнего + 8ч.
    - Если очередь пуста: max(now, last_published_at + 8ч).
    """
    now = datetime.now(timezone.utc)
    interval = timedelta(hours=PUBLISH_INTERVAL_HOURS)

    pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
    if pending:
        last_dt = None
        for p in pending:
            try:
                p_dt = datetime.fromisoformat(p["publish_at"])
                if last_dt is None or p_dt > last_dt:
                    last_dt = p_dt
            except Exception:
                pass
        if last_dt:
            next_time = last_dt + interval
            if next_time < now:
                next_time = now
            return next_time

    # Очередь пуста: смотрим на время последней успешной публикации
    last_pub_str = data.get("last_published_at")
    if last_pub_str:
        try:
            last_pub = datetime.fromisoformat(last_pub_str)
            earliest_next = last_pub + interval
            if earliest_next > now:
                return earliest_next
        except Exception:
            pass

    return now


def publish_draft_urgent(draft_id: str) -> tuple[bool, str]:
    """
    Публикует черновик поста немедленно в оба канала:
    - в @iberia_news — чистый текст;
    - в @noticia666 — с добавлением '#dai breaking catX'.
    Сдвигает очередь регулярных постов, чтобы выдержать интервал 8ч.
    """
    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id)
        if not draft:
            return False, "Черновик поста не найден или срок его действия истёк."
        text = strip_category_footer(draft["text"])
        title = draft.get("title", "")
        chat_id = draft.get("chat_id", "")
        cat_code = draft.get("category") or extract_category_from_text(draft["text"]) or detect_post_category(text, title)

    success = publish_post_to_channel(text, category=cat_code, is_breaking=True)
    if not success:
        return False, f"Ошибка отправки в канал {TELEGRAM_CHANNEL}. Убедитесь, что бот является администратором канала с правом публикации."

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    with QUEUE_LOCK:
        data = load_queue_data()
        data["last_published_at"] = now_iso
        data.setdefault("queue", []).append({
            "id": draft_id,
            "title": title,
            "text": text,
            "category": cat_code,
            "status": "published",
            "is_urgent": True,
            "created_at": draft.get("created_at", now_iso),
            "publish_at": now_iso,
            "published_at": now_iso,
            "chat_id": chat_id
        })

        # Сдвигаем ожидающие посты в очереди, чтобы следующий вышел не ранее now + 8ч
        interval = timedelta(hours=PUBLISH_INTERVAL_HOURS)
        min_next = now + interval
        pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
        pending.sort(key=lambda x: x.get("publish_at", ""))

        curr_slot = min_next
        for p in pending:
            try:
                p_dt = datetime.fromisoformat(p["publish_at"])
                if p_dt < curr_slot:
                    p["publish_at"] = curr_slot.isoformat()
                curr_slot = datetime.fromisoformat(p["publish_at"]) + interval
            except Exception:
                pass

        data.get("drafts", {}).pop(draft_id, None)
        save_queue_data(data)

    log.info("⚡ Срочный пост [%s] (#dai breaking %s) опубликован в каналы %s, %s",
             draft_id, cat_code, TELEGRAM_CHANNEL, TELEGRAM_SECONDARY_CHANNEL)
    return True, f"Пост опубликован в {TELEGRAM_CHANNEL} и {TELEGRAM_SECONDARY_CHANNEL}"


def add_draft_to_queue(draft_id: str) -> tuple[bool, str, Optional[datetime]]:
    """Ставит черновик поста в очередь регулярных публикаций (каждые 8 часов)."""
    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id)
        if not draft:
            return False, "Черновик поста не найден или срок его действия истёк.", None

        next_time = calculate_next_publish_time(data)
        now = datetime.now(timezone.utc)
        raw_text = draft["text"]
        title = draft.get("title", "")
        cat_code = draft.get("category") or extract_category_from_text(raw_text) or detect_post_category(raw_text, title)
        final_text = strip_category_footer(raw_text)

        data.setdefault("queue", []).append({
            "id": draft_id,
            "title": title,
            "text": final_text,
            "category": cat_code,
            "status": "pending",
            "is_urgent": False,
            "created_at": draft.get("created_at", now.isoformat()),
            "publish_at": next_time.isoformat(),
            "published_at": None,
            "chat_id": draft.get("chat_id", "")
        })
        data.get("drafts", {}).pop(draft_id, None)
        save_queue_data(data)

    log.info("🕒 Пост [%s] (%s) поставлен в очередь на %s", draft_id, cat_code, next_time.isoformat())
    return True, "Пост добавлен в очередь", next_time


def publish_queued_now(post_id: str) -> tuple[bool, str]:
    """Публикует пост из очереди немедленно (пользователь нажал 'Опубликовать сейчас' в /queue)."""
    with QUEUE_LOCK:
        data = load_queue_data()
        target = None
        for p in data.get("queue", []):
            if p.get("id") == post_id and p.get("status") == "pending":
                target = p
                break
        if not target:
            return False, "Пост не найден в очереди или уже был опубликован."
        cat_code = target.get("category") or extract_category_from_text(target["text"]) or detect_post_category(target["text"], target.get("title", ""))

    success = publish_post_to_channel(target["text"], category=cat_code, is_breaking=True)
    if not success:
        return False, f"Ошибка отправки в канал {TELEGRAM_CHANNEL}"

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    with QUEUE_LOCK:
        data = load_queue_data()
        for p in data.get("queue", []):
            if p.get("id") == post_id and p.get("status") == "pending":
                p["status"] = "published"
                p["published_at"] = now_iso
                p["is_urgent"] = True
                p["category"] = cat_code
                p["text"] = strip_category_footer(p["text"])
                break
        data["last_published_at"] = now_iso

        # Сдвигаем оставшиеся посты на +8ч
        interval = timedelta(hours=PUBLISH_INTERVAL_HOURS)
        min_next = now + interval
        pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
        pending.sort(key=lambda x: x.get("publish_at", ""))
        curr_slot = min_next
        for p in pending:
            try:
                p_dt = datetime.fromisoformat(p["publish_at"])
                if p_dt < curr_slot:
                    p["publish_at"] = curr_slot.isoformat()
                curr_slot = datetime.fromisoformat(p["publish_at"]) + interval
            except Exception:
                pass

        save_queue_data(data)

    log.info("⚡ Пост из очереди [%s] (#dai breaking %s) досрочно опубликован в каналы %s, %s",
             post_id, cat_code, TELEGRAM_CHANNEL, TELEGRAM_SECONDARY_CHANNEL)
    return True, f"Пост опубликован в {TELEGRAM_CHANNEL} и {TELEGRAM_SECONDARY_CHANNEL}"


def remove_from_queue(post_id: str) -> bool:
    """Удаляет (отменяет) пост из очереди."""
    with QUEUE_LOCK:
        data = load_queue_data()
        found = False
        for p in data.get("queue", []):
            if p.get("id") == post_id and p.get("status") == "pending":
                p["status"] = "cancelled"
                found = True
                break
        if found:
            save_queue_data(data)
            log.info("🗑 Пост [%s] удалён из очереди", post_id)
            return True
        return False


def check_and_publish_next_due():
    """Проверяет, подошло ли время для публикации очередного поста из очереди."""
    now = datetime.now(timezone.utc)
    due_post = None

    with QUEUE_LOCK:
        data = load_queue_data()
        pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
        pending.sort(key=lambda x: x.get("publish_at", ""))
        for p in pending:
            try:
                p_dt = datetime.fromisoformat(p["publish_at"])
                if p_dt <= now:
                    due_post = p
                    break
            except Exception as e:
                log.warning("⚠️ Некорректный publish_at в посте %s: %s", p.get("id"), e)

    if not due_post:
        return

    post_id = due_post["id"]
    post_text = due_post["text"]
    title = due_post.get("title", "")
    author_chat = due_post.get("chat_id")
    cat_code = due_post.get("category") or extract_category_from_text(post_text) or detect_post_category(post_text, title)

    log.info("⏰ Наступило время публикации поста [%s] (#dai %s) в каналы %s, %s",
             post_id, cat_code, TELEGRAM_CHANNEL, TELEGRAM_SECONDARY_CHANNEL)
    success = publish_post_to_channel(post_text, category=cat_code, is_breaking=False)

    now_iso = datetime.now(timezone.utc).isoformat()
    with QUEUE_LOCK:
        data = load_queue_data()
        for p in data.get("queue", []):
            if p.get("id") == post_id and p.get("status") == "pending":
                if success:
                    p["status"] = "published"
                    p["published_at"] = now_iso
                    p["category"] = cat_code
                    data["last_published_at"] = now_iso
                    log.info("✅ Пост [%s] успешно опубликован по расписанию!", post_id)
                else:
                    retry_time = datetime.now(timezone.utc) + timedelta(minutes=5)
                    p["publish_at"] = retry_time.isoformat()
                    log.error("❌ Сбой публикации [%s] в канал, повтор в %s", post_id, retry_time.isoformat())
                break
        save_queue_data(data)

    if success and author_chat:
        try:
            preview = title or (post_text[:60] + "...")
            channels_html = format_channels_html()
            send_message(
                author_chat,
                f"📢 <b>Пост из очереди опубликован в каналах {channels_html}!</b>\n\n"
                f"📰 <i>«{escape_html(preview)}»</i>",
                disable_web_page_preview=True
            )
        except Exception as e:
            log.warning("⚠️ Не удалось отправить уведомление автору поста: %s", e)


def queue_worker_thread_func():
    """Фоновый поток планировщика публикаций."""
    log.info("🕒 Планировщик публикаций запущен (канал: %s, интервал: %d ч)",
             TELEGRAM_CHANNEL, PUBLISH_INTERVAL_HOURS)
    while True:
        try:
            check_and_publish_next_due()
        except Exception as e:
            log.error("❌ Ошибка в планировщике публикаций: %s", e, exc_info=True)
        time.sleep(30)


def build_publication_menu(draft_id: str) -> tuple[str, dict]:
    """
    Формирует текст и inline-клавиатуру для меню публикации с интерактивными кнопками выбора заголовка и категории.
    """
    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id, {})
        next_time = calculate_next_publish_time(data)

    now = datetime.now(timezone.utc)
    diff_sec = int((next_time - now).total_seconds())
    if diff_sec <= 60:
        time_hint = "прямо сейчас (очередь свободна)"
    else:
        hours = diff_sec // 3600
        mins = (diff_sec % 3600) // 60
        time_hint = f"через {hours}ч {mins}м"

    options = draft.get("headline_options", [])
    selected_idx = draft.get("selected_headline_idx", 1)
    raw_text = draft.get("text", "")
    selected_cat = (
        draft.get("category")
        or extract_category_from_text(raw_text)
        or detect_post_category(raw_text, draft.get("title", ""))
    )

    keyboard = []

    # 1. Кнопки выбора заголовка (по 1 кнопке в ряд с реальным текстом каждого сгенерированного заголовка)
    if options:
        for opt in options:
            idx = opt["idx"]
            short_cat = opt.get("short_category", f"Вариант {idx}")
            title_t = opt.get("title", "")
            short_t = title_t if len(title_t) <= 38 else title_t[:35] + "…"
            is_active = (idx == selected_idx)
            prefix = f"✅ {idx}️⃣ [{short_cat}]" if is_active else f"{idx}️⃣ [{short_cat}]"
            btn_text = f"{prefix}: {short_t}"
            keyboard.append([{"text": btn_text, "callback_data": f"hl:{draft_id}:{idx}"}])

    # 2. Кнопки выбора категории (cat1..cat6, по 2 кнопки в ряд — 3 ряда)
    cat_codes = ["cat1", "cat2", "cat3", "cat4", "cat5", "cat6"]
    for i in range(0, len(cat_codes), 2):
        row = []
        for c_code in cat_codes[i:i + 2]:
            c_info = POST_CATEGORIES[c_code]
            is_cat_active = (c_code == selected_cat)
            btn_base = c_info.get("btn", c_code)
            c_label = f"✅ {btn_base}" if is_cat_active else btn_base
            row.append({"text": c_label, "callback_data": f"cat:{draft_id}:{c_code}"})
        keyboard.append(row)

    # 3. Кнопки публикации и действий
    keyboard.append([
        {"text": f"📅 В очередь (#dai {selected_cat})", "callback_data": f"pub_q:{draft_id}"},
        {"text": f"⚡ Срочно (#dai breaking {selected_cat})", "callback_data": f"pub_now:{draft_id}"}
    ])
    keyboard.append([
        {"text": "⭐ В избранное (на потом)", "callback_data": f"pub_fav:{draft_id}"},
        {"text": "❌ Не публиковать", "callback_data": f"pub_c:{draft_id}"}
    ])

    channels_html = format_channels_html()
    cat_meta = POST_CATEGORIES.get(selected_cat, POST_CATEGORIES["cat1"])
    cat_title = cat_meta.get("title") or cat_meta.get("label") or selected_cat

    # Извлекаем чистый текст поста (без блока вариантов заголовков и без catX)
    clean_primary_post = strip_category_footer(strip_headline_options_block(raw_text)).strip()
    clean_post_html = markdown_to_telegram_html(clean_primary_post)

    curr_opt = next((o for o in options if o.get("idx") == selected_idx), None)
    cat_lbl = f" [{curr_opt.get('category')}]" if (curr_opt and curr_opt.get("category")) else ""
    active_headline = (curr_opt.get("title") if curr_opt else "") or extract_current_body_headline(raw_text)

    lines = []
    if options:
        lines.append(f"📌 <b>Настоящий (активный) заголовок поста ({selected_idx}️⃣{cat_lbl}):</b>")
        lines.append(f"<b>«{escape_html(active_headline)}»</b>\n")
        lines.append("🎯 <b>Все сгенерированные варианты заголовков:</b>")
        for opt in options:
            idx = opt["idx"]
            cat = opt.get("category", "")
            title_text = opt.get("title", "")
            cat_badge = f" [{cat}]" if cat else ""
            if idx == selected_idx:
                lines.append(f"👉 <b>{idx}️⃣{cat_badge}: {escape_html(title_text)}</b> <i>(выбран)</i>")
            else:
                lines.append(f"{idx}️⃣<b>{cat_badge}:</b> {escape_html(title_text)}")
        lines.append("")
        lines.append(f"🏷 <b>Рубрика для {TELEGRAM_SECONDARY_CHANNEL}:</b> <b>{escape_html(cat_title)}</b> (<code>#dai {selected_cat}</code>)")
        lines.append(f"📝 <b>Итоговый текст поста для {channels_html}:</b>")
    else:
        if active_headline:
            lines.append(f"📌 <b>Заголовок поста:</b> <b>«{escape_html(active_headline)}»</b>\n")
        lines.append(f"🏷 <b>Рубрика для {TELEGRAM_SECONDARY_CHANNEL}:</b> <b>{escape_html(cat_title)}</b> (<code>#dai {selected_cat}</code>)")
        lines.append(f"📝 <b>Текст для публикации в каналах {channels_html}:</b>")
    lines.append("─────────────────────────")

    footer_text = (
        f"─────────────────────────\n"
        f"📢 <b>Опубликовать этот пост в каналах {channels_html}?</b>\n"
        f"• <b>{TELEGRAM_CHANNEL}:</b> выйдет чистый пост (без <code>cat1..cat6</code> и <code>breaking</code>)\n"
        f"• <b>{TELEGRAM_SECONDARY_CHANNEL}:</b> внизу добавится <code>#dai {selected_cat}</code> (при ⚡ Срочно: <code>#dai breaking {selected_cat}</code>)\n"
        f"• 📅 <b>В очередь:</b> выйдет <i>{time_hint}</i> (шаг {PUBLISH_INTERVAL_HOURS}ч)\n"
        f"• ⚡ <b>Срочно:</b> мгновенная публикация прямо сейчас\n"
        f"• ⭐ <b>В избранное:</b> отложить на потом | ❌ <b>Не публиковать</b>"
    )
    if options:
        footer_text += "\n<i>(кнопками ниже можно сменить заголовок 1️⃣–4️⃣ или категорию cat1–cat6 перед отправкой)</i>"
    else:
        footer_text += "\n<i>(кнопками ниже можно сменить категорию cat1–cat6 перед отправкой)</i>"

    available_chars = 4050 - sum(len(l) for l in lines) - len(footer_text) - 30
    if len(clean_post_html) > available_chars:
        cut_text = clean_primary_post[:max(200, available_chars - 70)]
        p = max(cut_text.rfind('.\n'), cut_text.rfind('. '), cut_text.rfind('\n\n'))
        if p > int(available_chars * 0.5):
            cut_text = cut_text[:p + 1]
        post_preview = (
            markdown_to_telegram_html(cut_text)
            + "\n\n<i>... [полный текст поста в сообщении выше ⬆️]</i>"
        )
    else:
        post_preview = clean_post_html

    lines.append(post_preview)
    lines.append(footer_text)

    text_msg = "\n".join(lines)
    return text_msg, {"inline_keyboard": keyboard}


def offer_channel_publication(chat_id: str, draft_id: str):
    """Отправляет пользователю интерактивное меню публикации сгенерированного поста."""
    text_msg, reply_markup = build_publication_menu(draft_id)
    send_message(
        chat_id,
        text_msg,
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )


def handle_headline_selection(callback_query: dict, draft_id: str, opt_idx: int):
    """Обрабатывает выбор заголовка по inline-кнопке [1️⃣], [2️⃣], [3️⃣], [4️⃣]."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    menu_message_id = callback_query["message"]["message_id"]

    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id)
        if not draft:
            answer_callback_query(callback_query["id"], "❌ Черновик устарел или не найден.", show_alert=True)
            return

        options = draft.get("headline_options", [])
        chosen_opt = next((o for o in options if o.get("idx") == opt_idx), None)
        if not chosen_opt:
            answer_callback_query(callback_query["id"], f"❌ Вариант {opt_idx} не найден.", show_alert=True)
            return

        chosen_title = chosen_opt.get("title", "")
        cat_code = draft.get("category") or extract_category_from_text(draft["text"]) or detect_post_category(draft["text"], chosen_title)
        new_text = strip_category_footer(apply_chosen_headline(strip_category_footer(draft["text"]), chosen_title))
        draft["text"] = new_text
        draft["category"] = cat_code
        draft["selected_headline_idx"] = opt_idx
        draft["title"] = chosen_title[:100]
        post_msg_id = draft.get("post_msg_id")
        save_queue_data(data)

    # 1. Обновляем отдельное сообщение с текстом черновика в чате (если известно post_msg_id)
    if post_msg_id:
        try:
            clean_updated = strip_category_footer(strip_headline_options_block(new_text))
            html_updated = markdown_to_telegram_html(clean_updated)
            cut_html = html_updated if len(html_updated) <= 4090 else html_updated[:4085] + "…"
            edit_message_text(chat_id, post_msg_id, cut_html, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            log.warning("⚠️ Не удалось обновить текст черновика в сообщении %s: %s", post_msg_id, e)

    # 2. Обновляем интерактивное меню
    text_msg, reply_markup = build_publication_menu(draft_id)
    try:
        edit_message_text(chat_id, menu_message_id, text_msg, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        log.warning("⚠️ Не удалось обновить меню публикации: %s", e)

    answer_callback_query(callback_query["id"], f"✅ Выбран заголовок {opt_idx}!")


def handle_category_selection(callback_query: dict, draft_id: str, cat_code: str):
    """Обрабатывает выбор категории поста по inline-кнопкам cat1..cat6 (локально, без облака)."""
    if cat_code not in POST_CATEGORIES:
        answer_callback_query(callback_query["id"], f"❌ Неизвестная категория: {cat_code}", show_alert=True)
        return

    chat_id = str(callback_query["message"]["chat"]["id"])
    menu_message_id = callback_query["message"]["message_id"]

    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id)
        if not draft:
            answer_callback_query(callback_query["id"], "❌ Черновик устарел или не найден.", show_alert=True)
            return

        draft["category"] = cat_code
        draft["text"] = strip_category_footer(draft["text"])
        save_queue_data(data)

    # Обновляем интерактивное меню
    text_msg, reply_markup = build_publication_menu(draft_id)
    try:
        edit_message_text(chat_id, menu_message_id, text_msg, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        log.warning("⚠️ Не удалось обновить меню публикации: %s", e)

    cat_btn_name = POST_CATEGORIES[cat_code]["btn"]
    answer_callback_query(callback_query["id"], f"✅ Категория для {TELEGRAM_SECONDARY_CHANNEL}: {cat_btn_name}")


def handle_publish_queue(callback_query: dict, draft_id: str):
    """Обработка кнопки '📅 В очередь (раз в 8ч)'."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    success, msg, next_time = add_draft_to_queue(draft_id)
    if not success or not next_time:
        answer_callback_query(callback_query["id"], f"❌ {msg}", show_alert=True)
        edit_message_text(chat_id, message_id, f"❌ {msg}")
        return

    answer_callback_query(callback_query["id"], "📅 Пост поставлен в очередь")

    time_str = next_time.strftime("%d.%m в %H:%M UTC")
    now = datetime.now(timezone.utc)
    diff_sec = max(0, int((next_time - now).total_seconds()))
    hours = diff_sec // 3600
    mins = (diff_sec % 3600) // 60
    rel_str = f"через {hours}ч {mins}м" if diff_sec > 60 else "в ближайшую минуту"

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "⚡ Опубликовать сейчас", "callback_data": f"pub_q_now:{draft_id}"},
                {"text": "❌ Удалить из очереди", "callback_data": f"pub_q_del:{draft_id}"}
            ]
        ]
    }
    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    edit_message_text(
        chat_id,
        message_id,
        f"✅ <b>Пост поставлен в очередь публикаций!</b>\n\n"
        f"📢 Канал: {link_html}\n"
        f"🕒 Запланировано на: <b>{time_str}</b> (<i>{rel_str}</i>)\n"
        f"⏱ Интервал очереди: <b>каждые {PUBLISH_INTERVAL_HOURS} часов</b>\n\n"
        f"<i>Управлять очередью можно командой <code>/queue</code>.</i>",
        reply_markup=keyboard,
        disable_web_page_preview=True
    )


def handle_publish_urgent(callback_query: dict, draft_id: str):
    """Обработка кнопки '⚡ Срочно (сейчас)'."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    answer_callback_query(callback_query["id"], "⚡ Публикую срочный пост...")
    edit_message_text(chat_id, message_id, f"⏳ Публикую пост в {TELEGRAM_CHANNEL}...")

    success, msg = publish_draft_urgent(draft_id)
    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    if success:
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
        edit_message_text(
            chat_id,
            message_id,
            f"⚡ <b>Пост успешно опубликован в канале!</b>\n\n"
            f"📢 Канал: {link_html}\n"
            f"🕒 Время публикации: <b>{now_str}</b>\n"
            f"ℹ️ Следующая регулярная публикация в очереди сдвинута на {PUBLISH_INTERVAL_HOURS}ч.",
            disable_web_page_preview=True
        )
    else:
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔄 Попробовать снова", "callback_data": f"pub_now:{draft_id}"}],
                [{"text": "❌ Отмена", "callback_data": f"pub_c:{draft_id}"}]
            ]
        }
        edit_message_text(
            chat_id,
            message_id,
            f"❌ <b>Не удалось опубликовать пост в канале {link_html}:</b>\n\n"
            f"<code>{escape_html(msg)}</code>\n\n"
            f"<i>Убедитесь, что бот добавлен администратором канала с правом публикации.</i>",
            reply_markup=keyboard,
            disable_web_page_preview=True
        )


def handle_publish_cancel(callback_query: dict, draft_id: str):
    """Обработка кнопки '❌ Не публиковать'."""
    answer_callback_query(callback_query["id"], "❌ Публикация отменена")
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    with QUEUE_LOCK:
        data = load_queue_data()
        data.get("drafts", {}).pop(draft_id, None)
        save_queue_data(data)

    edit_message_text(chat_id, message_id, "ℹ️ Публикация отменена. Пост остался только в этом чате.")


def handle_queue_publish_now(callback_query: dict, post_id: str):
    """Публикует пост из очереди прямо сейчас."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    answer_callback_query(callback_query["id"], "⚡ Публикую пост...")
    success, msg = publish_queued_now(post_id)
    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    if success:
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
        edit_message_text(
            chat_id,
            message_id,
            f"⚡ <b>Пост опубликован в канале!</b>\n\n"
            f"📢 Канал: {link_html}\n"
            f"🕒 Время публикации: <b>{now_str}</b>",
            disable_web_page_preview=True
        )
    else:
        answer_callback_query(callback_query["id"], f"❌ {msg}", show_alert=True)


def handle_queue_delete(callback_query: dict, post_id: str):
    """Удаляет пост из очереди."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    if remove_from_queue(post_id):
        answer_callback_query(callback_query["id"], "🗑 Пост удалён из очереди")
        edit_message_text(chat_id, message_id, "🗑 <b>Пост удалён из очереди публикаций.</b>")
    else:
        answer_callback_query(callback_query["id"], "❌ Пост не найден или уже опубликован", show_alert=True)


def handle_queue_command(chat_id: str):
    """Команда /queue — показывает текущую очередь публикаций."""
    with QUEUE_LOCK:
        data = load_queue_data()

    pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
    pending.sort(key=lambda x: x.get("publish_at", ""))

    last_pub_str = data.get("last_published_at")
    last_info = ""
    if last_pub_str:
        try:
            lp_dt = datetime.fromisoformat(last_pub_str)
            last_info = f"🕒 Последний пост на канале: <b>{lp_dt.strftime('%d.%m в %H:%M UTC')}</b>\n"
        except Exception:
            pass

    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    header = (
        f"📋 <b>Очередь публикаций канала {link_html}</b>\n\n"
        f"{last_info}"
        f"⏱ Дефолтный интервал: <b>каждые {PUBLISH_INTERVAL_HOURS} часов</b>\n"
        f"📦 Постов в очереди: <b>{len(pending)}</b>\n\n"
    )

    if not pending:
        send_message(
            chat_id,
            header + "📭 <i>Очередь публикаций пуста.</i>\n\n"
                     "Новые посты можно сгенерировать через кнопку «📝 Сгенерировать ТГ-пост» в /top "
                     "или через команду <code>/post [тема]</code>.",
            disable_web_page_preview=True
        )
        return

    lines = []
    keyboard_buttons = []
    now = datetime.now(timezone.utc)

    for idx, p in enumerate(pending[:8], start=1):
        p_id = p.get("id", "")
        raw_text = p.get("text", "")
        first_line = raw_text.split("\n")[0] if raw_text else ""
        title = p.get("title") or first_line[:40] or f"Пост #{idx}"
        try:
            p_dt = datetime.fromisoformat(p["publish_at"])
            diff_sec = int((p_dt - now).total_seconds())
            if diff_sec <= 0:
                time_badge = "🔥 <b>сейчас</b>"
            else:
                hours = diff_sec // 3600
                mins = (diff_sec % 3600) // 60
                time_badge = f"🕒 {p_dt.strftime('%d.%m %H:%M UTC')} (<i>через {hours}ч {mins}м</i>)"
        except Exception:
            time_badge = p.get("publish_at", "—")

        lines.append(f"<b>{idx}.</b> {time_badge}\n    📰 <i>«{escape_html(title)}»</i>")
        keyboard_buttons.append([
            {"text": f"⚡ Опубликовать #{idx}", "callback_data": f"pub_q_now:{p_id}"},
            {"text": f"❌ Удалить #{idx}", "callback_data": f"pub_q_del:{p_id}"}
        ])

    send_message(
        chat_id,
        header + "\n\n".join(lines),
        reply_markup={"inline_keyboard": keyboard_buttons} if keyboard_buttons else None,
        disable_web_page_preview=True
    )


# =================== ИЗБРАННЫЕ ПОСТЫ (FAV / НА ПОТОМ) ===================


def add_draft_to_favorites(draft_id: str) -> tuple[bool, str]:
    """Сохраняет черновик поста в Избранное (на потом)."""
    now = datetime.now(timezone.utc)
    with QUEUE_LOCK:
        data = load_queue_data()
        draft = data.get("drafts", {}).get(draft_id)
        if not draft:
            return False, "Черновик поста не найден или срок его действия истёк."

        raw_text = draft["text"]
        title = draft.get("title", "")
        cat_code = draft.get("category") or extract_category_from_text(raw_text) or detect_post_category(raw_text, title)
        final_text = strip_category_footer(raw_text)

        fav_item = {
            "id": draft_id,
            "title": title,
            "text": final_text,
            "category": cat_code,
            "created_at": draft.get("created_at", now.isoformat()),
            "saved_at": now.isoformat(),
            "chat_id": draft.get("chat_id", "")
        }
        if not any(f.get("id") == draft_id for f in data.setdefault("favorites", [])):
            data["favorites"].append(fav_item)
        data.get("drafts", {}).pop(draft_id, None)
        save_queue_data(data)

    log.info("⭐ Пост [%s] (%s) сохранён в Избранное", draft_id, cat_code)
    return True, "Пост сохранён в Избранное"


def move_favorite_to_queue(fav_id: str) -> tuple[bool, str, Optional[datetime]]:
    """Переносит пост из Избранного в очередь регулярных публикаций (раз в 8 часов)."""
    with QUEUE_LOCK:
        data = load_queue_data()
        fav = None
        for item in data.get("favorites", []):
            if item.get("id") == fav_id:
                fav = item
                break
        if not fav:
            return False, "Пост не найден в Избранном.", None

        next_time = calculate_next_publish_time(data)
        now = datetime.now(timezone.utc)
        raw_text = fav["text"]
        title = fav.get("title", "")
        cat_code = fav.get("category") or extract_category_from_text(raw_text) or detect_post_category(raw_text, title)
        final_text = strip_category_footer(raw_text)

        data.setdefault("queue", []).append({
            "id": fav["id"],
            "title": title,
            "text": final_text,
            "category": cat_code,
            "status": "pending",
            "is_urgent": False,
            "created_at": fav.get("created_at", now.isoformat()),
            "publish_at": next_time.isoformat(),
            "published_at": None,
            "chat_id": fav.get("chat_id", "")
        })
        data["favorites"] = [f for f in data.get("favorites", []) if f.get("id") != fav_id]
        save_queue_data(data)

    log.info("📅 Пост из Избранного [%s] (%s) отправлен в очередь на %s", fav_id, cat_code, next_time.isoformat())
    return True, "Пост перенесён из Избранного в очередь", next_time


def publish_favorite_now(fav_id: str) -> tuple[bool, str]:
    """Публикует пост из Избранного срочно прямо сейчас (в @noticia666 с '#dai breaking catX')."""
    with QUEUE_LOCK:
        data = load_queue_data()
        fav = None
        for item in data.get("favorites", []):
            if item.get("id") == fav_id:
                fav = item
                break
        if not fav:
            return False, "Пост не найден в Избранном."
        cat_code = fav.get("category") or extract_category_from_text(fav["text"]) or detect_post_category(fav["text"], fav.get("title", ""))
        clean_text = strip_category_footer(fav["text"])

    success = publish_post_to_channel(clean_text, category=cat_code, is_breaking=True)
    if not success:
        return False, f"Ошибка отправки в канал {TELEGRAM_CHANNEL}"

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    with QUEUE_LOCK:
        data = load_queue_data()
        data["last_published_at"] = now_iso
        data.setdefault("queue", []).append({
            "id": fav["id"],
            "title": fav.get("title", ""),
            "text": clean_text,
            "category": cat_code,
            "status": "published",
            "is_urgent": True,
            "created_at": fav.get("created_at", now_iso),
            "publish_at": now_iso,
            "published_at": now_iso,
            "chat_id": fav.get("chat_id", "")
        })
        data["favorites"] = [f for f in data.get("favorites", []) if f.get("id") != fav_id]

        # Сдвигаем ожидающие посты
        interval = timedelta(hours=PUBLISH_INTERVAL_HOURS)
        min_next = now + interval
        pending = [p for p in data.get("queue", []) if p.get("status") == "pending"]
        pending.sort(key=lambda x: x.get("publish_at", ""))
        curr_slot = min_next
        for p in pending:
            try:
                p_dt = datetime.fromisoformat(p["publish_at"])
                if p_dt < curr_slot:
                    p["publish_at"] = curr_slot.isoformat()
                curr_slot = datetime.fromisoformat(p["publish_at"]) + interval
            except Exception:
                pass

        save_queue_data(data)

    log.info("⚡ Пост из Избранного [%s] (breaking %s) опубликован в каналы %s, %s",
             fav_id, cat_code, TELEGRAM_CHANNEL, TELEGRAM_SECONDARY_CHANNEL)
    return True, f"Пост опубликован в {TELEGRAM_CHANNEL} и {TELEGRAM_SECONDARY_CHANNEL}"


def remove_from_favorites(fav_id: str) -> bool:
    """Удаляет пост из Избранного."""
    with QUEUE_LOCK:
        data = load_queue_data()
        initial_len = len(data.get("favorites", []))
        data["favorites"] = [f for f in data.get("favorites", []) if f.get("id") != fav_id]
        if len(data["favorites"]) < initial_len:
            save_queue_data(data)
            log.info("🗑 Пост [%s] удалён из Избранного", fav_id)
            return True
        return False


def handle_publish_fav(callback_query: dict, draft_id: str):
    """Обработка кнопки '⭐ В избранное (на потом)'."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    success, msg = add_draft_to_favorites(draft_id)
    if not success:
        answer_callback_query(callback_query["id"], f"❌ {msg}", show_alert=True)
        edit_message_text(chat_id, message_id, f"❌ {msg}")
        return

    answer_callback_query(callback_query["id"], "⭐ Пост сохранён в Избранное!")

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "📅 В очередь (раз в 8ч)", "callback_data": f"fav_q:{draft_id}"},
                {"text": "⚡ Срочно (сейчас)", "callback_data": f"fav_now:{draft_id}"}
            ],
            [
                {"text": "🗑 Удалить из избранного", "callback_data": f"fav_del:{draft_id}"}
            ]
        ]
    }
    edit_message_text(
        chat_id,
        message_id,
        "⭐ <b>Пост сохранён в Избранное (на потом)!</b>\n\n"
        "Он отложен без даты публикации. Вы можете:\n"
        "• Опубликовать его в любое время кнопками ниже\n"
        "• Или посмотреть список всех сохранённых постов командой <code>/fav</code>.",
        reply_markup=keyboard,
        disable_web_page_preview=True
    )


def handle_fav_to_queue(callback_query: dict, fav_id: str):
    """Переносит пост из Избранного в очередь публикаций."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    success, msg, next_time = move_favorite_to_queue(fav_id)
    if not success or not next_time:
        answer_callback_query(callback_query["id"], f"❌ {msg}", show_alert=True)
        return

    answer_callback_query(callback_query["id"], "📅 Пост поставлен в очередь")
    time_str = next_time.strftime("%d.%m в %H:%M UTC")
    now = datetime.now(timezone.utc)
    diff_sec = max(0, int((next_time - now).total_seconds()))
    hours = diff_sec // 3600
    mins = (diff_sec % 3600) // 60
    rel_str = f"через {hours}ч {mins}м" if diff_sec > 60 else "в ближайшую минуту"

    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "⚡ Опубликовать сейчас", "callback_data": f"pub_q_now:{fav_id}"},
                {"text": "❌ Удалить из очереди", "callback_data": f"pub_q_del:{fav_id}"}
            ]
        ]
    }
    edit_message_text(
        chat_id,
        message_id,
        f"✅ <b>Пост из Избранного поставлен в очередь!</b>\n\n"
        f"📢 Канал: {link_html}\n"
        f"🕒 Запланировано на: <b>{time_str}</b> (<i>{rel_str}</i>)\n"
        f"⏱ Интервал очереди: <b>каждые {PUBLISH_INTERVAL_HOURS} часов</b>",
        reply_markup=keyboard,
        disable_web_page_preview=True
    )


def handle_fav_publish_now(callback_query: dict, fav_id: str):
    """Публикует пост из Избранного немедленно."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    answer_callback_query(callback_query["id"], "⚡ Публикую пост...")
    success, msg = publish_favorite_now(fav_id)
    channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
    link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

    if success:
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
        edit_message_text(
            chat_id,
            message_id,
            f"⚡ <b>Пост из Избранного опубликован в канале!</b>\n\n"
            f"📢 Канал: {link_html}\n"
            f"🕒 Время публикации: <b>{now_str}</b>",
            disable_web_page_preview=True
        )
    else:
        answer_callback_query(callback_query["id"], f"❌ {msg}", show_alert=True)


def handle_fav_show_text(callback_query: dict, fav_id: str):
    """Показывает полный текст избранного поста в чате."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "📖 Отправляю текст...")

    with QUEUE_LOCK:
        data = load_queue_data()
        fav = next((f for f in data.get("favorites", []) if f.get("id") == fav_id), None)

    if not fav:
        send_message(chat_id, "❌ Пост не найден в Избранном.")
        return

    title = fav.get("title") or "Избранный пост"
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "📅 В очередь (8ч)", "callback_data": f"fav_q:{fav_id}"},
                {"text": "⚡ Срочно (сейчас)", "callback_data": f"fav_now:{fav_id}"}
            ],
            [
                {"text": "🗑 Удалить из избранного", "callback_data": f"fav_del:{fav_id}"}
            ]
        ]
    }
    send_message(
        chat_id,
        f"⭐ <b>Избранный пост:</b> <i>«{escape_html(title)}»</i>\n\n"
        f"<i>(текст ниже)</i>",
        disable_web_page_preview=True
    )
    send_long_message(
        chat_id,
        fav["text"],
        parse_mode=None,
        reply_markup=keyboard,
        disable_web_page_preview=True
    )


def handle_fav_delete(callback_query: dict, fav_id: str):
    """Удаляет пост из Избранного."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]

    if remove_from_favorites(fav_id):
        answer_callback_query(callback_query["id"], "🗑 Пост удалён из Избранного")
        edit_message_text(chat_id, message_id, "🗑 <b>Пост удалён из Избранного.</b>")
    else:
        answer_callback_query(callback_query["id"], "❌ Пост не найден в Избранном", show_alert=True)


def handle_favorites_command(chat_id: str):
    """Команда /fav — показывает список сохранённых постов."""
    with QUEUE_LOCK:
        data = load_queue_data()
    favorites = data.get("favorites", [])

    if not favorites:
        send_message(
            chat_id,
            "⭐ <b>Избранное пусто (нет отложенных постов).</b>\n\n"
            "Вы можете сгенерировать пост через <code>/post</code> или по новости из <code>/top</code> "
            "и нажать <b>«⭐ В избранное»</b>, чтобы сохранить его на потом.",
            disable_web_page_preview=True
        )
        return

    header = (
        f"⭐ <b>Избранные посты (отложенные на потом)</b>\n"
        f"📦 Всего сохранено: <b>{len(favorites)}</b>\n\n"
    )

    lines = []
    keyboard_buttons = []
    for idx, f_item in enumerate(favorites[:6], start=1):
        f_id = f_item.get("id", "")
        raw_text = f_item.get("text", "")
        first_line = raw_text.split("\n")[0] if raw_text else ""
        title = f_item.get("title") or first_line[:40] or f"Пост #{idx}"
        saved_at = f_item.get("saved_at", "")
        saved_str = ""
        if saved_at:
            try:
                saved_str = f" (<i>{datetime.fromisoformat(saved_at).strftime('%d.%m %H:%M')}</i>)"
            except Exception:
                pass

        lines.append(f"<b>{idx}.</b> 📰 <i>«{escape_html(title)}»</i>{saved_str}")
        keyboard_buttons.append([
            {"text": f"📅 В очередь #{idx}", "callback_data": f"fav_q:{f_id}"},
            {"text": f"⚡ Срочно #{idx}", "callback_data": f"fav_now:{f_id}"},
            {"text": "👁 Текст", "callback_data": f"fav_show:{f_id}"},
            {"text": "🗑", "callback_data": f"fav_del:{f_id}"}
        ])

    send_message(
        chat_id,
        header + "\n\n".join(lines),
        reply_markup={"inline_keyboard": keyboard_buttons} if keyboard_buttons else None,
        disable_web_page_preview=True
    )


# =================== СЕССИИ ЗАПРОСОВ (RAG / ПОСТЫ) ===================


def load_query_sessions() -> Dict[str, Dict[str, Any]]:
    """Загружает сохраненные сессии вопросов/тем."""
    if os.path.exists(QUERY_SESSIONS_FILE):
        try:
            with open(QUERY_SESSIONS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log.warning("⚠️ Ошибка чтения сессий запросов: %s", e)
            return {}
    return {}


def save_query_session(session_id: str, data: Dict[str, Any]):
    """Сохраняет сессию с очисткой старых (>24ч)."""
    sessions = load_query_sessions()
    data["updated_at"] = time.time()
    sessions[session_id] = data
    now = time.time()
    # Чистим устаревшие сессии (старше суток)
    sessions = {k: v for k, v in sessions.items() if now - v.get("updated_at", 0) < 86400}
    try:
        with open(QUERY_SESSIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(sessions, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("⚠️ Ошибка сохранения сессии запроса: %s", e)


def get_query_session(session_id: str) -> Optional[Dict[str, Any]]:
    """Получает данные сессии по ID."""
    sessions = load_query_sessions()
    return sessions.get(session_id)


# =================== КОНТЕКСТ СТАТЕЙ ===================


def load_article_context(article_id: str) -> Optional[Dict[str, Any]]:
    """Загружает контекст статьи из .tg_bot_articles.json."""
    if not os.path.exists(ARTICLES_CONTEXT_FILE):
        return None
    try:
        with open(ARTICLES_CONTEXT_FILE, 'r', encoding='utf-8') as f:
            context = json.load(f)
        return context.get(article_id)
    except (json.JSONDecodeError, IOError) as e:
        log.warning("⚠️ Ошибка чтения контекста статей: %s", e)
        return None


# =================== NOTEBOOKLM RAG ===================


def _get_available_notebooks_sync() -> List[Dict[str, str]]:
    """Получает список доступных ноутбуков NotebookLM (синхронная обертка)."""
    return asyncio.run(_get_available_notebooks_async())


async def _get_available_notebooks_async() -> List[Dict[str, str]]:
    """Получает список ноутбуков NotebookLM, фильтрует News Intelligence за последние 4 мес."""
    try:
        from notebooklm import NotebookLMClient
    except ImportError:
        log.error("❌ notebooklm-py не установлен")
        return []

    # Проверка авторизации
    profile_path = os.path.expanduser('~/.notebooklm/profiles/default/storage_state.json')
    if not os.path.exists(profile_path) or os.path.getsize(profile_path) == 0:
        log.error("❌ NotebookLM не авторизован. Выполните: notebooklm login")
        return []

    try:
        async with NotebookLMClient.from_storage() as client:
            notebooks = await client.notebooks.list()

            # Фильтруем ноутбуки "News Intelligence — YYYY-MM"
            now = datetime.now()
            min_date = now - timedelta(days=MAX_NOTEBOOK_MONTHS * 31)

            result = []
            for nb in notebooks:
                # Ищем паттерн "News Intelligence — YYYY-MM"
                m = re.search(r'News Intelligence\s*[—–-]\s*(\d{4}-\d{2})', nb.title)
                if m:
                    month_str = m.group(1)
                    try:
                        nb_date = datetime.strptime(month_str, "%Y-%m")
                        if nb_date >= min_date.replace(day=1):
                            result.append({
                                "id": nb.id,
                                "title": nb.title,
                                "month": month_str,
                            })
                    except ValueError:
                        continue

            # Сортируем по месяцу (новые первые)
            result.sort(key=lambda x: x["month"], reverse=True)
            return result

    except Exception as e:
        log.error("❌ Ошибка получения списка ноутбуков: %s", e)
        return []


# =================== ПЕРИОДЫ ДЛЯ RAG ===================

RAG_PERIODS = {
    "today": {
        "title": "📅 За сегодня",
        "short_title": "Сегодня",
        "days": 1,
        "hint": "сегодня"
    },
    "3days": {
        "title": "📅 За 3 дня",
        "short_title": "3 дня",
        "days": 3,
        "hint": "последние 3 дня"
    },
    "week": {
        "title": "📅 За неделю",
        "short_title": "Неделя",
        "days": 7,
        "hint": "последнюю неделю (7 дней)"
    },
    "month": {
        "title": "📅 За месяц",
        "short_title": "Месяц",
        "days": 31,
        "hint": "текущий месяц"
    }
}


def query_notebooklm_rag(
    notebook_id: str,
    prompt: str,
    period_key: Optional[str] = None
) -> tuple[Optional[str], list[str]]:
    """
    Отправляет RAG-запрос в NotebookLM и возвращает (ответ, список_ссылок_источников).
    Синхронная обертка с поддержкой фильтрации по периоду (today, 3days, week, month).
    """
    return asyncio.run(_query_rag_async(notebook_id, prompt, period_key=period_key))


async def _query_rag_async(
    notebook_id: str,
    prompt: str,
    period_key: Optional[str] = None
) -> tuple[Optional[str], list[str]]:
    """Отправляет RAG-запрос в NotebookLM через chat.ask(), возвращает ответ и найденные ссылки."""
    try:
        from notebooklm import NotebookLMClient
    except ImportError:
        log.error("❌ notebooklm-py не установлен")
        return None, []

    try:
        async with NotebookLMClient.from_storage() as client:
            source_ids = None
            period_cfg = RAG_PERIODS.get(period_key) if period_key else None

            # Фильтрация по дням (сегодня, 3 дня, неделя)
            if period_cfg and period_cfg.get("days", 0) < 30:
                days = period_cfg["days"]
                target_dates = [(date.today() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

                try:
                    all_sources = await client.sources.list(notebook_id)
                    matching_sources = [
                        s for s in all_sources
                        if any(d in (getattr(s, "title", "") or "") for d in target_dates)
                    ]
                    if matching_sources:
                        source_ids = [s.id for s in matching_sources]
                        log.info("🎯 RAG: фильтр источников за %s (%s): найдено %d источников (%s)",
                                 period_cfg['title'], target_dates, len(source_ids),
                                 [s.title for s in matching_sources])
                    else:
                        log.info("ℹ️ RAG: источники за %s (%s) не найдены в блокноте, ищем по блокноту с текстовым фильтром",
                                 period_cfg['title'], target_dates)
                except Exception as e:
                    log.warning("⚠️ Не удалось отфильтровать sources.list: %s", e)

                dates_str = ", ".join(target_dates)
                enriched_prompt = f"""[ВРЕМЕННОЙ ФИЛЬТР: Ищи информацию строго за {period_cfg['hint']} (даты: {dates_str})].

{prompt}

Важно: Опирайся в первую очередь на факты, новости и события, произошедшие за {period_cfg['hint']} ({dates_str})."""
            else:
                enriched_prompt = prompt

            try:
                result = await client.chat.ask(
                    notebook_id=notebook_id,
                    question=enriched_prompt,
                    source_ids=source_ids
                )
            except Exception as ask_err:
                err_str = str(ask_err)
                if "RPC response exceeded" in err_str or "RPCResponseTooLargeError" in type(ask_err).__name__:
                    log.warning("⚠️ История диалога NotebookLM переполнена (>50 МБ), очищаем старую ветку и повторяем запрос...")
                    try:
                        conv_id = await client.chat.get_conversation_id(notebook_id)
                        if conv_id:
                            await client.chat.delete_conversation(notebook_id, conv_id)
                    except Exception as del_err:
                        log.warning("⚠️ Не удалось удалить переполненную беседу: %s", del_err)
                    result = await client.chat.ask(
                        notebook_id=notebook_id,
                        question=enriched_prompt,
                        source_ids=source_ids
                    )
                else:
                    raise ask_err

            if not result or not result.answer:
                log.warning("⚠️ NotebookLM вернул пустой ответ")
                return None, []

            log.info("✅ RAG ответ получен (%d символов)", len(result.answer))

            # Извлекаем ссылки или названия источников из references
            cited_links = []
            ref_url_map: Dict[int, str] = {}
            answer_raw = result.answer or ""
            # Находим номера цитат [1], [2, 3], реально использованные в тексте ответа
            cited_nums_in_answer = {
                int(x)
                for m_cit in re.finditer(r'\[([0-9,\s\-–—]+)\]', answer_raw)
                for x in re.findall(r'\d+', m_cit.group(1))
            }
            query_keywords = {
                w.lower()
                for w in re.findall(r'[A-Za-zА-Яа-яЁёÁÉÍÓÚÑáéíóúñÜü0-9]{4,}', f"{prompt} {answer_raw}")
            }

            if hasattr(result, "references") and result.references:
                for idx_ref, ref in enumerate(result.references, 1):
                    cited_text = getattr(ref, "cited_text", "") or ""
                    cit_num = int(getattr(ref, "citation_number", None) or idx_ref)

                    # Если один чанк NotebookLM захватил 2 соседние статьи через '---' или '### ',
                    # выбираем только тот подблок статьи, который максимально совпадает по ключевым словам с запросом и ответом
                    sub_blocks = [
                        b.strip()
                        for b in re.split(r'(?:\n\s*---+\s*\n|(?=###\s+\d+\.)|(?=\bArticle\s+ID\s*:))', cited_text)
                        if b and b.strip()
                    ]
                    if len(sub_blocks) > 1 and query_keywords:
                        def _score_subblock(blk: str) -> int:
                            blk_words = {
                                w.lower()
                                for w in re.findall(r'[A-Za-zА-Яа-яЁёÁÉÍÓÚÑáéíóúñÜü0-9]{4,}', blk)
                            }
                            return len(blk_words & query_keywords)
                        best_blocks = sorted(sub_blocks, key=_score_subblock, reverse=True)
                        target_texts = [best_blocks[0], cited_text]
                    else:
                        target_texts = [cited_text]

                    orig_urls = []
                    for t_chunk in target_texts:
                        # 0. Самый надежный способ: достаем точный URL из локальной БД по Article ID
                        art_ids = re.findall(r'Article\s+ID[:\s*]*(\d+)', t_chunk, flags=re.IGNORECASE)
                        for aid_str in art_ids:
                            db_u = lookup_article_url_by_id(int(aid_str))
                            if db_u and is_valid_article_url(db_u) and db_u not in orig_urls:
                                orig_urls.append(db_u)
                                break
                        if orig_urls:
                            break

                        # 1. Ищем явный Original URL из разметки корпуса
                        found_orig = re.findall(
                            r'Original\s+URL[:\s*]*\[?(https?://[^\s)\]"\'>]+)',
                            t_chunk,
                            flags=re.IGNORECASE
                        )
                        for fo in found_orig:
                            cu = sanitize_article_url(fo)
                            if cu and is_valid_article_url(cu) and cu not in orig_urls:
                                orig_urls.append(cu)
                                break
                        if orig_urls:
                            break

                    if not orig_urls:
                        raw_urls = re.findall(r'https?://[^\s)\]"\'>]+', cited_text)
                        for ru in raw_urls:
                            cu = sanitize_article_url(ru)
                            if cu and is_valid_article_url(cu) and cu not in orig_urls:
                                orig_urls.append(cu)
                                break

                    for u in orig_urls:
                        clean_u = sanitize_article_url(u)
                        if clean_u and is_valid_article_url(clean_u):
                            if cit_num not in ref_url_map:
                                ref_url_map[cit_num] = clean_u
                            # Добавляем в общий список ссылок только те источники, которые реально процитированы [N] в ответе
                            if (not cited_nums_in_answer or cit_num in cited_nums_in_answer) and clean_u not in cited_links:
                                cited_links.append(clean_u)

            answer_text = result.answer
            # Очищаем все прямые URL внутри самого ответа RAG (чтобы карточки 1..10 сразу имели чистые ссылки)
            if answer_text:
                answer_text = re.sub(
                    r'https?://[^\s)\]"\'>]+',
                    lambda m: sanitize_article_url(m.group(0)),
                    answer_text
                )
            # Если в ответе есть строки "- Источники: ... [N]" без прямой http-ссылки, подставляем точный URL из ref_url_map
            if ref_url_map and answer_text:
                out_lines = []
                for ln in answer_text.splitlines():
                    if ("источник" in ln.lower() or "ссылк" in ln.lower()) and "http" not in ln:
                        c_nums = [int(n) for n in re.findall(r'\[\s*(\d+)', ln)]
                        if not c_nums:
                            for m_br in re.finditer(r'\[([0-9,\s\-–—]+)\]', ln):
                                c_nums.extend(int(x) for x in re.findall(r'\d+', m_br.group(1)))
                        matched_urls = []
                        for cn in c_nums:
                            if cn in ref_url_map and ref_url_map[cn] not in matched_urls:
                                matched_urls.append(ref_url_map[cn])
                        if matched_urls:
                            ln = ln.rstrip(" .") + f" ({matched_urls[0]})"
                    out_lines.append(ln)
                answer_text = "\n".join(out_lines)

            return answer_text, cited_links
    except Exception as e:
        log.error("❌ Ошибка RAG-запроса к NotebookLM: %s", e)
        return None, []


# =================== ПОСТ-ОБРАБОТКА ЧЕРЕЗ ANYMODEL LLM ===================


# =================== СТИЛИ НАПИСАНИЯ ПОСТА ===================

POST_STYLES = {
    "standard": {
        "title": "📰 Стандартный",
        "desc": "Информативный, сбалансированный тон, экспертный и лёгкий для восприятия.",
        "prompt": "Стиль: стандартный редакционный пост для Telegram-канала — чётко, структурированно, без воды, с умеренными эмодзи."
    },
    "click": {
        "title": "⚡ Цепляющий / Интригующий",
        "desc": "Яркий заголовок, фокус на парадоксе или неожиданном факте, сильный крючок.",
        "prompt": "Стиль: цепляющий и виральный — привлекающий внимание заголовок, динамичное повествование, фокус на том, что удивит или возмутит читателя."
    },
    "guide": {
        "title": "📋 Инструкция / Гайд",
        "desc": "Пошаговый разбор, практическая польза, что конкретно делать читателю.",
        "prompt": "Стиль: пошаговая инструкция / how-to гайд — акцент на конкретные действия, пункты 'Что делать', сроки и практическую пользу."
    },
    "analytic": {
        "title": "📊 Аналитика / Факты",
        "desc": "Больше цифр, статистики, контекста законов и экономических последствий.",
        "prompt": "Стиль: аналитический и взвешенный — упор на цифры, сравнения, законы, последствия для кошелька и жизни экспатов."
    },
    "friendly": {
        "title": "☕ Разговорный / Лайфстайл",
        "desc": "Дружеский тон за чашкой кофе, личный опыт, живой диалог с подписчиками.",
        "prompt": "Стиль: разговорный и тёплый, как совет от опытного друга за чашкой кофе в испанском баре — просто о сложном, с лёгким юмором."
    }
}


# =================== ПОСТ-ОБРАБОТКА ЧЕРЕЗ ANYMODEL LLM ===================

MEDIA_DOMAIN_NAMES: Dict[str, str] = {
    "elpais.com": "El País",
    "cincodias.elpais.com": "Cinco Días",
    "elmundo.es": "El Mundo",
    "larazon.es": "La Razón",
    "abc.es": "ABC",
    "lavanguardia.com": "La Vanguardia",
    "elconfidencial.com": "El Confidencial",
    "eldiario.es": "elDiario.es",
    "20minutos.es": "20minutos",
    "eldebate.com": "El Debate",
    "antena3.com": "Antena 3",
    "rtve.es": "RTVE",
    "telecinco.es": "Telecinco",
    "lasexta.com": "laSexta",
    "cadenaser.com": "Cadena SER",
    "cope.es": "COPE",
    "ondacero.es": "Onda Cero",
    "eleconomista.es": "elEconomista",
    "expansion.com": "Expansión",
    "idealista.com": "Idealista",
    "fotocasa.es": "Fotocasa",
    "infobae.com": "Infobae",
    "theobjective.com": "The Objective",
    "okdiario.com": "OKDIARIO",
    "vozpopuli.com": "Vozpópuli",
    "publico.es": "Público",
    "huffingtonpost.es": "El HuffPost",
    "europapress.es": "Europa Press",
    "efe.com": "EFE",
    "boe.es": "BOE",
    "dgt.es": "DGT",
    "aemet.es": "AEMET",
    "seg-social.es": "Seguridad Social",
    "agenciatributaria.es": "Agencia Tributaria",
    "levante-emv.com": "Levante-EMV",
    "diariosur.es": "Diario Sur",
    "diariodemallorca.es": "Diario de Mallorca",
    "diariodenavarra.es": "Diario de Navarra",
    "diariodeibiza.es": "Diario de Ibiza",
    "diariodeleon.es": "Diario de León",
    "laprovincia.es": "La Provincia",
    "canarias7.es": "Canarias7",
    "lne.es": "La Nueva España",
    "elperiodicodearagon.com": "El Periódico de Aragón",
    "elperiodico.com": "El Periódico",
    "heraldo.es": "Heraldo",
    "elcorreo.com": "El Correo",
    "diariovasco.com": "El Diario Vasco",
    "lavozdegalicia.es": "La Voz de Galicia",
    "farodevigo.es": "Faro de Vigo",
    "ultimahora.es": "Última Hora",
    "cronicaglobal.elespanol.com": "Crónica Global",
    "elespanol.com": "El Español",
    "informacion.es": "Información",
    "lasprovincias.es": "Las Provincias",
    "laverdad.es": "La Verdad",
    "ideal.es": "Ideal",
    "surinenglish.com": "SUR in English",
    "theolivepress.es": "The Olive Press",
    "majorcadailybulletin.com": "Majorca Daily Bulletin",
    "ara.cat": "Ara",
    "naciodigital.cat": "NacióDigital",
    "vilaweb.cat": "VilaWeb",
    "eltemps.es": "Eltiempo.es",
    "eltiempo.es": "Eltiempo.es",
}


def lookup_article_url_by_id(article_id: int) -> Optional[str]:
    """Возвращает точный оригинальный URL статьи из локальной БД baza.db по её ID."""
    if not article_id or not os.path.exists(DB_FILE):
        return None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=2.0)
        row = conn.execute("SELECT link FROM news WHERE id = ?", (int(article_id),)).fetchone()
        conn.close()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception:
        pass
    return None


def _resolve_url_in_db(candidate_url: str) -> Optional[str]:
    """
    Проверяет URL по индексу таблицы news(link) в baza.db:
    - если точный URL есть в базе — возвращает его;
    - если к URL прилип хвост ('.../3462300/La') или обрезан конец ('.htm' вместо '.html') —
      находит и возвращает 100% точный оригинальный URL из базы.
    """
    if not candidate_url or len(candidate_url) < 22 or not os.path.exists(DB_FILE):
        return None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=2.0)
        try:
            # 1. Быстрая проверка точного совпадения по UNIQUE-индексу
            row = conn.execute("SELECT link FROM news WHERE link = ? LIMIT 1", (candidate_url,)).fetchone()
            if row and row[0]:
                return str(row[0]).strip()

            # 2. Поиск по префиксу (без последних 1..15 символов на случай прилипшего слова или обрезанного .htm)
            prefix = re.sub(r'/[A-ZÁÉÍÓÚÑÜ][A-Za-zÁÉÍÓÚÑáéíóúñÜü0-9_:-]*$', '/', candidate_url)
            prefix = re.sub(r'\.htm$', '.html', prefix)
            search_pref = prefix[:max(20, len(prefix) - 6)]
            rows = conn.execute(
                "SELECT link FROM news WHERE link >= ? AND link < ? LIMIT 10",
                (search_pref, search_pref + "\uffff")
            ).fetchall()
            for (db_link,) in rows:
                if not db_link:
                    continue
                # Если candidate_url начинается с db_link (к нему прилип хвост типа 'La' или 'TÍTULO:')
                # или db_link начинается с candidate_url (обрезан конец типа '.htm' вместо '.html')
                if candidate_url.startswith(db_link) or db_link.startswith(candidate_url) or db_link.startswith(prefix):
                    return str(db_link).strip()
        finally:
            conn.close()
    except Exception:
        pass
    return None


def is_valid_article_url(url: str) -> bool:
    """
    Проверяет, что URL ведёт на конкретную новостную статью, а не на главную страницу домена
    (например, 'https://elpais.com' или 'https://elpais.com/') и не на RSS-ленту.
    """
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    m = re.match(r'^https?://[^/?#]+(/[^\s?#]*)?', u, flags=re.IGNORECASE)
    if not m:
        return False
    path = (m.group(1) or "").strip("/")
    # Отсекаем голые домены без пути к статье или слишком короткие заглушки
    if len(path) < 8:
        return False
    if re.search(r'(^|/)(rss|feed|portada|home|index\.html?)(/|$)|\.xml$|\.rss$', path, flags=re.IGNORECASE):
        return False
    # У настоящей статьи в пути есть либо дефис в slug, либо цифры (дата/ID), либо несколько сегментов '/'
    if "-" not in path and not re.search(r'\d', path) and "/" not in path:
        return False
    return True


def sanitize_article_url(url: str) -> str:
    """
    Очищает URL от мусора, прилипающего из склеенных строк cited_text в NotebookLM
    (например: '.htmlTÍTULO:', '.htmlRubén', '.htmlLa', '...-3462300/La', 'Original'),
    и сверяет результат с локальной базой baza.db для 100% точности ссылки.
    """
    if not url or not isinstance(url, str):
        return ""
    u = url.strip().rstrip(").,;\"'>]")

    # 1. Если в URL есть .shtml / .html / .htm / .php / .aspx и сразу после него прилипло слово с заглавной буквы или двоеточие
    #    Важно: порядок shtml -> html -> htm и проверка (?=[A-ZА-ЯЁÁÉÍÓÚÑÜ:]), чтобы '.html' НИКОГДА не превращался в '.htm'!
    u = re.sub(
        r'(\.(?:shtml|html|htm|php|aspx|ece))(?=[A-ZА-ЯЁÁÉÍÓÚÑÜ:])[A-Za-zА-Яа-яЁёÁÉÍÓÚÑáéíóúñÜü0-9_:-]+$',
        r'\1',
        u
    )
    # 2. Срезаем прилипшие служебные маркеры в конце любого URL
    u = re.sub(r'(?:T[ÍI]TULO|RESUMEN|FUENTE|Original|User|Article)\b[:\s]*.*$', '', u, flags=re.IGNORECASE)
    # 3. Если после слэша или строчного slug/цифры в конце длинного пути прилипло слово с заглавной буквы
    #    (например, 'https://www.moncloa.com/2026/11/05/competencias-xunta-galicia-aerodromos-3462300/La')
    if u.count("/") >= 4:
        u = re.sub(r'/[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]{1,15}:?$', '/', u)
        u = re.sub(r'(?<=[a-z0-9])[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]{1,15}:?$', '', u)

    u = u.rstrip(").,;:\"'>]")

    # 4. Сверяем с локальной базой baza.db (восстанавливает обрезанный '.htm' -> '.html' и убирает любой прилипший хвост)
    db_exact = _resolve_url_in_db(u)
    if db_exact:
        return db_exact

    return u


def extract_source_domain_label(url: str) -> str:
    """Возвращает красивое название СМИ по URL (например, 'El País', 'La Razón', 'Antena 3')."""
    if not url:
        return "Источник"
    m = re.match(r'^https?://(?:www\.|m\.)?([^/?#]+)', url.strip(), flags=re.IGNORECASE)
    if not m:
        return "Источник"
    host = m.group(1).lower()
    if host in MEDIA_DOMAIN_NAMES:
        return MEDIA_DOMAIN_NAMES[host]
    for dom, label in MEDIA_DOMAIN_NAMES.items():
        if host.endswith("." + dom):
            return label
    # Генерируем читаемое имя из домена первого уровня (без .es/.com)
    parts = host.split(".")
    core = parts[-2] if len(parts) >= 2 else parts[0]
    return core.replace("-", " ").capitalize()


def visible_post_length(text: str) -> int:
    """Возвращает реальную видимую длину текста в Telegram (без учета скрытых URL внутри [текст](url) и **)."""
    if not text:
        return 0
    vis = re.sub(r'\[([^\]]+)\]\((https?://[^\)\s]+)\)', r'\1', text)
    vis = vis.replace("**", "").replace("__", "")
    return len(vis)


def prettify_post_links(body_text: str, allowed_urls: Optional[List[str]] = None) -> str:
    """
    1. Очищает все URL внутри существующих Markdown-ссылок [текст](url) от мусора (.htmlTÍTULO: и т.д.)
       и удаляет битые ссылки на корень домена (вроде https://elpais.com/).
    2. Схлопывает вертикальные списки голых ссылок вида '• https://...' в компактную строку
       '🔗 Источники: [La Razón](url1) • [El País](url2) • [Antena 3](url3)' (максимум 5 уникальных СМИ).
    3. Превращает любые оставшиеся голые ссылки https://... в аккуратные спрятанные ссылки [Название СМИ](url).
    """
    if not body_text:
        return ""

    allowed_set = {sanitize_article_url(u) for u in (allowed_urls or []) if u}

    # 1. Чистим URL внутри уже оформленных [текст](url)
    def _fix_md_link(m: re.Match) -> str:
        anchor = m.group(1).strip()
        clean_u = sanitize_article_url(m.group(2))
        if not clean_u or not is_valid_article_url(clean_u):
            return ""
        if allowed_set and clean_u not in allowed_set:
            return ""
        if anchor.startswith("http"):
            anchor = extract_source_domain_label(clean_u)
        return f"[{anchor}]({clean_u})"

    body_text = re.sub(r'\[([^\]]+)\]\((https?://[^\)\s]+)\)', _fix_md_link, body_text)

    # 2. Если есть блок '🔗 Источники:' с вертикальным списком голых или оформленных ссылок — собираем его в одну красивую строку
    src_block_re = re.compile(
        r'(?:🔗\s*)?(?:\*\*)?Источники?(?:\*\*)?[:\s]*\n+(?:[ \t]*[•\-*]\s*https?://[^\n]+\n?)+',
        flags=re.IGNORECASE
    )

    def _collapse_sources_block(m: re.Match) -> str:
        block_str = m.group(0)
        raw_urls = re.findall(r'https?://[^\s)\]"\'>]+', block_str)
        pretty_items = []
        seen_labels = set()
        for ru in raw_urls:
            cu = sanitize_article_url(ru)
            if not cu or not is_valid_article_url(cu):
                continue
            if allowed_set and cu not in allowed_set:
                continue
            lbl = extract_source_domain_label(cu)
            if lbl not in seen_labels:
                seen_labels.add(lbl)
                pretty_items.append(f"[{lbl}]({cu})")
            if len(pretty_items) >= 5:
                break
        if not pretty_items:
            return ""
        prefix = "🔗 Источник: " if len(pretty_items) == 1 else "🔗 Источники: "
        return prefix + " • ".join(pretty_items) + "\n"

    body_text = src_block_re.sub(_collapse_sources_block, body_text)

    # 3. Заменяем оставшиеся голые URL (не внутри скобок (...)) на спрятанные ссылки [Название СМИ](url)
    def _replace_bare_url(m: re.Match) -> str:
        raw_u = m.group(1)
        clean_u = sanitize_article_url(raw_u)
        if not clean_u or not is_valid_article_url(clean_u):
            return ""
        if allowed_set and clean_u not in allowed_set:
            return ""
        label = extract_source_domain_label(clean_u)
        return f"[{label}]({clean_u})"

    body_text = re.sub(r'(?<!\()(?<!\[)(https?://[^\s)\]"\'>]+)', _replace_bare_url, body_text)

    # 4. Приводим в порядок однострочные блоки '🔗 Источник(и): ...' после удаления пустых/корневых ссылок
    def _clean_inline_sources_line(m: re.Match) -> str:
        line_content = m.group(1)
        valid_md_links = re.findall(r'\[[^\]]+\]\(https?://[^\)\s]+\)', line_content)
        if not valid_md_links:
            return ""
        prefix = "🔗 Источник: " if len(valid_md_links) == 1 else "🔗 Источники: "
        return prefix + " • ".join(valid_md_links)

    body_text = re.sub(
        r'^[ \t]*🔗\s*(?:\*\*)?Источники?(?:\*\*)?[:\s]*(.+)$',
        _clean_inline_sources_line,
        body_text,
        flags=re.IGNORECASE | re.MULTILINE
    )

    # Убираем пустые маркеры списка, если в конце был оборванный URL
    body_text = re.sub(r'\n[ \t]*[•\-]\s*(?=\n|$)', '', body_text)
    body_text = re.sub(r'\n{3,}', '\n\n', body_text)
    return body_text.strip()


def ensure_post_integrity(post_text: str, links: Optional[List[str]] = None, max_chars: int = 2200) -> str:
    """
    1. Очищает одиночные звёздочки (*), оставляя только **жирный шрифт**.
    2. Очищает ссылки от прилипших слов (.htmlTÍTULO: и т.п.) и прячет ВСЕ ссылки под красивые названия СМИ [El País](url).
    3. Гарантирует, что пост ВСЕГДА помещается ровно в 1 стандартное сообщение Telegram (без разбиения на 2 поста).
    """
    if not post_text:
        return ""

    # 0. Очищаем одиночные звёздочки (*), сохраняя **жирный**
    post_text = clean_asterisks_formatting(post_text)

    # Отделяем служебный блок вариантов заголовка (💡 ВАРИАНТЫ ЗАГОЛОВКА: ... ---) от тела поста
    m_split = re.search(r'^(.*?)\n\s*[-_*=]{3,}\s*\n(.*)$', post_text, flags=re.DOTALL)
    if m_split and "ВАРИАНТ" in m_split.group(1).upper():
        head_block = m_split.group(1).strip()
        body_text = m_split.group(2).strip()
    else:
        head_block = ""
        body_text = post_text.strip()

    # 1. Извлекаем и очищаем переданные эталонные ссылки
    valid_links = []
    if links:
        for lk in links:
            if isinstance(lk, str) and lk.strip().startswith("http"):
                clean_lk = sanitize_article_url(lk)
                if clean_lk and is_valid_article_url(clean_lk) and clean_lk not in valid_links:
                    valid_links.append(clean_lk)

    # 2. Приводим все ссылки внутри текста поста к аккуратному спрятанному виду [Название СМИ](url)
    #    и отсекаем любые галлюцинированные/чужие ссылки, если известен точный список valid_links
    body_text = prettify_post_links(body_text, allowed_urls=valid_links if valid_links else None)

    # Если в тексте поста вообще нет ни одной ссылки, добавляем компактную строчку (до 4–5 уникальных СМИ)
    has_any_link_in_body = bool(re.search(r'\[[^\]]+\]\(https?://[^\)\s]+\)', body_text))
    if not has_any_link_in_body and valid_links:
        pretty_missing = []
        seen_domains = set()
        for lk in valid_links:
            dom_lbl = extract_source_domain_label(lk)
            if dom_lbl not in seen_domains:
                seen_domains.add(dom_lbl)
                pretty_missing.append(f"[{dom_lbl}]({lk})")
            if len(pretty_missing) >= 4:
                break

        if pretty_missing:
            if len(pretty_missing) == 1:
                lbl = f"\n\n🔗 Источник: {pretty_missing[0]}"
            else:
                lbl = "\n\n🔗 Источники: " + " • ".join(pretty_missing)

            tag_match = re.search(r'(\n+(?:#[\w\d_]+\s*)+)$', body_text)
            if tag_match:
                pos = tag_match.start(1)
                body_text = body_text[:pos].rstrip() + lbl + "\n\n" + body_text[pos:].lstrip()
            else:
                body_text = body_text.rstrip() + lbl

    # 3. Гарантируем, что пост помещается в 1 стандартное сообщение Telegram (и по видимому тексту, и по HTML)
    vis_len = visible_post_length(body_text)
    html_len = len(markdown_to_telegram_html(body_text))
    if vis_len > max_chars or html_len > 3650:
        # Выделяем подвал (блок 🔗 Источник(и) + хэштеги), чтобы никогда его не обрезать
        footer_match = re.search(
            r'(\n+(?:🔗\s*(?:\*\*)?Источники?(?:\*\*)?[^\n]*\n+)?(?:#[\w\d_]+\s*)+)$',
            body_text,
            flags=re.IGNORECASE
        )
        footer = footer_match.group(1) if footer_match else ""
        main_body = body_text[:len(body_text) - len(footer)].rstrip()

        footer_vis = visible_post_length(footer)
        target_vis = min(max_chars, 2400) - footer_vis - 15
        if target_vis > 300 and len(main_body) > target_vis:
            # Пересчитываем пропорциональный срез с учетом спрятанных ссылок в main_body
            ratio = target_vis / max(1, visible_post_length(main_body))
            approx_cut_idx = min(len(main_body), int(len(main_body) * min(1.0, ratio)))
            cut = main_body[:approx_cut_idx]
            # Не режем внутри незакрытой Markdown-ссылки [текст](url)
            last_open_br = cut.rfind('[')
            last_close_par = cut.rfind(')')
            if last_open_br > last_close_par:
                cut = cut[:last_open_br].rstrip()
            sent_ends = [m.end() for m in re.finditer(r'(?<!\d)[.!?](?=\s|\n|$)', cut)]
            if sent_ends and sent_ends[-1] > int(len(cut) * 0.55):
                cut = cut[:sent_ends[-1]]
            cut = re.sub(r'\n+(?:📌\s*)?\d+[.)]\s*$', '', cut.rstrip())
            body_text = cut.rstrip() + ("\n\n" + footer.lstrip() if footer else "")

    # Очищаем случайно оборванный пустой пункт перед хэштегами
    body_text = re.sub(r'\n+(?:📌\s*)?\d+[.)]\s*(?=\n+#)', '\n', body_text)

    if head_block:
        return f"{head_block}\n---\n{body_text.strip()}"
    return body_text.strip()


def polish_post_with_llm(
    raw_rag_text: str,
    article_context: Dict[str, Any],
    style_key: str = "standard",
    chosen_angle: Optional[str] = None,
    extra_links: Optional[List[str]] = None
) -> str:
    """
    Прогоняет сырой RAG-ответ через AnyModel LLM для финальной полировки.
    """
    if not ANYMODEL_KEY:
        log.warning("⚠️ ANYMODEL_KEY не задан. Возвращаю RAG-текст без пост-обработки.")
        return _basic_cleanup(raw_rag_text, article_context, extra_links)

    original_url = sanitize_article_url(article_context.get("url", ""))
    title = article_context.get("title", "")
    categories = article_context.get("categories", [])
    hashtags = " ".join(f"#{c}" for c in categories[:5]) if categories else "#испания #новости"

    style_cfg = POST_STYLES.get(style_key, POST_STYLES["standard"])
    style_instruction = style_cfg["prompt"]

    # Если у статьи из /top уже есть точный оригинальный URL — используем только его,
    # чтобы в пост по одной конкретной статье не попадали ссылки из соседних чанков NotebookLM
    all_links = []
    if original_url and is_valid_article_url(original_url):
        all_links.append(original_url)
    elif extra_links:
        for lk in extra_links:
            clean_lk = sanitize_article_url(lk)
            if clean_lk and is_valid_article_url(clean_lk) and clean_lk not in all_links:
                all_links.append(clean_lk)

    pretty_links_list = [f"- [{extract_source_domain_label(lk)}]({lk})" for lk in all_links[:5]]
    links_text = "\n".join(pretty_links_list) if pretty_links_list else "(нет ссылок)"

    angle_instruction = f"Фокус публикации (выбранная идея): {chosen_angle}" if chosen_angle else ""

    system_prompt = f"""Ты — главный редактор русскоязычного Telegram-канала о жизни и новостях в Испании.
Твоя задача — взять сырой материал от RAG-системы (NotebookLM) и превратить его в идеальный, готовый к публикации ТГ-пост.

{style_instruction}
{angle_instruction}

СТРОГИЕ ПРАВИЛА И СТРУКТУРА:
1. Пиши ТОЛЬКО на русском языке.
2. ОБЯЗАТЕЛЬНЫЙ БЛОК ВАРИАНТОВ ЗАГОЛОВКА В НАЧАЛЕ:
   Сообщение ДОЛЖНО начинаться строго с блока 3–4 принципиально РАЗНЫХ, КОНТРАСТНЫХ вариантов заголовка на выбор редактора, затем разделитель '---', а затем сам готовый пост:

💡 ВАРИАНТЫ ЗАГОЛОВКА:
1️⃣ [Фактический]: краткий новостной заголовок, суть события, конкретные цифры/даты/факты (без кликбейта).
2️⃣ [Цепляющий / Виральный]: фокус на парадоксе, неожиданности, сильной эмоции, контрасте или интригующем крючке.
3️⃣ [Практический / Польза]: формат «Как...», «Что изменится для...», акцент на выгоде, рисках или защите кошелька экспата.
4️⃣ [Вопрос / Дискуссия]: острый, вовлекающий вопрос к читателям для комментариев и репостов.

---

[Далее — готовый текст поста, который начинается с одного из этих заголовков с тематическим эмодзи]

3. КАТЕГОРИЧЕСКИЙ ЗАПРЕТ НА ПОХОЖИЕ ЗАГОЛОВКИ:
   Варианты заголовков ОБЯЗАНЫ быть принципиально разными по структуре, тону, длине и подаче!
4. КАТЕГОРИЧЕСКИ УДАЛИ все квадратные скобки вида [1], [2], [N], [source 1] — это технические маркеры цитирования RAG.
5. КРАСИВЫЕ СПРЯТАННЫЕ ССЫЛКИ НА ИСТОЧНИКИ:
   КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО выводить голые длинные URL (https://...) открытым текстом!
   Все ссылки ОБЯЗАНЫ быть спрятаны под короткое название СМИ в формате Markdown: [El País](https://...) или [La Razón](https://...).
   В конце поста (перед хэштегами) оформи источники ОДНОЙ компактной строкой через точку:
   🔗 Источники: [La Razón](url1) • [El País](url2) (максимум 3–4 главных источника).
6. Длина основного текста поста — СТРОГО 900–1400 символов. Пост ОБЯЗАН компактно помещаться ровно в 1 стандартное сообщение Telegram!
7. Используй продуманные эмодзи для структуры (📌, 💡, ⚠️, 📊, 🏠, 💰 и т.д.).
8. В самом конце обязательно добавь 3-5 релевантных хэштегов.
9. НЕ используй HTML теги.
10. КАТЕГОРИЧЕСКИЙ ЗАПРЕТ НА ОДИНОЧНЫЕ ЗВЁЗДОЧКИ (*): Запрещено использовать одиночные звёздочки (*). Списки оформляй только через эмодзи или маркер •. Для выделения важного разрешено использовать ТОЛЬКО двойные звёздочки **жирный текст**."""

    user_prompt = f"""Сырой текст от RAG-системы:
---
{raw_rag_text}
---

Данные статьи:
- Тема: {title}
- Выбранный угол/идея: {chosen_angle or "Общий обзор"}
- Доступные ссылки на источники (прячь их под названия СМИ в формате [Название СМИ](URL)):
{links_text}
- Рекомендуемые хэштеги: {hashtags}

Напиши готовый ТГ-пост в стиле «{style_cfg['title']}».
Требования:
1. В самом верху дай 4 КОНТРАСТНЫХ, не похожих друг на друга варианта заголовка (Фактический, Цепляющий, Практический, Вопрос/Дискуссия).
2. Разделитель '---'.
3. Полный готовый текст поста (ровно 900–1400 символов, компактно в 1 пост ТГ, со спрятанными ссылками [Название СМИ](URL))."""

    endpoint = f"{ANYMODEL_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {ANYMODEL_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": ANYMODEL_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.65,
        "max_tokens": 2000,
    }

    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            log.warning("⚠️ AnyModel API ошибка (%d): %s", resp.status_code, resp.text[:200])
            return _basic_cleanup(raw_rag_text, article_context, extra_links)

        data = resp.json()
        choices = data.get("choices", [])
        if not choices:
            return _basic_cleanup(raw_rag_text, article_context, extra_links)

        polished = choices[0].get("message", {}).get("content", "")
        if polished:
            polished = clean_rag_citations(polished)
            polished = ensure_post_integrity(polished, all_links, max_chars=1550)
            log.info("✅ Пост отполирован через AnyModel (%d символов, стиль: %s)", len(polished), style_key)
            return polished.strip()

    except Exception as e:
        log.warning("⚠️ Ошибка AnyModel пост-обработки: %s", e)

    return _basic_cleanup(raw_rag_text, article_context, extra_links)


def _basic_cleanup(text: str, article_context: Dict[str, Any], extra_links: Optional[List[str]] = None) -> str:
    """Базовая чистка текста без LLM (fallback)."""
    cleaned = clean_rag_citations(text)

    url = sanitize_article_url(article_context.get("url", ""))
    all_links = []
    if url and is_valid_article_url(url):
        all_links.append(url)
    elif extra_links:
        for lk in extra_links:
            clean_lk = sanitize_article_url(lk)
            if clean_lk and is_valid_article_url(clean_lk) and clean_lk not in all_links:
                all_links.append(clean_lk)

    categories = article_context.get("categories", [])
    if categories:
        hashtags = " ".join(f"#{c}" for c in categories[:5])
        cleaned += f"\n\n{hashtags}"

    return ensure_post_integrity(cleaned.strip(), all_links, max_chars=1550)


def polish_custom_topic_post_with_llm(
    raw_rag_text: str,
    topic: str,
    style_key: str = "standard",
    extra_links: Optional[List[str]] = None,
    length_hint: str = "900-1400",
    max_tokens: int = 1500,
    item_count: int = 1,
    max_chars: int = 2200
) -> str:
    """
    Прогоняет сырой RAG-ответ по произвольной теме через AnyModel LLM
    для написания идеального ТГ-поста в выбранном стиле.
    """
    if not ANYMODEL_KEY:
        log.warning("⚠️ ANYMODEL_KEY не задан. Возвращаю RAG-текст без пост-обработки.")
        return _basic_topic_cleanup(raw_rag_text, topic, extra_links, max_chars=max_chars)

    style_cfg = POST_STYLES.get(style_key, POST_STYLES["standard"])
    style_instruction = style_cfg["prompt"]

    clean_links = []
    if extra_links:
        for lk in extra_links:
            cu = sanitize_article_url(lk)
            if cu and cu not in clean_links:
                clean_links.append(cu)

    max_links_to_pass = max(4, item_count)
    pretty_links_list = [f"- [{extract_source_domain_label(lk)}]({lk})" for lk in clean_links[:max_links_to_pass]]
    links_text = "\n".join(pretty_links_list) if pretty_links_list else "(нет внешних ссылок)"

    system_prompt = f"""Ты — главный редактор русскоязычного Telegram-канала о жизни, законах, налогах и новостях в Испании.
Твоя задача — написать идеальный, готовый к публикации пост для канала на заданную тему, опираясь на факты и материал от RAG-системы.

{style_instruction}

СТРОГИЕ ПРАВИЛА И СТРУКТУРА:
1. Пиши ТОЛЬКО на русском языке.
2. ОБЯЗАТЕЛЬНЫЙ БЛОК ВАРИАНТОВ ЗАГОЛОВКА В НАЧАЛЕ:
   Сообщение ДОЛЖНО начинаться строго с блока 3–4 принципиально РАЗНЫХ, КОНТРАСТНЫХ вариантов заголовка на выбор редактора, затем разделитель '---', а затем сам готовый пост:

💡 ВАРИАНТЫ ЗАГОЛОВКА:
1️⃣ [Фактический]: краткий информационный заголовок, суть события, конкретные цифры/даты/факты (без кликбейта).
2️⃣ [Цепляющий / Виральный]: фокус на парадоксе, неожиданности, сильной эмоции, контрасте или интригующем крючке.
3️⃣ [Практический / Польза]: формат «Как...», «Что изменится для...», акцент на выгоде, рисках или защите кошелька экспата.
4️⃣ [Вопрос / Дискуссия]: острый, вовлекающий вопрос к читателям для комментариев и репостов.

---

[Далее — готовый текст поста, который начинается с одного из этих заголовков с тематическим эмодзи]

3. КАТЕГОРИЧЕСКИЙ ЗАПРЕТ НА ПОХОЖИЕ ЗАГОЛОВКИ:
   Варианты заголовков ОБЯЗАНЫ быть принципиально разными по структуре, тону, длине и подаче!
4. КАТЕГОРИЧЕСКИ УДАЛИ все квадратные скобки вида [1], [2], [N], [source 1] — никаких технических меток цитирования в тексте быть не должно!
5. КРАСИВЫЕ СПРЯТАННЫЕ ССЫЛКИ НА ПЕРВОИСТОЧНИКИ:
   КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО вставлять голые длинные URL (https://...) открытым текстом или делать длинный список сырых URL!
   Все ссылки ОБЯЗАНЫ быть спрятаны под короткое название СМИ в формате Markdown: [El País](https://...) или [La Razón](https://...).
   - Если это пост по одной теме: укажи в конце перед хэштегами одну компактную строчку (не более 3–4 главных СМИ):
     🔗 Источники: [La Razón](url1) • [El País](url2) • [Antena 3](url3)
   - Если это дайджест из нескольких новостей: в конце каждого пункта ставь аккуратную ссылку 🔗 [Название СМИ](url).
6. Длина видимого текста поста — СТРОГО {length_hint} символов. Текст ОБЯЗАН целиком помещаться в 1 стандартный пост Telegram!
7. Используй продуманные эмодзи для структуры (📌, 💡, ⚠️, 📊, 🏠, 💰 и т.д.).
8. В самом конце обязательно добавь 3-5 релевантных хэштегов (например: #испания #законы #новости #жизньвиспании).
9. НЕ используй HTML теги.
10. КАТЕГОРИЧЕСКИЙ ЗАПРЕТ НА ОДИНОЧНЫЕ ЗВЁЗДОЧКИ (*): Запрещено использовать одиночные звёздочки (*). Списки оформляй только через эмодзи или маркер •. Для выделения важного разрешено использовать ТОЛЬКО двойные звёздочки **жирный текст**."""

    if item_count > 1:
        per_item_chars = max(200, min(340, 1750 // item_count))
        system_prompt += f"""\n11. ОБЯЗАТЕЛЬНОЕ ВКЛЮЧЕНИЕ ВСЕХ {item_count} СЮЖЕТОВ В 1 СООБЩЕНИЕ TELEGRAM:
Во входных материалах передано РОВНО {item_count} выбранных новостных сюжетов (от Сюжета 1 до Сюжета {item_count}).
Ты ОБЯЗАН включить в основной текст поста КАЖДЫЙ из этих {item_count} сюжетов отдельным нумерованным пунктом!
Формат каждого пункта:
📌 1. **[Название 1-го сюжета]**
Кратко суть события, цифры, ключевые детали (ёмко, 2–3 предложения, ~{per_item_chars} символов). 🔗 [Название СМИ](url)
...
📌 {item_count}. **[Название {item_count}-го сюжета]**
Кратко суть события... 🔗 [Название СМИ](url)
Категорически ЗАПРЕЩЕНО игнорировать или отбрасывать любые из {item_count} сюжетов! Пиши ёмко, чтобы все {item_count} пунктов гарантированно уместились в 1 стандартный пост Telegram."""

    user_prompt = f"""Тема поста, сформулированная автором:
«{topic}»

Фактический материал и контекст от RAG-системы (NotebookLM):
---
{raw_rag_text if (raw_rag_text and raw_rag_text.strip()) else "(RAG не вернул специфических данных, напиши максимально экспертный пост по теме, опираясь на общие знания о ситуации в Испании)"}
---

Найденные ссылки на источники (обязательно прячь их под название издания [Название СМИ](URL), НЕ выводи голые URL!):
{links_text}

Напиши готовый ТГ-пост в стиле «{style_cfg['title']}».
Требования:
1. В самом верху дай 4 КОНТРАСТНЫХ, не похожих друг на друга варианта заголовка (Фактический, Цепляющий, Практический, Вопрос/Дискуссия).
2. Разделитель '---'.
3. Полный готовый текст поста (видимой длиной {length_hint} символов, строго в 1 сообщение Telegram, со спрятанными ссылками [Название СМИ](URL) и хэштегами в конце)."""

    if item_count > 1:
        user_prompt += f"""
4. ВНИМАНИЕ: Во входном материале передано ровно {item_count} сюжетов.
Твой пост ОБЯЗАН содержать РОВНО {item_count} нумерованных пунктов (от 1 до {item_count}), раскрывающих КАЖДЫЙ из этих {item_count} сюжетов, и при этом уложиться в 1 сообщение Telegram!"""

    endpoint = f"{ANYMODEL_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {ANYMODEL_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": ANYMODEL_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.65,
        "max_tokens": max_tokens + 500,
    }

    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        if resp.status_code != 200:
            log.warning("⚠️ AnyModel API ошибка (%d): %s", resp.status_code, resp.text[:200])
            return _basic_topic_cleanup(raw_rag_text, topic, clean_links, max_chars=max_chars)

        data = resp.json()
        choices = data.get("choices", [])
        if not choices:
            return _basic_topic_cleanup(raw_rag_text, topic, clean_links, max_chars=max_chars)

        polished = choices[0].get("message", {}).get("content", "")
        if polished:
            polished = clean_rag_citations(polished)
            polished = ensure_post_integrity(polished, clean_links, max_chars=max_chars)
            log.info("✅ Пост по теме '%s' отполирован через AnyModel (%d симв, стиль: %s)",
                     topic[:30], len(polished), style_key)
            return polished.strip()

    except Exception as e:
        log.warning("⚠️ Ошибка AnyModel пост-обработки: %s", e)

    return _basic_topic_cleanup(raw_rag_text, topic, clean_links, max_chars=max_chars)


def _basic_topic_cleanup(text: str, topic: str, extra_links: Optional[List[str]] = None, max_chars: int = 2200) -> str:
    """Базовая чистка для поста по теме без LLM."""
    cleaned = clean_rag_citations(text or "")
    res = f"📌 {topic}\n\n{cleaned}\n\n#испания #новости"
    return ensure_post_integrity(res.strip(), extra_links, max_chars=max_chars)


# =================== ПОСТРОЕНИЕ RAG-ПРОМПТА ===================


def build_rag_prompt(article_context: Dict[str, Any], chosen_angle: Optional[str] = None) -> str:
    """Формирует промпт для NotebookLM RAG на основе контекста статьи и выбранной идеи."""
    title = article_context.get("title", "")
    editorial_angles = article_context.get("editorial_angles", [])
    why = article_context.get("why", [])

    if chosen_angle:
        angles_text = f"→ ГЛАВНЫЙ ФОКУС: {chosen_angle}"
    elif editorial_angles:
        angles_text = "\n".join(f"→ {a}" for a in editorial_angles)
    else:
        angles_text = "→ общий обзор темы"

    why_text = "\n".join(f"• {w}" for w in why) if why else "• актуальная тема для жителей Испании"

    prompt = f"""Напиши подробный информативный обзор по следующей теме для русскоязычного Telegram-канала о жизни в Испании.

Тема новости: {title}

Ключевая идея / угол раскрытия:
{angles_text}

Почему это важно для аудитории:
{why_text}

Требования:
- Используй информацию из своих источников (RAG), чтобы дополнить и обогатить тему.
- Дай конкретные факты, цифры, даты — не абстрактные рассуждения.
- Если в источниках есть ссылки на законы, нормативные акты, BOE, первоисточники статей или официальные порталы — обязательно упомяни их названия и URL.
- Ответ должен быть на русском языке.
- Объём: 1000-2000 символов."""

    return prompt


# =================== ОБРАБОТКА CALLBACK QUERIES ===================


def handle_start_generation(callback_query: dict, article_id: str):
    """
    Шаг 1: Пользователь нажал '📝 Сгенерировать ТГ-пост'.
    Если у статьи есть идеи для публикации (editorial_angles), даём выбрать идею.
    Если идей нет, сразу переходим к выбору стиля.
    """
    article = load_article_context(article_id)
    if not article:
        answer_callback_query(callback_query["id"], "❌ Статья не найдена в контексте")
        return

    angles = article.get("editorial_angles", [])
    if angles:
        handle_select_angle(callback_query, article_id)
    else:
        handle_select_style(callback_query, article_id, angle_idx=0)


def handle_select_angle(callback_query: dict, article_id: str):
    """Шаг 1.1: Выбор идеи публикации из editorial_angles."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    article = load_article_context(article_id)
    if not article:
        answer_callback_query(callback_query["id"], "❌ Статья не найдена в контексте")
        return

    answer_callback_query(callback_query["id"], "💡 Выберите идею")

    angles = article.get("editorial_angles", [])
    keyboard = []

    for idx, angle in enumerate(angles[:4], 1):
        btn_text = f"💡 {angle[:45]}..." if len(angle) > 45 else f"💡 {angle}"
        keyboard.append([{"text": btn_text, "callback_data": f"ang:{article_id}:{idx}"}])

    # Опция "Все идеи / Комплексно"
    keyboard.append([{"text": "✨ Раскрыть все идеи комплексно", "callback_data": f"ang:{article_id}:0"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    title = article.get("title", "")[:60]
    send_message(
        chat_id,
        f"🎯 <b>Выберите идею подачи для поста:</b>\n"
        f"📰 <i>{title}...</i>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_select_style(callback_query: dict, article_id: str, angle_idx: int):
    """Шаг 2: Выбор стиля публикации."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    article = load_article_context(article_id)
    if not article:
        answer_callback_query(callback_query["id"], "❌ Статья не найдена в контексте")
        return

    answer_callback_query(callback_query["id"], "🎨 Выберите стиль")

    keyboard = []
    for s_key, s_data in POST_STYLES.items():
        btn_text = f"{s_data['title']}"
        keyboard.append([{"text": btn_text, "callback_data": f"sty:{article_id}:{angle_idx}:{s_key}"}])

    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    send_message(
        chat_id,
        "✍️ <b>Выберите стиль написания поста:</b>\n\n"
        "• <b>Стандартный</b> — экспертно, структурированно\n"
        "• <b>Цепляющий</b> — яркий заголовок, фокус на парадоксе\n"
        "• <b>Инструкция</b> — пошаговый гайд, польза\n"
        "• <b>Аналитика</b> — больше цифр, законов и фактов\n"
        "• <b>Разговорный</b> — тёплый тон, личный совет",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_select_notebook(callback_query: dict, article_id: str, angle_idx: int = 0, style_key: str = "standard"):
    """Шаг 3: Показывает кнопки выбора ноутбука NotebookLM (до 4 месяцев)."""
    chat_id = str(callback_query["message"]["chat"]["id"])

    article = load_article_context(article_id)
    if not article:
        answer_callback_query(callback_query["id"], "❌ Статья не найдена в контексте")
        return

    answer_callback_query(callback_query["id"], "🔍 Загружаю список ноутбуков...")
    send_message(chat_id, "⏳ <i>Загружаю список ноутбуков NotebookLM...</i>")

    notebooks = _get_available_notebooks_sync()

    if not notebooks:
        send_message(chat_id,
                     "❌ Не найдены ноутбуки NotebookLM.\n"
                     "Убедитесь, что выполнена авторизация: <code>notebooklm login</code>\n"
                     "И что существуют ноутбуки с именем 'News Intelligence — YYYY-MM'.")
        return

    keyboard = []
    for nb in notebooks[:MAX_NOTEBOOK_MONTHS]:
        btn_text = f"📓 {nb['month']}"
        short_key = nb['month']
        _save_notebook_mapping(short_key, nb['id'])

        # callback_data: rag:{article_id}:{angle_idx}:{style_key}:{short_key}
        callback_data = f"rag:{article_id}:{angle_idx}:{style_key}:{short_key}"
        keyboard.append([{"text": btn_text, "callback_data": callback_data}])

    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    title = article.get("title", "")[:60]
    style_title = POST_STYLES.get(style_key, {}).get("title", style_key)

    send_message(
        chat_id,
        f"📝 <b>Генерация поста</b>\n"
        f"📰 <i>{title}...</i>\n"
        f"🎨 Стиль: <b>{style_title}</b>\n\n"
        f"Выберите базу знаний NotebookLM для RAG-поиска:",
        reply_markup={"inline_keyboard": keyboard}
    )


# Маппинг коротких ключей на полные notebook_id
_NOTEBOOK_MAP_FILE = os.path.join(SCRIPT_DIR, '.tg_notebook_map.json')


def _save_notebook_mapping(short_key: str, notebook_id: str):
    """Сохраняет маппинг short_key → notebook_id."""
    mapping = {}
    if os.path.exists(_NOTEBOOK_MAP_FILE):
        try:
            with open(_NOTEBOOK_MAP_FILE, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    mapping[short_key] = notebook_id
    with open(_NOTEBOOK_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def _resolve_notebook_id(key: str) -> str:
    """Резолвит short_key в полный notebook_id (или возвращает key как есть)."""
    if os.path.exists(_NOTEBOOK_MAP_FILE):
        try:
            with open(_NOTEBOOK_MAP_FILE, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
            if key in mapping:
                return mapping[key]
        except (json.JSONDecodeError, IOError):
            pass
    return key


def handle_rag_generate(
    callback_query: dict,
    article_id: str,
    angle_idx: int,
    style_key: str,
    notebook_key: str
):
    """
    Шаг 4: Запускает RAG-генерацию поста с учётом выбранной идеи и стиля.
    Сохраняет все оригинальные ссылки.
    """
    chat_id = str(callback_query["message"]["chat"]["id"])

    article = load_article_context(article_id)
    if not article:
        answer_callback_query(callback_query["id"], "❌ Статья не найдена в контексте")
        return

    answer_callback_query(callback_query["id"], "🚀 Запускаю генерацию...")

    notebook_id = _resolve_notebook_id(notebook_key)

    # Определяем выбранную идею
    angles = article.get("editorial_angles", [])
    chosen_angle = None
    if angle_idx > 0 and angle_idx <= len(angles):
        chosen_angle = angles[angle_idx - 1]

    style_title = POST_STYLES.get(style_key, {}).get("title", style_key)
    title = article.get("title", "")[:60]

    send_message(chat_id,
                 f"⏳ <b>Генерирую ТГ-пост...</b>\n"
                 f"📰 <i>{title}...</i>\n"
                 f"🎯 Идея: <i>{chosen_angle or 'Комплексно'}</i>\n"
                 f"🎨 Стиль: <b>{style_title}</b>\n\n"
                 f"1️⃣ Поиск фактов и ссылок в NotebookLM RAG...",
                 disable_web_page_preview=True)

    # Шаг 1: RAG-запрос в NotebookLM
    prompt = build_rag_prompt(article, chosen_angle=chosen_angle)
    raw_answer, extra_links = query_notebooklm_rag(notebook_id, prompt)

    if not raw_answer:
        send_message(chat_id,
                     "❌ <b>Не удалось получить ответ от NotebookLM.</b>\n"
                     "Проверьте авторизацию: <code>notebooklm login</code>")
        return

    links_found_count = len(extra_links)
    send_message(chat_id,
                 f"✅ RAG-ответ получен ({len(raw_answer)} симв., доп. ссылок: {links_found_count})\n"
                 f"2️⃣ Полирую пост через AnyModel ({style_title})...",
                 disable_web_page_preview=True)

    # Шаг 2: Пост-обработка через AnyModel с сохранением ссылок и стилем
    polished_post = polish_post_with_llm(
        raw_rag_text=raw_answer,
        article_context=article,
        style_key=style_key,
        chosen_angle=chosen_angle,
        extra_links=extra_links
    )
    polished_post, _, _ = sync_initial_headline(polished_post)

    # Шаг 3: Отправляем готовый пост
    send_message(chat_id,
                 f"✅ <b>Готовый ТГ-пост ({style_title}):</b>\n"
                 f"<i>(скопируйте текст ниже)</i>",
                 disable_web_page_preview=True)

    # Отправляем пост отформатированным текстом (без сырых **)
    clean_for_copy = strip_headline_options_block(polished_post)
    post_resp = send_long_message(
        chat_id,
        markdown_to_telegram_html(clean_for_copy),
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    post_msg_id = None
    if post_resp and isinstance(post_resp, dict) and "result" in post_resp:
        post_msg_id = post_resp["result"].get("message_id")

    log.info("✅ ТГ-пост сгенерирован для статьи '%s' (длина: %d симв, стиль: %s)",
             title, len(polished_post), style_key)

    # Предлагаем публикацию в канал @iberia_news (в очередь 8ч или срочно)
    draft_id = save_post_draft(polished_post, title=title, chat_id=chat_id, post_msg_id=post_msg_id)
    offer_channel_publication(chat_id, draft_id)


def handle_cancel(callback_query: dict):
    """Обрабатывает кнопку 'Отмена'."""
    answer_callback_query(callback_query["id"], "❌ Отменено")
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]
    edit_message_reply_markup(chat_id, message_id, reply_markup={"inline_keyboard": []})


def process_callback_query(callback_query: dict):
    """Маршрутизирует callback_query по типу действия."""
    data = callback_query.get("data", "")
    log.info("📥 Callback: %s", data)

    if data == "cancel":
        handle_cancel(callback_query)
        return

    # 1. Начало: sel_nb:{article_id}
    if data.startswith("sel_nb:"):
        article_id = data[7:]
        handle_start_generation(callback_query, article_id)
        return

    # 2. Выбрана идея: ang:{article_id}:{angle_idx}
    if data.startswith("ang:"):
        parts = data[4:].split(":")
        if len(parts) == 2:
            article_id, angle_idx_str = parts
            handle_select_style(callback_query, article_id, int(angle_idx_str))
        return

    # 3. Выбран стиль: sty:{article_id}:{angle_idx}:{style_key}
    if data.startswith("sty:"):
        parts = data[4:].split(":")
        if len(parts) == 3:
            article_id, angle_idx_str, style_key = parts
            handle_select_notebook(callback_query, article_id, int(angle_idx_str), style_key)
        return

    # 4. Выбран блокнот и запуск RAG: rag:{article_id}:{angle_idx}:{style_key}:{notebook_key}
    # Поддерживаем и старый формат rag:{article_id}:{notebook_key}
    if data.startswith("rag:"):
        parts = data[4:].split(":")
        if len(parts) == 4:
            article_id, angle_idx_str, style_key, notebook_key = parts
            handle_rag_generate(callback_query, article_id, int(angle_idx_str), style_key, notebook_key)
        elif len(parts) == 2:
            article_id, notebook_key = parts
            handle_rag_generate(callback_query, article_id, 0, "standard", notebook_key)
        else:
            answer_callback_query(callback_query["id"], "❌ Некорректный формат данных")
        return

    # --- /top flow callbacks ---
    if data.startswith("top_n:"):
        count = data[6:]
        handle_top_select_date(callback_query, count)
        return

    if data.startswith("top_go:"):
        parts = data[7:].split(":", 1)
        if len(parts) == 2:
            count_str, date_key = parts
            handle_top_send(callback_query, int(count_str), date_key)
        return

    # --- /toppromt ---
    if data.startswith("tp_n:"):
        handle_toppromt_select_period(callback_query, data[5:])
        return

    if data.startswith("tp_go:"):
        parts = data[6:].split(":", 1)
        if len(parts) == 2:
            handle_toppromt_go(callback_query, parts[0], parts[1])
        return

    # --- Редакторские промпты ---
    if data.startswith("ed_pr:"):
        handle_editorial_select(callback_query, data[6:])
        return

    # --- Прямой вопрос к RAG ---
    if data.startswith("ask_p:"):
        parts = data[6:].split(":")
        if len(parts) == 2:
            session_id, period_key = parts
            if period_key == "archive":
                handle_ask_show_archive(callback_query, session_id)
            else:
                latest_nb = _get_latest_notebook_key()
                handle_ask_execute(callback_query, session_id, latest_nb, period_key=period_key)
        return

    if data.startswith("ask_nb:"):
        parts = data[7:].split(":")
        if len(parts) == 2:
            session_id, nb_key = parts
            handle_ask_execute(callback_query, session_id, nb_key, period_key="month")
        return

    # --- Корзина новостей из промптов / toppromt ---
    if data.startswith("bsk_one:"):
        parts = data[8:].split(":")
        if len(parts) == 2:
            handle_basket_single_item_prompt(callback_query, parts[0], int(parts[1]))
        return

    if data.startswith("bsk_one_s:"):
        parts = data[10:].split(":")
        if len(parts) == 3:
            handle_basket_single_item_execute(callback_query, parts[0], int(parts[1]), parts[2])
        return

    if data.startswith("bsk_t:"):
        parts = data[6:].split(":")
        if len(parts) == 2:
            handle_basket_toggle(callback_query, parts[0], int(parts[1]))
        return

    if data.startswith("bsk_c:"):
        session_id = data[6:]
        handle_basket_clear(callback_query, session_id)
        return

    if data.startswith("bsk_g:"):
        session_id = data[6:]
        handle_basket_generate_prompt(callback_query, session_id)
        return

    if data.startswith("bsk_s:"):
        parts = data[6:].split(":")
        if len(parts) == 2:
            handle_basket_generate_execute(callback_query, parts[0], parts[1])
        return

    # --- Генерация поста по теме ---
    if data.startswith("p_sty:"):
        parts = data[6:].split(":")
        if len(parts) == 2:
            session_id, style_key = parts
            handle_post_select_period(callback_query, session_id, style_key)
        return

    if data.startswith("p_p:"):
        parts = data[4:].split(":")
        if len(parts) == 3:
            session_id, style_key, period_key = parts
            if period_key == "archive":
                handle_post_select_notebook(callback_query, session_id, style_key)
            else:
                latest_nb = _get_latest_notebook_key()
                handle_post_execute(callback_query, session_id, style_key, latest_nb, period_key=period_key)
        return

    if data.startswith("p_nb:"):
        parts = data[5:].split(":")
        if len(parts) == 3:
            session_id, style_key, nb_key = parts
            handle_post_execute(callback_query, session_id, style_key, nb_key, period_key="month")
        return

    # --- Выбор варианта заголовка поста ---
    if data.startswith("hl:"):
        parts = data[3:].split(":")
        if len(parts) == 2:
            draft_id, opt_idx_str = parts
            try:
                handle_headline_selection(callback_query, draft_id, int(opt_idx_str))
            except Exception as e:
                log.error("❌ Ошибка handle_headline_selection: %s", e)
        return

    # --- Выбор категории поста (cat1..cat6) ---
    if data.startswith("cat:"):
        parts = data[4:].split(":")
        if len(parts) == 2:
            draft_id, cat_code = parts
            try:
                handle_category_selection(callback_query, draft_id, cat_code)
            except Exception as e:
                log.error("❌ Ошибка handle_category_selection: %s", e)
        return

    # --- Публикация в канал @iberia_news ---
    if data.startswith("pub_q:"):
        draft_id = data[6:]
        handle_publish_queue(callback_query, draft_id)
        return

    if data.startswith("pub_now:"):
        draft_id = data[8:]
        handle_publish_urgent(callback_query, draft_id)
        return

    if data.startswith("pub_c:"):
        draft_id = data[6:]
        handle_publish_cancel(callback_query, draft_id)
        return

    if data.startswith("pub_q_now:"):
        post_id = data[10:]
        handle_queue_publish_now(callback_query, post_id)
        return

    if data.startswith("pub_q_del:"):
        post_id = data[10:]
        handle_queue_delete(callback_query, post_id)
        return

    if data.startswith("pub_fav:"):
        draft_id = data[8:]
        handle_publish_fav(callback_query, draft_id)
        return

    # --- Избранные посты (Fav) ---
    if data.startswith("fav_q:"):
        fav_id = data[6:]
        handle_fav_to_queue(callback_query, fav_id)
        return

    if data.startswith("fav_now:"):
        fav_id = data[8:]
        handle_fav_publish_now(callback_query, fav_id)
        return

    if data.startswith("fav_show:"):
        fav_id = data[9:]
        handle_fav_show_text(callback_query, fav_id)
        return

    if data.startswith("fav_del:"):
        fav_id = data[8:]
        handle_fav_delete(callback_query, fav_id)
        return

    answer_callback_query(callback_query["id"], "❓ Неизвестная команда")


# =================== ЗАГРУЗКА ОЦЕНЁННЫХ СТАТЕЙ ИЗ БД ===================


def load_scored_articles_from_db(
    since_date: Optional[str] = None,
    top_n: int = 20,
    min_score: float = 5.0
) -> list:
    """
    Загружает ранее оценённые статьи из baza.db (news_scores + news).
    Возвращает список ScoredArticle-подобных dict'ов, совместимых с telegram_notifier.
    """
    if not os.path.exists(DB_FILE):
        log.error("❌ База данных не найдена: %s", DB_FILE)
        return []

    try:
        # Импортируем Pydantic-модели из scorer
        from scorer import ScoredArticle, EditorialClassification
        from date_filter import build_since_condition
    except ImportError as e:
        log.error("❌ Не удалось импортировать scorer: %s", e)
        return []

    conn = sqlite3.connect(DB_FILE)
    try:
        query = """
            SELECT n.id, n.title, n.llm_summary, n.source, n.link, n.published,
                   s.news_score, s.content_score, s.tier, s.is_top, s.is_evergreen,
                   s.scores_json, s.scored_at
            FROM news_scores s
            JOIN news n ON n.id = s.news_id
        """
        conditions = []
        params = []

        if since_date:
            date_cond, date_params = build_since_condition(since_date)
            if date_cond:
                conditions.append(date_cond)
                params.extend(date_params)

        if min_score > 0:
            conditions.append("s.news_score >= ?")
            params.append(min_score)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY s.news_score DESC"
        query += f" LIMIT {int(top_n * 2)}"  # берём с запасом

        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()

        articles = []
        for r in rows:
            try:
                editorial_data = json.loads(r[11])
                editorial = EditorialClassification.model_validate(editorial_data)
                is_breaking = editorial.scores.urgency >= 8

                articles.append(ScoredArticle(
                    id=r[0],
                    title=r[1] or "",
                    summary=r[2] or "",
                    source=r[3],
                    url=r[4],
                    published_at=r[5],
                    country="Spain",
                    editorial=editorial,
                    news_score=r[6],
                    content_score=r[7],
                    tier=r[8],
                    is_top=bool(r[9]),
                    is_evergreen_gem=bool(r[10]),
                    is_breaking=is_breaking,
                    scored_at=r[12] or datetime.now(timezone.utc).isoformat()
                ))
            except Exception as e:
                log.warning("⚠️ Ошибка восстановления статьи id=%s: %s", r[0], e)

        log.info("📋 Загружено %d оценённых статей из БД (since=%s)", len(articles), since_date)
        return articles

    except Exception as e:
        log.error("❌ Ошибка загрузки статей из БД: %s", e)
        return []
    finally:
        conn.close()


def _resolve_date_key(date_key: str) -> str:
    """Превращает date_key в формат YYYY-MM-DD или 'today'."""
    if date_key == "today":
        return "today"
    elif date_key == "yesterday":
        d = date.today() - timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    elif date_key == "3days":
        d = date.today() - timedelta(days=3)
        return d.strftime("%Y-%m-%d")
    elif date_key == "week":
        d = date.today() - timedelta(days=7)
        return d.strftime("%Y-%m-%d")
    else:
        # Предполагаем YYYY-MM-DD
        return date_key


# =================== КОМАНДА /top ===================


def handle_top_command(chat_id: str, args_text: str):
    """
    Обрабатывает команду /top с аргументами.

    Форматы:
      /top              → интерактивный выбор кнопками
      /top 20           → ТОП-20 за сегодня
      /top 20 today     → то же
      /top 10 2026-09-02 → ТОП-10 за дату
      /top 15 yesterday → ТОП-15 за вчера
    """
    parts = args_text.strip().split()

    if not parts:
        # Интерактивный выбор — показываем кнопки количества
        keyboard = [
            [
                {"text": "5️⃣ ТОП-5", "callback_data": "top_n:5"},
                {"text": "🔟 ТОП-10", "callback_data": "top_n:10"},
            ],
            [
                {"text": "2️⃣0️⃣ ТОП-20", "callback_data": "top_n:20"},
                {"text": "4️⃣0️⃣ ТОП-40", "callback_data": "top_n:40"},
            ],
            [{"text": "❌ Отмена", "callback_data": "cancel"}]
        ]
        send_message(
            chat_id,
            "📊 <b>Отправка ТОП новостей</b>\n\n"
            "Сколько новостей отправить?",
            reply_markup={"inline_keyboard": keyboard}
        )
        return

    # Парсим аргументы
    try:
        count = int(parts[0])
    except ValueError:
        send_message(chat_id,
                     "❌ Неверный формат. Используйте:\n"
                     "<code>/top 20</code> — ТОП-20 за сегодня\n"
                     "<code>/top 20 today</code>\n"
                     "<code>/top 10 2026-09-02</code>\n"
                     "<code>/top 15 yesterday</code>")
        return

    count = min(count, 100)  # ограничение

    date_key = parts[1] if len(parts) > 1 else "today"
    since_date = _resolve_date_key(date_key)

    # Запускаем отправку
    _send_top_articles(chat_id, count, since_date, date_key)


def handle_top_select_date(callback_query: dict, count_str: str):
    """Показывает кнопки выбора даты после выбора количества."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], f"Выбрано: ТОП-{count_str}")

    today_str = date.today().strftime("%Y-%m-%d")
    yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    keyboard = [
        [
            {"text": f"📅 Сегодня ({today_str})", "callback_data": f"top_go:{count_str}:today"},
        ],
        [
            {"text": f"📅 Вчера ({yesterday_str})", "callback_data": f"top_go:{count_str}:yesterday"},
        ],
        [
            {"text": "📅 3 дня", "callback_data": f"top_go:{count_str}:3days"},
            {"text": "📅 Неделя", "callback_data": f"top_go:{count_str}:week"},
        ],
        [{"text": "❌ Отмена", "callback_data": "cancel"}]
    ]
    send_message(
        chat_id,
        f"📊 <b>ТОП-{count_str} новостей</b>\n\n"
        f"За какой период отправить?",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_top_send(callback_query: dict, count: int, date_key: str):
    """Загружает статьи из БД и отправляет в Telegram с кнопками."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🚀 Загружаю статьи...")

    since_date = _resolve_date_key(date_key)
    _send_top_articles(chat_id, count, since_date, date_key)


def _send_top_articles(chat_id: str, count: int, since_date: str, date_label: str):
    """Загружает оценённые статьи из БД и отправляет в Telegram с inline-кнопками."""
    from telegram_notifier import (
        format_article_telegram,
        send_telegram_message_with_buttons,
        send_telegram_message,
        _save_article_context,
    )

    send_message(chat_id,
                 f"⏳ <b>Загружаю ТОП-{count} статей</b> (с {date_label})...",
                 disable_web_page_preview=True)

    articles = load_scored_articles_from_db(
        since_date=since_date,
        top_n=300,  # пул для News Diversity & Expansion (max_pool=300)
        min_score=0.0  # берём все, фильтрация по кол-ву
    )

    if not articles:
        send_message(chat_id,
                     f"❌ Нет оценённых статей за период <b>{date_label}</b>.\n"
                     f"Запустите скоринг: <code>python3 scorer.py --today</code>")
        return

    # Сортируем (рейтинг не меняется) и отбираем ТОП с разнообразием событий
    articles.sort(key=lambda x: x.news_score, reverse=True)
    try:
        from news_diversification import diversify
        div = diversify(articles, count)
        selected = div.selected
        log.info("🧩 Diversity: %d/%d, событий=%d, пул=%d",
                 len(selected), count, div.n_events_selected, div.pool_size)
        log.debug("\n%s", div.explain())
    except Exception as e:
        log.warning("⚠️ Diversity недоступен (%s) — обычный ТОП", e)
        selected = articles[:count]

    # Заголовок
    send_telegram_message(
        BOT_TOKEN, chat_id,
        f"🏆 <b>ТОП-{len(selected)} новостей Испании</b>\n"
        f"📅 Период: <b>{date_label}</b> (с {since_date})\n"
        f"📊 Всего оценённых: {len(articles)} | Показано: {len(selected)}"
    )
    time.sleep(0.5)

    # Отправляем каждую статью с кнопкой генерации поста
    sent = 0
    for art in selected:
        # Сохраняем контекст для кнопки генерации
        _save_article_context(art)

        msg_text = format_article_telegram(art)
        article_id = str(art.id or hash(art.title))

        inline_keyboard = [
            [{"text": "📝 Сгенерировать ТГ-пост", "callback_data": f"sel_nb:{article_id}"}]
        ]

        success = send_telegram_message_with_buttons(
            BOT_TOKEN, chat_id, msg_text, inline_keyboard
        )
        if success:
            sent += 1
        time.sleep(0.8)  # не спамим

    send_message(chat_id,
                 f"✅ Отправлено <b>{sent}/{len(selected)}</b> статей с кнопками генерации постов.")
    log.info("✅ /top: отправлено %d/%d статей в чат %s", sent, len(selected), chat_id)


# =================== ПРЯМОЙ ВОПРОС К RAG (/ask, /rag) ===================


def _get_latest_notebook_key() -> str:
    """Возвращает ключ самого свежего блокнота (например '2026-09')."""
    notebooks = _get_available_notebooks_sync()
    if notebooks:
        nb = notebooks[0]
        _save_notebook_mapping(nb['month'], nb['id'])
        return nb['month']
    return datetime.now().strftime("%Y-%m")


def handle_ask_command(chat_id: str, args_text: str):
    """
    Обрабатывает команду /ask или /rag.
    Если аргументы переданы -> сразу показываем выбор периода.
    Если нет -> переходим в режим ожидания ввода вопроса.
    """
    question = args_text.strip()
    if not question:
        USER_STATES[chat_id] = {"action": "waiting_ask_question"}
        send_message(
            chat_id,
            "❓ <b>Вопрос к базе знаний (NotebookLM RAG)</b>\n\n"
            "Напишите ваш вопрос следующим сообщением, и бот обратится к базе накопленных материалов.\n\n"
            "<i>(Для отмены отправьте /cancel)</i>"
        )
        return

    handle_ask_question_input(chat_id, question)


def handle_ask_question_input(chat_id: str, question: str):
    """Принимает вопрос от пользователя и предлагает выбрать период поиска в RAG."""
    session_id = uuid.uuid4().hex[:8]
    save_query_session(session_id, {
        "type": "ask",
        "question": question,
        "chat_id": chat_id
    })

    keyboard = [
        [
            {"text": "📅 За сегодня", "callback_data": f"ask_p:{session_id}:today"},
            {"text": "📅 За 3 дня", "callback_data": f"ask_p:{session_id}:3days"}
        ],
        [
            {"text": "📅 За неделю", "callback_data": f"ask_p:{session_id}:week"},
            {"text": "📅 За месяц", "callback_data": f"ask_p:{session_id}:month"}
        ],
        [
            {"text": "📓 Выбрать архивный месяц...", "callback_data": f"ask_p:{session_id}:archive"}
        ],
        [{"text": "❌ Отмена", "callback_data": "cancel"}]
    ]

    q_preview = question[:100] + ("..." if len(question) > 100 else "")
    send_message(
        chat_id,
        f"❓ <b>Вопрос к RAG:</b>\n<i>«{q_preview}»</i>\n\n"
        f"За какой период искать информацию в базе знаний?",
        reply_markup={"inline_keyboard": keyboard}
    )


# =================== /toppromt: топ по всем рубрикам ===================

TOPPROMPT_PERIODS = {"today": "сегодня", "3days": "3 дня", "week": "неделя", "month": "месяц"}
_TOPPROMPT_RUNNING: set = set()  # chat_id, для защиты от повторного запуска


def handle_toppromt_command(chat_id: str, args_text: str):
    """/toppromt → кнопки; /toppromt 10 week → сразу запуск."""
    parts = args_text.split()
    if not parts:
        keyboard = [
            [{"text": "5️⃣ ТОП-5", "callback_data": "tp_n:5"}, {"text": "🔟 ТОП-10", "callback_data": "tp_n:10"}],
            [{"text": "ТОП-20", "callback_data": "tp_n:20"}, {"text": "ТОП-40", "callback_data": "tp_n:40"}],
            [{"text": "❌ Отмена", "callback_data": "cancel"}],
        ]
        send_message(chat_id, "🏆 <b>ТОП по всем редакторским рубрикам</b>\n\n"
                     "Сколько сюжетов вывести?", reply_markup={"inline_keyboard": keyboard})
        return
    try:
        count = max(1, min(int(parts[0]), 100))
    except ValueError:
        send_message(chat_id, "❌ Формат: <code>/toppromt 10 week</code> (периоды: today, 3days, week, month)")
        return
    period = parts[1] if len(parts) > 1 and parts[1] in TOPPROMPT_PERIODS else "today"
    _start_toppromt(chat_id, count, period)


def handle_toppromt_select_period(callback_query: dict, count_str: str):
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], f"ТОП-{count_str}")
    keyboard = [
        [{"text": "📅 Сегодня", "callback_data": f"tp_go:{count_str}:today"},
         {"text": "📅 3 дня", "callback_data": f"tp_go:{count_str}:3days"}],
        [{"text": "📅 Неделя", "callback_data": f"tp_go:{count_str}:week"},
         {"text": "📅 Месяц", "callback_data": f"tp_go:{count_str}:month"}],
        [{"text": "❌ Отмена", "callback_data": "cancel"}],
    ]
    send_message(chat_id, f"🏆 <b>ТОП-{count_str} по всем рубрикам</b>\n\nЗа какой период?",
                 reply_markup={"inline_keyboard": keyboard})


def handle_toppromt_go(callback_query: dict, count_str: str, period: str):
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🚀 Запускаю...")
    _start_toppromt(chat_id, int(count_str), period if period in TOPPROMPT_PERIODS else "today")


def _start_toppromt(chat_id: str, count: int, period: str):
    if chat_id in _TOPPROMPT_RUNNING:
        send_message(chat_id, "⏳ Подборка уже готовится, подождите завершения.")
        return
    _TOPPROMPT_RUNNING.add(chat_id)
    send_message(chat_id,
                 f"⏳ <b>Готовлю ТОП-{count}</b> по всем рубрикам (период: {TOPPROMPT_PERIODS[period]}).\n"
                 f"Это займёт около 2–5 минут, бот пришлёт результат сам.")

    def _worker():
        try:
            _run_toppromt(chat_id, count, period)
        except Exception as e:
            log.exception("❌ /toppromt: %s", e)
            send_message(chat_id, f"❌ Ошибка при сборке ТОПа: {e}")
        finally:
            _TOPPROMPT_RUNNING.discard(chat_id)

    threading.Thread(target=_worker, daemon=True, name="TopPromptWorker").start()


_SUBFIELD_PREFIXES = (
    "пошаговый алгоритм", "алгоритм действий", "алгоритм", "что произошло", "почему это скандал",
    "кто участвует", "где произошло", "когда произошло", "текущий статус",
    "источники", "источник", "ссылки", "ссылка", "кого касается", "кто имеет право",
    "какую проблему решает", "какие документы", "необходимые документы", "документы",
    "сколько стоит", "стоимость", "сколько времени", "куда обращаться", "куда обратиться",
    "ограничения", "подводные камни", "дата проверки", "дата последней проверки",
    "дата события", "дата публикации", "автор", "что говорят", "потенциал для",
    "почему интересно", "краткое содержание", "суть события", "описание", "программа",
    "расписание", "билеты", "возрастные ограничения"
)


def _is_subfield_title(text: str) -> bool:
    """Проверяет, является ли строка служебным подполем сюжета/инструкции (а не новым сюжетом)."""
    t = text.lower().strip("*_ #:\t")
    for p in _SUBFIELD_PREFIXES:
        if t.startswith(p):
            return True
    return False


def _clean_trailing_footer(text: str) -> str:
    """Удаляет завершающий вопрос или служебный футер модели (например, '--- \\n 🍷 Хотите, чтобы я...')."""
    m = re.search(r'\n\s*[-_*]{3,}\s*\n([^\n]+(?:\n[^\n]+)*)\Z', text)
    if m:
        ft = m.group(1).strip()
        if len(ft) < 400 and (any(w in ft.lower() for w in ['хотите', 'обращайтесь', 'дайте знать', 'надеюсь', 'вопросы', 'подборк']) or ft.endswith('?')):
            text = text[:m.start()].strip()
    lines = text.strip().splitlines()
    if lines and len(lines[-1]) < 250:
        last = lines[-1].strip().lower()
        if any(w in last for w in ['хотите, чтобы я', 'дайте знать', 'если вам потребуется', 'обращайтесь']) or (last.startswith('?') or last.endswith('?')):
            text = '\n'.join(lines[:-1]).strip()
    return text.strip()


def _split_inline_title_and_body(raw_after_num: str) -> tuple[str, str]:
    """
    Если после номера следует жирный заголовок и через двоеточие/тире сразу идёт текст описания
    (например: '**Заголовок сюжета**: Подробное описание...'), разделяет их на (title, inline_body).
    """
    s = raw_after_num.strip()
    m = re.match(r'^\*\*(.+?)\*\*\s*(?:[:—–]\s+|\s+-\s+)(.+)$', s)
    if m:
        t_part = m.group(1).strip("*_ #:\t")
        b_part = m.group(2).strip()
        if t_part and not _is_subfield_title(t_part):
            return t_part, b_part
    # Вариант когда открывающие ** были перед номером: '**1. Заголовок** — описание'
    m2 = re.match(r'^([^\*]+?)\*\*\s*(?:[:—–]\s+|\s+-\s+)(.+)$', s)
    if m2:
        t_part = m2.group(1).strip("*_ #:\t")
        b_part = m2.group(2).strip()
        if t_part and not _is_subfield_title(t_part):
            return t_part, b_part
    clean_t = re.sub(r'^(?:Заголовок[:\s*]*|Тема[:\s*]*)', '', s, flags=re.IGNORECASE).strip('*_ #:\t')
    return (clean_t or s.strip('*_ #:\t')), ""


def _parse_rag_news_items(text: str, fallback_links: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Универсально и устойчиво парсит новости/сюжеты/локации из ответа NotebookLM:
    1. Если ответ содержит JSON-массив -> парсит его.
    2. Очищает технические сноски [1, 2], [10-12] и служебные завершающие вопросы модели.
    3. Если в тексте есть 3 и более разделителей --- -> они разделяют целые инструкции/статьи.
    4. Если ответ дан нумерованным списком (1. Title / **1.** Title / **1. Title** / ### 1. Title / 1) Title),
       при этом:
       - даты ("5 ноября 2026...") и возрасты/числа через дефис ("31-летний...") НЕ считаются нумерацией;
       - служебные подполя и внутренние пошаговые списки (Пошаговый алгоритм: 1. ..., 2. ..., 3. ...)
         остаются внутри карточки своего сюжета и НЕ разбивают её на фейковые карточки.
    5. Если ответ разделен разделителями --- и нумерованный список не найден -> берёт разделители.
    6. Если оформлено markdown-заголовками (### Title / ## Title) с фильтрацией подполей.
    7. Если оформлено списком с жирными названиями (* **Title**: body / - **Title** — body).
    8. Fallback: если ничего не распознано по шаблонам, делит текст на карточки по абзацам.
    """
    if not text:
        return []
    clean = clean_rag_citations(text)
    fb_src = fallback_links[0] if fallback_links else ""

    # 1. Пробуем JSON
    m = re.search(r'\[\s*\{.*\}\s*\]', clean, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(0))
            if isinstance(data, list) and data:
                res = []
                for d in data:
                    if isinstance(d, dict) and (d.get("title") or d.get("name")):
                        res.append({
                            "title": clean_rag_citations(str(d.get("title") or d.get("name") or "")).strip("*_ #\t"),
                            "summary": clean_rag_citations(str(d.get("summary") or d.get("description") or "")).strip(),
                            "why": clean_rag_citations(str(d.get("why", ""))).strip(),
                            "source": str(d.get("source", "")).strip() or fb_src,
                            "news_score": float(d.get("score")) if d.get("score") is not None else None
                        })
                if res:
                    return res
        except Exception:
            pass

    # Очищаем служебный футер модели в конце сообщения
    clean = _clean_trailing_footer(clean)

    # Строгий паттерн префикса нумерации заголовка (только '1.', '1)', '№ 1', но НЕ '5 ноября' и НЕ '31-летний')
    title_prefix_re = re.compile(
        r'^(?:#{1,4}\s*)?(?:\*\*\s*)?(?:№\s*\d+\s*[:.)\-—\s]+|\d+\s*[.)]\s*)?(?:\*\*\s*)?(?:Заголовок[:\s*]*|Тема[:\s*]*)?',
        flags=re.IGNORECASE
    )

    # 2. Если в тексте 3 и более разделителей --- -> они разделяют целые инструкции или статьи
    hr_chunks = [c.strip() for c in re.split(r'\n\s*[-_*]{3,}\s*\n|\n---\s*\n?', clean) if c.strip()]
    hr_items = []
    if len(hr_chunks) >= 3:
        for i, chunk in enumerate(hr_chunks):
            # Пропускаем чисто вступительный абзац без деталей
            if i == 0 and len(chunk) < 500 and any(w in chunk.lower() for w in ['подборка', 'обзор', 'период', 'испани', 'список', 'материал', 'рубрик']):
                continue
            if i == len(hr_chunks) - 1 and len(chunk) < 300 and any(w in chunk.lower() for w in ['если вам потребуется', 'обращайтесь', 'дайте знать', 'напишите', 'помощь в подготовке']):
                continue
            clines = [l.strip() for l in chunk.splitlines() if l.strip()]
            if not clines:
                continue

            raw_title = clines[0]
            body_start_idx = 1
            for idx_l, line in enumerate(clines):
                c_line = title_prefix_re.sub('', line).strip('*_ #:\t')
                if c_line and not _is_subfield_title(c_line):
                    raw_title = line
                    body_start_idx = idx_l + 1
                    break

            rest_after_prefix = title_prefix_re.sub('', raw_title).strip()
            t, inline_b = _split_inline_title_and_body(rest_after_prefix)
            body_lines = ([inline_b] if inline_b else []) + clines[body_start_idx:]
            b = '\n'.join(body_lines).strip()
            urls = [
                re.sub(r'Original$', '', u).rstrip('.,;)')
                for u in re.findall(r'https?://[^\s)\]"\'>]+', chunk)
                if not re.search(r'(/rss/?(?:Original)?$|/feed/?$|\.xml$|\.rss$)', u, re.IGNORECASE)
            ]
            hr_items.append({
                "title": t or raw_title[:60],
                "summary": b,
                "why": "",
                "source": urls[0] if urls else "",
                "news_score": None
            })
        if len(hr_items) >= 3:
            return hr_items

    lines = clean.splitlines()

    # 3. Нумерованные пункты с защитой от дат ("5 ноября"), возрастов ("31-летний") и внутренних подшагов ("Пошаговый алгоритм: 1. ..."):
    # Разрешаем только точку или скобку после цифры (1. / 1)), либо явный знак № (№ 1).
    num_pat = re.compile(r'^(?:#{1,4}\s*)?(?:\*\*\s*)?(?:№\s*(\d+)\s*[:.)\-—\s]+|(\d+)\s*[.)])\s*(.+)$')
    emph_pat = re.compile(r'^(?:#{1,4}\s*|\*\*\s*(?:№\s*)?\d+|(?:№\s*)?\d+\s*[.)]\s*\*\*)')

    # Проверяем, использует ли документ жирное/заголовочное выделение для основных пунктов (1. **Title** / ### 1. Title)
    emphasized_count = 0
    for ln in lines:
        sln = ln.strip()
        m_check = num_pat.match(sln)
        if m_check and emph_pat.match(sln):
            cand_check = m_check.group(3).strip("*_ #:\t")
            if not _is_subfield_title(cand_check):
                emphasized_count += 1
    require_emphasis = (emphasized_count >= 2)

    sections = []
    curr_title = None
    curr_body: List[str] = []
    in_sublist = False

    for line in lines:
        sline = line.strip()
        if not sline:
            if curr_title is not None:
                curr_body.append("")
            continue

        indent = len(line) - len(line.lstrip(' \t'))
        match = num_pat.match(sline)
        if match:
            num_val = int(match.group(1) or match.group(2))
            rest = match.group(3).strip()
            cand_title = rest.strip("*_ #:\t")
            has_emphasis = bool(emph_pat.match(sline))

            # Проверяем, не находимся ли мы сразу после заголовка подполя (например, "- **Пошаговый алгоритм**:")
            prev_non_empty = ""
            for prev_l in reversed(curr_body):
                if prev_l.strip():
                    prev_non_empty = prev_l.strip()
                    break
            after_colon_or_subfield = (
                prev_non_empty.endswith(":")
                or _is_subfield_title(prev_non_empty.lstrip("-*• \t"))
            )

            is_substep = False
            if curr_title is not None:
                if _is_subfield_title(cand_title):
                    is_substep = True
                elif require_emphasis and not has_emphasis:
                    # Основные пункты выделены жирным/заголовком, а эта строка — обычный шаг алгоритма
                    is_substep = True
                elif indent >= 2 and not has_emphasis:
                    is_substep = True
                elif in_sublist and not has_emphasis:
                    is_substep = True
                elif after_colon_or_subfield and (num_val == 1 or not has_emphasis):
                    is_substep = True
                elif len([x for x in curr_body if x.strip()]) == 0 and not has_emphasis:
                    # Строка сразу под заголовком без жирного выделения — это текст описания, а не перезапись заголовка
                    is_substep = True

            if is_substep:
                in_sublist = True
                curr_body.append(sline)
            else:
                if curr_title is not None:
                    sections.append((curr_title, "\n".join(curr_body).strip()))
                curr_body = []
                in_sublist = False
                t_part, inline_b = _split_inline_title_and_body(rest)
                curr_title = t_part or cand_title
                if inline_b:
                    curr_body.append(inline_b)
        elif curr_title is not None:
            if sline.startswith(("-", "*", "•")):
                in_sublist = False
            curr_body.append(sline)

    if curr_title is not None:
        sections.append((curr_title, "\n".join(curr_body).strip()))

    if len(sections) >= 2:
        items = []
        for t, b in sections:
            urls = [
                re.sub(r'Original$', '', u).rstrip('.,;)')
                for u in re.findall(r'https?://[^\s)\]"\'>]+', b)
                if not re.search(r'(/rss/?(?:Original)?$|/feed/?$|\.xml$|\.rss$)', u, re.IGNORECASE)
            ]
            items.append({
                "title": t,
                "summary": b,
                "why": "",
                "source": urls[0] if urls else "",
                "news_score": None
            })
        return items

    # 4. Если нумерованный список не найден, но было хотя бы 2 блока по разделителям ---
    if len(hr_items) >= 2:
        return hr_items

    # 5. Markdown-заголовки: ### Title или ## Title (с фильтрацией подполей)
    header_pat = re.compile(r'^(?:#{2,4})\s+(.+)$')
    h_sections = []
    curr_h = None
    curr_hb = []
    for line in lines:
        match = header_pat.match(line.strip())
        if match:
            cand_h = match.group(1).strip("*_ #:\t")
            if _is_subfield_title(cand_h) and curr_h is not None:
                curr_hb.append(line.strip())
            else:
                if curr_h is not None:
                    h_sections.append((curr_h, "\n".join(curr_hb).strip()))
                    curr_hb = []
                clean_h = re.sub(r'^(?:Заголовок[:\s*]*|Тема[:\s*]*)', '', cand_h, flags=re.IGNORECASE).strip('*_ #:\t')
                curr_h = clean_h or cand_h
        elif curr_h is not None:
            curr_hb.append(line.strip())
    if curr_h is not None:
        h_sections.append((curr_h, "\n".join(curr_hb).strip()))

    if len(h_sections) >= 2:
        items = []
        for t, b in h_sections:
            urls = re.findall(r'https?://[^\s)\]"\'>]+', b)
            items.append({
                "title": t,
                "summary": b,
                "why": "",
                "source": urls[0] if urls else fb_src,
                "news_score": None
            })
        return items

    # 6. Маркеры списков с жирными названиями: * **Заголовок** или - **Заголовок**
    bullet_pat = re.compile(r'^[-*•]\s+\*\*(.+?)\*\*(?::\s*|\s*[-—]\s*|\s*)(.*)$')
    b_items = []
    curr_b_title = None
    curr_b_body: List[str] = []
    for line in lines:
        sline = line.strip()
        m = bullet_pat.match(sline)
        if m:
            cand_b = m.group(1).strip("*_ #:\t")
            if not _is_subfield_title(cand_b):
                if curr_b_title is not None:
                    full_b = "\n".join(curr_b_body).strip()
                    urls = re.findall(r'https?://[^\s)\]"\'>]+', full_b)
                    b_items.append({
                        "title": curr_b_title,
                        "summary": full_b,
                        "why": "",
                        "source": urls[0] if urls else fb_src,
                        "news_score": None
                    })
                curr_b_title = cand_b
                curr_b_body = [m.group(2).strip()] if m.group(2).strip() else []
                continue
        if curr_b_title is not None and sline:
            curr_b_body.append(sline)
    if curr_b_title is not None:
        full_b = "\n".join(curr_b_body).strip()
        urls = re.findall(r'https?://[^\s)\]"\'>]+', full_b)
        b_items.append({
            "title": curr_b_title,
            "summary": full_b,
            "why": "",
            "source": urls[0] if urls else fb_src,
            "news_score": None
        })
    if len(b_items) >= 2:
        return b_items

    # 7. Если был ровно 1 нумерованный пункт (len(sections) == 1)
    if len(sections) == 1:
        t, b = sections[0]
        urls = re.findall(r'https?://[^\s)\]"\'>]+', b)
        return [{
            "title": t,
            "summary": b,
            "why": "",
            "source": urls[0] if urls else fb_src,
            "news_score": None
        }]

    # 8. Fallback: разделение на карточки по абзацам (2+ перевода строки)
    raw_paras = [p.strip() for p in re.split(r'\n\s*\n+', clean) if len(p.strip()) > 30]
    if raw_paras:
        if len(raw_paras) > 1 and any(w in raw_paras[0].lower() for w in ['подборка', 'обзор', 'период', 'сводка']):
            raw_paras = raw_paras[1:]
        items = []
        for p in raw_paras:
            plines = [l.strip() for l in p.splitlines() if l.strip()]
            t = plines[0].strip("*_ #:\t") if plines else "Новость"
            if len(t) > 80:
                t = t[:77] + "..."
            b = "\n".join(plines[1:]).strip() if len(plines) > 1 else plines[0]
            urls = re.findall(r'https?://[^\s)\]"\'>]+', p)
            items.append({
                "title": t,
                "summary": b,
                "why": "",
                "source": urls[0] if urls else fb_src,
                "news_score": None
            })
        return items

    return []


def _parse_rag_json(text: str) -> list:
    """Для обратной совместимости."""
    return _parse_rag_news_items(text)


def _send_interactive_news_basket(
    chat_id: str,
    items: List[Dict[str, Any]],
    title: str,
    period_title: str,
    extra_note: str = ""
):
    """
    Отправляет каждую новость отдельным сообщением с inline-кнопкой «➕ Добавить в пост».
    В конце отправляет контрольное сообщение со счётчиком и кнопкой генерации общего ТГ-поста.
    """
    import html as _html
    esc = _html.escape

    session_id = uuid.uuid4().hex[:8]
    session_data = {
        "type": "digest_basket",
        "chat_id": chat_id,
        "title": title,
        "period_title": period_title,
        "items": items,
        "selected": [],
        "item_msg_ids": {},
        "control_msg_id": None
    }
    save_query_session(session_id, session_data)

    header = f"🏆 <b>{esc(title)}</b>\n📅 Период: <b>{esc(period_title)}</b>\n"
    if extra_note:
        header += f"<i>{esc(extra_note)}</i>\n"
    header += f"📊 Найдено новостей: <b>{len(items)}</b>\n\n"
    header += "👇 <i>Отмечайте нужные новости кнопками «➕ Добавить в пост», затем нажмите «Сгенерировать ТГ-пост» внизу:</i>"

    send_message(chat_id, header, disable_web_page_preview=True)
    time.sleep(0.3)

    item_msg_ids = {}
    for idx, it in enumerate(items):
        raw_t = it.get("title", f"Сюжет #{idx+1}").strip()
        t = clean_rag_citations(raw_t).replace("**", "").strip("*_ #\t") or f"Сюжет #{idx+1}"
        s = clean_rag_citations(it.get("summary", "").strip())
        w = clean_rag_citations(it.get("why", "").strip()).replace("**", "")
        src = it.get("source", "").strip()
        sc = it.get("news_score")
        rubrics = it.get("rubrics") or ([it["rubric"]] if "rubric" in it else [])

        block = []
        sc_badge = f" — ⭐ <b>{sc:.1f}</b>" if sc is not None else ""
        block.append(f"<b>{idx+1}. {esc(t)}</b>{sc_badge}")
        if s:
            block.append(markdown_to_telegram_html(s))
        if w:
            block.append(f"💡 <i>{esc(w)}</i>")
        if rubrics:
            from editorial_prompts import EDITORIAL_PROMPTS
            rub_names = [EDITORIAL_PROMPTS[r]["title"].split(" ", 1)[-1] for r in rubrics if r in EDITORIAL_PROMPTS]
            if rub_names:
                block.append(f"🏷 {esc(', '.join(rub_names))}")
        if src and (not s or src not in s):
            block.append(f"🔗 {esc(src)}")

        card_text = "\n\n".join(block)
        if len(card_text) > 3800:
            card_text = card_text[:3800] + "..."

        reply_markup = {
            "inline_keyboard": [
                [{"text": "📝 Сгенерировать ТГ-пост", "callback_data": f"bsk_one:{session_id}:{idx}"}],
                [{"text": "➕ Добавить в сборный дайджест", "callback_data": f"bsk_t:{session_id}:{idx}"}]
            ]
        }
        res = send_message(chat_id, card_text, reply_markup=reply_markup, disable_web_page_preview=True)
        if res and res.get("result"):
            item_msg_ids[str(idx)] = res["result"]["message_id"]
        time.sleep(0.25)

    # Контрольное сообщение внизу со счётчиком
    ctrl_text = (
        "📌 <b>Сборный дайджест: выбрано 0 новостей</b>\n\n"
        "<i>• Нажимайте «📝 Сгенерировать ТГ-пост» под любой новостью для отдельного поста.\n"
        "• Или отмечайте новости кнопкой «➕ Добавить в сборный дайджест» и соберите единый пост ниже:</i>"
    )
    ctrl_markup = {
        "inline_keyboard": [
            [{"text": "📝 Сгенерировать общий дайджест (0)", "callback_data": f"bsk_g:{session_id}"}],
            [{"text": "🗑 Сбросить выбор", "callback_data": f"bsk_c:{session_id}"}]
        ]
    }
    ctrl_res = send_message(chat_id, ctrl_text, reply_markup=ctrl_markup)
    if ctrl_res and ctrl_res.get("result"):
        session_data["control_msg_id"] = ctrl_res["result"]["message_id"]

    session_data["item_msg_ids"] = item_msg_ids
    save_query_session(session_id, session_data)


def handle_basket_toggle(callback_query: dict, session_id: str, idx: int):
    """Переключает статус выбора новости (добавить/убрать)."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    msg_id = callback_query["message"]["message_id"]

    session = get_query_session(session_id)
    if not session or "items" not in session:
        answer_callback_query(callback_query["id"], "❌ Сессия устарела. Запустите выбор заново.", show_alert=True)
        return

    selected = set(session.get("selected", []))
    items = session.get("items", [])
    if idx < 0 or idx >= len(items):
        answer_callback_query(callback_query["id"], "❌ Новость не найдена.")
        return

    item_title = items[idx].get("title", f"Новость #{idx+1}")
    if idx in selected:
        selected.remove(idx)
        btn_text = "➕ Добавить в сборный дайджест"
        alert_text = f"❌ Убрано: {item_title[:30]}"
    else:
        selected.add(idx)
        btn_text = "✅ В сборном дайджесте (убрать)"
        alert_text = f"✅ Добавлено: {item_title[:30]}"

    session["selected"] = sorted(list(selected))
    save_query_session(session_id, session)

    # 1. Обновляем кнопку на сообщении этой новости
    new_markup = {
        "inline_keyboard": [
            [{"text": "📝 Сгенерировать ТГ-пост", "callback_data": f"bsk_one:{session_id}:{idx}"}],
            [{"text": btn_text, "callback_data": f"bsk_t:{session_id}:{idx}"}]
        ]
    }
    try:
        edit_message_reply_markup(chat_id, msg_id, reply_markup=new_markup)
    except Exception as e:
        log.warning("⚠️ edit_message_reply_markup error: %s", e)

    # 2. Обновляем контрольную панель со счётчиком
    ctrl_id = session.get("control_msg_id")
    if ctrl_id:
        count = len(selected)
        ctrl_text = (
            f"📌 <b>Сборный дайджест: выбрано {count}</b>\n\n"
            f"<i>• Нажимайте «📝 Сгенерировать ТГ-пост» под любой новостью для отдельного поста.\n"
            f"• Нажмите кнопку ниже, когда завершите сборку общего дайджеста:</i>"
        )
        ctrl_markup = {
            "inline_keyboard": [
                [{"text": f"📝 Сгенерировать общий дайджест ({count})", "callback_data": f"bsk_g:{session_id}"}],
                [{"text": "🗑 Сбросить выбор", "callback_data": f"bsk_c:{session_id}"}]
            ]
        }
        try:
            edit_message_text(chat_id, ctrl_id, ctrl_text, reply_markup=ctrl_markup)
        except Exception as e:
            log.warning("⚠️ edit_message_text error: %s", e)

    answer_callback_query(callback_query["id"], alert_text)


def handle_basket_clear(callback_query: dict, session_id: str):
    """Сбрасывает все выбранные новости."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    session = get_query_session(session_id)
    if not session:
        answer_callback_query(callback_query["id"], "❌ Сессия устарела.")
        return

    old_selected = list(session.get("selected", []))
    session["selected"] = []
    save_query_session(session_id, session)

    item_msg_ids = session.get("item_msg_ids", {})
    for idx in old_selected:
        m_id = item_msg_ids.get(str(idx))
        if m_id:
            try:
                edit_message_reply_markup(
                    chat_id, m_id,
                    reply_markup={
                        "inline_keyboard": [
                            [{"text": "📝 Сгенерировать ТГ-пост", "callback_data": f"bsk_one:{session_id}:{idx}"}],
                            [{"text": "➕ Добавить в сборный дайджест", "callback_data": f"bsk_t:{session_id}:{idx}"}]
                        ]
                    }
                )
            except Exception:
                pass

    ctrl_id = session.get("control_msg_id")
    if ctrl_id:
        ctrl_text = (
            "📌 <b>Сборный дайджест: выбрано 0</b>\n\n"
            "<i>Выбор сброшен. Отмечайте нужные новости кнопками выше:</i>"
        )
        ctrl_markup = {
            "inline_keyboard": [
                [{"text": "📝 Сгенерировать общий дайджест (0)", "callback_data": f"bsk_g:{session_id}"}],
                [{"text": "🗑 Сбросить выбор", "callback_data": f"bsk_c:{session_id}"}]
            ]
        }
        try:
            edit_message_text(chat_id, ctrl_id, ctrl_text, reply_markup=ctrl_markup)
        except Exception:
            pass

    answer_callback_query(callback_query["id"], "🗑 Выбор сброшен")


def handle_basket_single_item_prompt(callback_query: dict, session_id: str, idx: int):
    """Предлагает выбрать стиль для генерации отдельного ТГ-поста по конкретной новости."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    session = get_query_session(session_id)
    if not session or "items" not in session or idx >= len(session["items"]):
        answer_callback_query(callback_query["id"], "❌ Новость не найдена или сессия истекла", show_alert=True)
        return

    item = session["items"][idx]
    title = item.get("title", f"Новость #{idx+1}")[:60]
    answer_callback_query(callback_query["id"], "🎨 Выберите стиль")

    keyboard = []
    for s_key, s_data in POST_STYLES.items():
        keyboard.append([{"text": s_data['title'], "callback_data": f"bsk_one_s:{session_id}:{idx}:{s_key}"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    send_message(
        chat_id,
        f"📝 <b>Генерация отдельного ТГ-поста:</b>\n"
        f"📰 <i>{title}...</i>\n\n"
        f"✍️ <b>Выберите стиль написания:</b>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_basket_single_item_execute(callback_query: dict, session_id: str, idx: int, style_key: str):
    """Запускает генерацию готового ТГ-поста по одной конкретной новости."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🚀 Генерирую пост...")

    session = get_query_session(session_id)
    if not session or "items" not in session or idx >= len(session["items"]):
        send_message(chat_id, "❌ Новость не найдена или сессия истекла.")
        return

    item = session["items"][idx]
    title = item.get("title", f"Новость #{idx+1}")
    summary = item.get("summary", "")
    why = item.get("why", "")
    source = item.get("source", "")

    # Извлекаем все ссылки на первоисточники
    links = []
    if source and source.strip().startswith("http"):
        links.append(source.strip())
    for u in re.findall(r'https?://[^\s)\]"\'>]+', f"{source} {summary} {why}"):
        clean_u = u.rstrip(").,;")
        if clean_u not in links:
            links.append(clean_u)

    style_cfg = POST_STYLES.get(style_key, POST_STYLES["standard"])
    style_title = style_cfg["title"]

    send_message(
        chat_id,
        f"⏳ <b>Пишу ТГ-пост по новости...</b>\n"
        f"📰 <i>{title[:60]}...</i>\n"
        f"🎨 Стиль: <b>{style_title}</b>\n\n"
        f"Пожалуйста, подождите (5–15 сек)...",
        disable_web_page_preview=True
    )

    def _worker():
        try:
            raw_text = f"Тема: {title}\n\nФакты и детали:\n{summary}"
            if why:
                raw_text += f"\n\nЗначимость: {why}"

            polished = polish_custom_topic_post_with_llm(
                raw_rag_text=raw_text,
                topic=title,
                style_key=style_key,
                extra_links=links,
                length_hint="800-1400",
                max_tokens=1500
            )
            polished, _, _ = sync_initial_headline(polished)

            send_message(
                chat_id,
                f"✅ <b>Готовый ТГ-пост ({style_title}):</b>\n"
                f"<i>(скопируйте текст ниже)</i>",
                disable_web_page_preview=True
            )
            clean_for_copy = strip_headline_options_block(polished)
            post_resp = send_long_message(
                chat_id,
                markdown_to_telegram_html(clean_for_copy),
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            post_msg_id = None
            if post_resp and isinstance(post_resp, dict) and "result" in post_resp:
                post_msg_id = post_resp["result"].get("message_id")

            log.info("✅ ТГ-пост по новости '%s' сгенерирован (длина: %d)", title[:40], len(polished))

            draft_id = save_post_draft(polished, title=title[:60], chat_id=chat_id, post_msg_id=post_msg_id)
            offer_channel_publication(chat_id, draft_id)
        except Exception as e:
            log.exception("❌ Ошибка генерации отдельного ТГ-поста: %s", e)
            send_message(chat_id, f"❌ Ошибка при генерации поста: {e}")

    threading.Thread(target=_worker, daemon=True, name="SingleNewsPostWorker").start()


def handle_basket_generate_prompt(callback_query: dict, session_id: str):
    """Предлагает выбрать стиль перед генерацией общего поста."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    session = get_query_session(session_id)
    if not session or "items" not in session:
        answer_callback_query(callback_query["id"], "❌ Сессия устарела. Запустите поиск заново.", show_alert=True)
        return

    selected = session.get("selected", [])
    if not selected:
        answer_callback_query(callback_query["id"], "⚠️ Сначала отметьте хотя бы одну новость кнопкой «➕ Добавить в пост»!", show_alert=True)
        return

    answer_callback_query(callback_query["id"], "🎨 Выберите стиль")

    keyboard = []
    for s_key, s_data in POST_STYLES.items():
        keyboard.append([{"text": s_data['title'], "callback_data": f"bsk_s:{session_id}:{s_key}"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    send_message(
        chat_id,
        f"📝 <b>Генерация общего ТГ-поста из {len(selected)} выбранных новостей</b>\n\n"
        f"✍️ <b>Выберите стиль написания:</b>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_basket_generate_execute(callback_query: dict, session_id: str, style_key: str):
    """Запускает генерацию единого ТГ-поста из отмеченных новостей."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🚀 Запускаю сборку поста...")

    session = get_query_session(session_id)
    if not session or "items" not in session:
        send_message(chat_id, "❌ Сессия генерации истекла.")
        return

    selected_indices = session.get("selected", [])
    if not selected_indices:
        send_message(chat_id, "⚠️ Нет выбранных новостей для поста.")
        return

    items = session.get("items", [])
    chosen_items = [items[i] for i in selected_indices if i < len(items)]
    style_title = POST_STYLES.get(style_key, {}).get("title", style_key)

    send_message(
        chat_id,
        f"⏳ <b>Пишу ТГ-пост на основе {len(chosen_items)} новостей...</b>\n"
        f"🎨 Стиль: <b>{style_title}</b>\n\n"
        f"Подождите 10-25 секунд..."
    )

    def _worker():
        try:
            _run_basket_post_generation(
                chat_id,
                session_id,
                chosen_items,
                style_key,
                session.get("title", "Дайджест новостей")
            )
        except Exception as e:
            log.exception("❌ Ошибка сборки поста из корзины: %s", e)
            send_message(chat_id, f"❌ Ошибка при генерации поста: {e}")

    threading.Thread(target=_worker, daemon=True, name="BasketPostWorker").start()


def _run_basket_post_generation(
    chat_id: str,
    session_id: str,
    chosen_items: List[Dict[str, Any]],
    style_key: str,
    basket_title: str
):
    """Синтезирует общий пост через AnyModel LLM на основе выбранных новостей."""
    style_cfg = POST_STYLES.get(style_key, POST_STYLES["standard"])
    style_title = style_cfg["title"]
    item_count = len(chosen_items)

    materials = []
    extra_links = []
    for i, it in enumerate(chosen_items, 1):
        t = it.get("title", "").strip()
        s = it.get("summary", "").strip()
        w = it.get("why", "").strip()
        raw_src = it.get("source", "").strip()
        src = sanitize_article_url(raw_src)
        if re.search(r'(/rss/?$|/feed/?$|\.xml$|\.rss$)', src, re.IGNORECASE):
            src = ""
        if src and src not in extra_links:
            extra_links.append(src)
        block = f"СЮЖЕТ {i} из {item_count}: {t}\nСуть события и факты:\n{s}"
        if w:
            block += f"\nКонтекст / почему важно: {w}"
        if src:
            block += f"\nПервоисточник / ссылка: [{extract_source_domain_label(src)}]({src})"
        materials.append(block)

    raw_rag_text = "\n\n".join(materials)
    topic = f"{basket_title} (Обзор всех {item_count} сюжетов)"

    if item_count == 1:
        length_hint = "800-1300"
        max_tokens = 1500
        max_chars = 1600
    elif item_count == 2:
        length_hint = "1100-1600"
        max_tokens = 2000
        max_chars = 1900
    elif item_count == 3:
        length_hint = "1300-1800"
        max_tokens = 2400
        max_chars = 2100
    else:
        min_len = min(1500, item_count * 260)
        max_len = min(2200, item_count * 380)
        length_hint = f"{min_len}-{max_len}"
        max_tokens = 3200
        max_chars = 2400

    polished_post = polish_custom_topic_post_with_llm(
        raw_rag_text=raw_rag_text,
        topic=topic,
        style_key=style_key,
        extra_links=extra_links,
        length_hint=length_hint,
        max_tokens=max_tokens,
        item_count=item_count,
        max_chars=max_chars
    )
    polished_post, _, _ = sync_initial_headline(polished_post)

    clean_post_for_copy = strip_headline_options_block(polished_post)
    send_message(
        chat_id,
        f"✅ <b>Готовый ТГ-пост на основе {item_count} новостей ({style_title}):</b>\n"
        f"<i>(скопируйте текст ниже)</i>",
        disable_web_page_preview=True
    )
    post_resp = send_long_message(
        chat_id,
        markdown_to_telegram_html(clean_post_for_copy),
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    post_msg_id = None
    if post_resp and isinstance(post_resp, dict) and "result" in post_resp:
        post_msg_id = post_resp["result"].get("message_id")

    draft_id = save_post_draft(
        polished_post,
        title=f"{basket_title} ({item_count} сюжетов)",
        chat_id=chat_id,
        post_msg_id=post_msg_id
    )
    offer_channel_publication(chat_id, draft_id)


def _run_toppromt(chat_id: str, count: int, period: str):
    from editorial_prompts import build_structured_prompt, news_keys

    notebook_id = _resolve_notebook_id(_get_latest_notebook_key())
    per_rubric = max(5, min(12, -(-count * 15 // 100) + 3))
    items: List[Dict[str, Any]] = []
    failed: List[str] = []

    rubric_keys = news_keys()
    for n, key in enumerate(rubric_keys, 1):
        raw, _links = query_notebooklm_rag(notebook_id, build_structured_prompt(key, per_rubric),
                                           period_key=period)
        found = _parse_rag_news_items(raw or "", fallback_links=_links)
        log.info("🧭 toppromt рубрика %s (%d/%d): %d сюжетов", key, n, len(rubric_keys), len(found))
        if not raw or (not found and "[]" not in (raw or "")):
            failed.append(key)
        for f in found:
            raw_sc = f.get("news_score") if f.get("news_score") is not None else f.get("score", 5)
            try:
                score = max(1.0, min(10.0, float(raw_sc)))
            except (TypeError, ValueError):
                score = 5.0
            items.append({
                "title": str(f.get("title", "")).strip(),
                "summary": str(f.get("summary", "")).strip(),
                "why": str(f.get("why", "")).strip(),
                "source": str(f.get("source", "")).strip(),
                "rubric": key,
                "news_score": score,
            })

    if not items:
        send_message(chat_id, "❌ RAG не вернул сюжетов за этот период (или требуется <code>notebooklm login</code>).")
        return

    # --- Объединение: кластеризация одного события из разных рубрик ---
    reps: List[Dict[str, Any]] = []
    try:
        from news_diversification import NewsDiversifier, DiversityConfig, diversify
        items.sort(key=lambda x: x["news_score"], reverse=True)
        engine = NewsDiversifier(DiversityConfig.for_target(count))
        labels = engine.cluster_events(items)
        groups: Dict[int, List[Dict[str, Any]]] = {}
        for it, lb in zip(items, labels):
            groups.setdefault(lb, []).append(it)
        for grp in groups.values():
            best = dict(grp[0])  # лучшая по score
            rubrics = sorted({g["rubric"] for g in grp}, key=int)
            best["rubrics"] = rubrics
            # бонус: сюжет попал в несколько рубрик — значит важнее (базовая оценка не теряется)
            best["merged_score"] = round(best["news_score"] + min(1.5, 0.5 * (len(rubrics) - 1)), 2)
            best["news_score"] = best["merged_score"]
            reps.append(best)
        reps.sort(key=lambda x: x["news_score"], reverse=True)
        # Тематическое разнообразие + добор, если нужно
        div = diversify(reps, count, embedder=engine.embedder, max_per_event=1)
        selected = div.selected
        log.info("🧩 toppromt: %d сюжетов -> %d событий -> топ %d", len(items), len(reps), len(selected))
    except Exception as e:
        log.warning("⚠️ toppromt: объединение без кластеризации (%s)", e)
        seen, selected = set(), []
        for it in sorted(items, key=lambda x: x["news_score"], reverse=True):
            k = re.sub(r'\W+', ' ', it["title"].lower()).strip()
            if k not in seen:
                seen.add(k)
                it["rubrics"] = [it["rubric"]]
                selected.append(it)
        selected = selected[:count]

    # --- Интерактивный вывод новостей карточками с кнопками добавления в пост ---
    extra_note = f"Сюжетов собрано: {len(items)}, уникальных событий: {len(reps) or len(selected)}"
    if failed:
        extra_note += f" | Рубрики без ответа: {', '.join(failed)}"

    _send_interactive_news_basket(
        chat_id=chat_id,
        items=selected,
        title=f"ТОП-{len(selected)} по всем рубрикам",
        period_title=TOPPROMPT_PERIODS.get(period, period),
        extra_note=extra_note
    )


def handle_prompts_command(chat_id: str):
    """Показывает 10 редакторских промптов кнопками."""
    from editorial_prompts import EDITORIAL_PROMPTS
    keyboard = [[{"text": p["title"], "callback_data": f"ed_pr:{k}"}]
                for k, p in EDITORIAL_PROMPTS.items()]
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])
    send_message(
        chat_id,
        "🧭 <b>Редакторские промпты</b>\n\nВыберите рубрику, затем период — "
        "RAG подготовит подборку уникальных сюжетов.",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_editorial_select(callback_query: dict, key: str):
    """Выбран редакторский промпт -> создаём RAG-сессию и предлагаем период."""
    from editorial_prompts import EDITORIAL_PROMPTS, build_editorial_prompt
    chat_id = str(callback_query["message"]["chat"]["id"])
    if key not in EDITORIAL_PROMPTS:
        answer_callback_query(callback_query["id"], "❌ Неизвестный промпт")
        return
    answer_callback_query(callback_query["id"], "✅ Промпт выбран")

    p = EDITORIAL_PROMPTS[key]
    session_id = uuid.uuid4().hex[:8]
    save_query_session(session_id, {
        "type": "editorial",
        "question": build_editorial_prompt(key),
        "title": p["title"],
        "chat_id": chat_id
    })
    keyboard = [
        [
            {"text": "📅 За сегодня", "callback_data": f"ask_p:{session_id}:today"},
            {"text": "📅 За 3 дня", "callback_data": f"ask_p:{session_id}:3days"}
        ],
        [
            {"text": "📅 За неделю", "callback_data": f"ask_p:{session_id}:week"},
            {"text": "📅 За месяц", "callback_data": f"ask_p:{session_id}:month"}
        ],
        [{"text": "📓 Выбрать архивный месяц...", "callback_data": f"ask_p:{session_id}:archive"}],
        [{"text": "❌ Отмена", "callback_data": "cancel"}]
    ]
    send_message(
        chat_id,
        f"{p['title']}\n<i>{p['goal']}</i>\n\nЗа какой период сделать сводку?",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_ask_show_archive(callback_query: dict, session_id: str):
    """Показывает кнопки архивных месяцев блокнотов NotebookLM."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "📓 Загружаю архивы...")

    notebooks = _get_available_notebooks_sync()
    if not notebooks:
        send_message(chat_id, "❌ Не найдены блокноты NotebookLM.")
        return

    keyboard = []
    for nb in notebooks[:MAX_NOTEBOOK_MONTHS]:
        short_key = nb['month']
        _save_notebook_mapping(short_key, nb['id'])
        keyboard.append([{"text": f"📓 {nb['month']}", "callback_data": f"ask_nb:{session_id}:{short_key}"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    send_message(
        chat_id,
        "📓 <b>Выберите блокнот архива для поиска:</b>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_ask_execute(
    callback_query: dict,
    session_id: str,
    notebook_key: str,
    period_key: Optional[str] = None
):
    """Выполняет прямой RAG-запрос к выбранному блокноту NotebookLM и возвращает ответ."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🔍 Ищу в NotebookLM...")

    session = get_query_session(session_id)
    if not session or "question" not in session:
        send_message(chat_id, "❌ Сессия запроса истекла. Задайте вопрос заново через <code>/ask</code>.")
        return

    question = session["question"]
    notebook_id = _resolve_notebook_id(notebook_key)

    period_title = RAG_PERIODS.get(period_key, {}).get("title", f"Блокнот {notebook_key}")
    q_preview = session.get("title") or (question[:70] + ("..." if len(question) > 70 else ""))
    send_message(
        chat_id,
        f"⏳ <b>Ищу ответ в NotebookLM RAG...</b>\n"
        f"<i>«{q_preview}»</i>\n"
        f"📅 Период: <b>{period_title}</b> (база: {notebook_key})\n\n"
        f"Пожалуйста, подождите (обычно 5–15 сек)..."
    )

    raw_answer, extra_links = query_notebooklm_rag(notebook_id, question, period_key=period_key)

    if not raw_answer:
        send_message(
            chat_id,
            f"❌ <b>NotebookLM не вернул ответ</b> на вопрос за {period_title}.\n"
            f"Возможно, за указанный период нет материалов в базе или требуется повторить вход: <code>notebooklm login</code>."
        )
        return

    # Очищаем технические скобки [1], [1, 2], [10-12]
    clean_answer = clean_rag_citations(raw_answer)

    # Если это сессия редакторского промпта — выдаём интерактивными карточками с кнопками выбора
    if session.get("type") == "editorial":
        items = _parse_rag_news_items(raw_answer, fallback_links=extra_links)
        if not items:
            items = [{
                "title": session.get("title", "Новость из подборки"),
                "summary": clean_answer,
                "why": "",
                "source": extra_links[0] if extra_links else "",
                "news_score": None
            }]
        log.info("🧩 Редакторская рубрика '%s': сформировано %d интерактивных карточек", session.get("title"), len(items))
        _send_interactive_news_basket(
            chat_id=chat_id,
            items=items,
            title=session.get("title", "Редакторская рубрика"),
            period_title=period_title
        )
        return

    ans_header = f"💡 <b>Ответ NotebookLM RAG ({period_title}):</b>\n\n"
    response_text = ans_header + markdown_to_telegram_html(clean_answer)

    if extra_links:
        import html as _html
        response_text += "\n\n🔗 <b>Найденные источники:</b>\n" + "\n".join(f"• {_html.escape(lk)}" for lk in extra_links[:10])

    send_long_message(chat_id, response_text, parse_mode="HTML")


# =================== ГЕНЕРАЦИЯ ПОСТА ПО ТЕМЕ (/post, /create_post) ===================


def handle_post_command(chat_id: str, args_text: str):
    """
    Обрабатывает команду /post или /create_post.
    Если тема передана -> сразу переходим к выбору стиля.
    Если нет -> переходим в режим ожидания темы.
    """
    topic = args_text.strip()
    if not topic:
        USER_STATES[chat_id] = {"action": "waiting_post_topic"}
        send_message(
            chat_id,
            "📝 <b>Генерация ТГ-поста на вашу тему</b>\n\n"
            "Напишите тему, тезисы или вопрос для поста следующим сообщением.\n"
            "Бот найдёт факты в RAG-системе и оформит готовый пост для Telegram-канала.\n\n"
            "<i>(Для отмены отправьте /cancel)</i>"
        )
        return

    handle_post_topic_input(chat_id, topic)


def handle_post_topic_input(chat_id: str, topic: str):
    """Принимает тему от пользователя и показывает кнопки выбора стиля."""
    session_id = uuid.uuid4().hex[:8]
    save_query_session(session_id, {
        "type": "custom_post",
        "topic": topic,
        "chat_id": chat_id
    })

    keyboard = []
    for s_key, s_data in POST_STYLES.items():
        keyboard.append([{"text": s_data['title'], "callback_data": f"p_sty:{session_id}:{s_key}"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    t_preview = topic[:100] + ("..." if len(topic) > 100 else "")
    send_message(
        chat_id,
        f"📝 <b>Тема будущего поста:</b>\n<i>«{t_preview}»</i>\n\n"
        f"✍️ <b>Выберите стиль написания:</b>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_post_select_period(callback_query: dict, session_id: str, style_key: str):
    """Шаг выбора периода поиска в RAG (сегодня, 3 дня, неделя, месяц, архив)."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🎨 Стиль выбран")

    session = get_query_session(session_id)
    if not session:
        send_message(chat_id, "❌ Сессия генерации истекла. Начните заново с <code>/post</code>.")
        return

    session["style_key"] = style_key
    save_query_session(session_id, session)

    keyboard = [
        [
            {"text": "📅 За сегодня", "callback_data": f"p_p:{session_id}:{style_key}:today"},
            {"text": "📅 За 3 дня", "callback_data": f"p_p:{session_id}:{style_key}:3days"}
        ],
        [
            {"text": "📅 За неделю", "callback_data": f"p_p:{session_id}:{style_key}:week"},
            {"text": "📅 За месяц", "callback_data": f"p_p:{session_id}:{style_key}:month"}
        ],
        [
            {"text": "📓 Выбрать архивный месяц...", "callback_data": f"p_p:{session_id}:{style_key}:archive"}
        ],
        [{"text": "❌ Отмена", "callback_data": "cancel"}]
    ]

    topic = session.get("topic", "")
    t_preview = topic[:70] + ("..." if len(topic) > 70 else "")
    style_title = POST_STYLES.get(style_key, {}).get("title", style_key)

    send_message(
        chat_id,
        f"📝 <b>Генерация поста по теме</b>\n"
        f"🎯 <i>«{t_preview}»</i>\n"
        f"🎨 Стиль: <b>{style_title}</b>\n\n"
        f"За какой период использовать информацию из базы знаний?",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_post_select_notebook(callback_query: dict, session_id: str, style_key: str):
    """Шаг выбора архивного блокнота (если выбран архивный месяц)."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "📓 Загружаю блокноты...")

    notebooks = _get_available_notebooks_sync()
    if not notebooks:
        send_message(chat_id, "❌ Не найдены блокноты NotebookLM.")
        return

    keyboard = []
    for nb in notebooks[:MAX_NOTEBOOK_MONTHS]:
        short_key = nb['month']
        _save_notebook_mapping(short_key, nb['id'])
        keyboard.append([{"text": f"📓 {nb['month']}", "callback_data": f"p_nb:{session_id}:{style_key}:{short_key}"}])
    keyboard.append([{"text": "❌ Отмена", "callback_data": "cancel"}])

    send_message(
        chat_id,
        "📓 <b>Выберите блокнот архива для генерации поста:</b>",
        reply_markup={"inline_keyboard": keyboard}
    )


def handle_post_execute(
    callback_query: dict,
    session_id: str,
    style_key: str,
    notebook_key: str,
    period_key: Optional[str] = None
):
    """Выполняет RAG-поиск по теме и пост-обработку через AnyModel LLM."""
    chat_id = str(callback_query["message"]["chat"]["id"])
    answer_callback_query(callback_query["id"], "🚀 Запускаю генерацию...")

    session = get_query_session(session_id)
    if not session or "topic" not in session:
        send_message(chat_id, "❌ Сессия генерации истекла. Начните заново с <code>/post</code>.")
        return

    topic = session["topic"]
    notebook_id = _resolve_notebook_id(notebook_key)
    style_title = POST_STYLES.get(style_key, {}).get("title", style_key)
    period_title = RAG_PERIODS.get(period_key, {}).get("title", f"Блокнот {notebook_key}")
    t_preview = topic[:60] + ("..." if len(topic) > 60 else "")

    send_message(
        chat_id,
        f"⏳ <b>Генерирую ТГ-пост по вашей теме...</b>\n"
        f"🎯 <i>«{t_preview}»</i>\n"
        f"🎨 Стиль: <b>{style_title}</b>\n"
        f"📅 Период: <b>{period_title}</b>\n\n"
        f"1️⃣ Поиск фактов и ссылок в NotebookLM RAG...",
        disable_web_page_preview=True
    )

    rag_prompt = f"""Найди всю ключевую информацию, факты, цифры, даты, контекст, цитаты и ссылки по следующей теме для русскоязычного Telegram-канала о жизни в Испании:

Тема: {topic}

Требования:
- Опирайся на материалы и статьи в источниках блокнота.
- Укажи конкретные цифры, законы, даты и ссылки на первоисточники/официальные порталы, если они есть.
- Ответ на русском языке, подробно (1000-2500 символов)."""

    raw_answer, extra_links = query_notebooklm_rag(notebook_id, rag_prompt, period_key=period_key)

    links_found_count = len(extra_links)
    rag_len = len(raw_answer) if raw_answer else 0
    send_message(
        chat_id,
        f"✅ RAG-ответ получен ({rag_len} симв., ссылок: {links_found_count})\n"
        f"2️⃣ Полирую пост через AnyModel ({style_title})...",
        disable_web_page_preview=True
    )

    polished_post = polish_custom_topic_post_with_llm(
        raw_rag_text=raw_answer or "",
        topic=topic,
        style_key=style_key,
        extra_links=extra_links
    )
    polished_post, _, _ = sync_initial_headline(polished_post)

    send_message(
        chat_id,
        f"✅ <b>Готовый ТГ-пост ({style_title}):</b>\n"
        f"<i>(скопируйте текст ниже)</i>",
        disable_web_page_preview=True
    )

    # Отправляем отформатированный текст поста для копирования (без сырых **)
    clean_for_copy = strip_headline_options_block(polished_post)
    post_resp = send_long_message(
        chat_id,
        markdown_to_telegram_html(clean_for_copy),
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    post_msg_id = None
    if post_resp and isinstance(post_resp, dict) and "result" in post_resp:
        post_msg_id = post_resp["result"].get("message_id")

    log.info("✅ ТГ-пост по теме '%s' успешно сгенерирован (стиль: %s, длина: %d)",
             topic[:40], style_key, len(polished_post))

    # Предлагаем публикацию в канал @iberia_news (в очередь 8ч или срочно)
    draft_id = save_post_draft(polished_post, title=topic[:60], chat_id=chat_id, post_msg_id=post_msg_id)
    offer_channel_publication(chat_id, draft_id)


# =================== LONG-POLLING ===================


def run_polling():
    """Запускает long-polling для обработки обновлений Telegram."""
    if not BOT_TOKEN:
        log.error("❌ Не задан TELEGA_BOT в .env. Бот не может запуститься.")
        sys.exit(1)

    log.info("🤖 Telegram-бот запущен (long-polling)...")
    log.info("📋 Слушаю callback_query для генерации ТГ-постов через NotebookLM RAG")
    log.info("📋 Команды: /ask, /post, /top [N] [date], /queue, /status, /cancel, /help")

    # Регистрируем меню команд в клиенте Telegram
    try:
        tg_api("setMyCommands", commands=[
            {"command": "ask", "description": "Задать вопрос RAG-системе (NotebookLM)"},
            {"command": "post", "description": "Сгенерировать ТГ-пост на свою тему"},
            {"command": "top", "description": "Показать ТОП новостей (кнопки или /top 20)"},
            {"command": "prompts", "description": "редакторские рубрики (подборка из RAG)"},
            {"command": "toppromt", "description": "ТОП-N по всем рубрикам вместе"},
            {"command": "queue", "description": "Очередь публикаций в канал @iberia_news"},
            {"command": "fav", "description": "Избранные посты (отложенные на потом)"},
            {"command": "help", "description": "Справка и список команд"},
            {"command": "status", "description": "Статус бота, блокноты NotebookLM и LLM"},
            {"command": "cancel", "description": "Отменить текущий ввод"},
            {"command": "start", "description": "Начало работы"}
        ])
    except Exception as e:
        log.warning("⚠️ Не удалось зарегистрировать setMyCommands: %s", e)

    # Запуск фонового воркера очереди публикаций в канал
    try:
        worker_thread = threading.Thread(target=queue_worker_thread_func, daemon=True, name="QueuePublisherWorker")
        worker_thread.start()
        log.info("🚀 Фоновый планировщик публикаций в канал запущен")
    except Exception as e:
        log.error("❌ Не удалось запустить планировщик публикаций: %s", e)

    offset = 0
    consecutive_errors = 0
    max_consecutive_errors = 10

    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={
                    "offset": offset,
                    "timeout": 30,
                    "allowed_updates": json.dumps(["callback_query", "message"])
                },
                timeout=35
            )

            if resp.status_code != 200:
                log.warning("⚠️ getUpdates ошибка (%d): %s", resp.status_code, resp.text[:200])
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    log.error("❌ Слишком много ошибок подряд (%d). Остановка.", consecutive_errors)
                    break
                time.sleep(min(consecutive_errors * 2, 30))
                continue

            data = resp.json()
            updates = data.get("result", [])
            consecutive_errors = 0

            for update in updates:
                offset = update["update_id"] + 1

                # Обработка callback_query (нажатия inline-кнопок)
                if "callback_query" in update:
                    try:
                        process_callback_query(update["callback_query"])
                    except Exception as e:
                        log.error("❌ Ошибка обработки callback_query: %s", e, exc_info=True)

                # Обработка текстовых команд
                elif "message" in update:
                    msg = update["message"]
                    text = msg.get("text", "")
                    chat_id = str(msg["chat"]["id"])

                    try:
                        _handle_text_command(chat_id, text)
                    except Exception as e:
                        log.error("❌ Ошибка обработки команды '%s': %s", text, e, exc_info=True)

        except requests.exceptions.Timeout:
            continue
        except requests.exceptions.ConnectionError:
            log.warning("⚠️ Ошибка соединения. Повтор через 5 сек...")
            time.sleep(5)
        except KeyboardInterrupt:
            log.info("🛑 Бот остановлен (Ctrl+C)")
            break
        except Exception as e:
            log.error("❌ Неожиданная ошибка polling: %s", e, exc_info=True)
            time.sleep(5)


def _handle_text_command(chat_id: str, text: str):
    """Маршрутизирует текстовые команды и сообщения бота."""
    if not text:
        return

    text_clean = text.strip()

    # 1. Проверка состояния пользователя (FSM)
    current_state = USER_STATES.get(chat_id, {})
    action = current_state.get("action")

    if text_clean.startswith("/cancel"):
        if action:
            USER_STATES.pop(chat_id, None)
            send_message(chat_id, "❌ Действие отменено.")
        else:
            send_message(chat_id, "ℹ️ Нет активных действий для отмены.")
        return

    if action == "waiting_ask_question" and not text_clean.startswith("/"):
        USER_STATES.pop(chat_id, None)
        handle_ask_question_input(chat_id, text_clean)
        return

    if action == "waiting_post_topic" and not text_clean.startswith("/"):
        USER_STATES.pop(chat_id, None)
        handle_post_topic_input(chat_id, text_clean)
        return

    # Если была активная сессия ввода, но пользователь прислал новую команду со слэшем
    if action and text_clean.startswith("/"):
        USER_STATES.pop(chat_id, None)

    # 2. Справка
    if text_clean in ("/start", "/help"):
        send_message(chat_id,
                     "🤖 <b>News Intelligence Bot — Справка</b>\n\n"
                     "Я помогаю работать с новостями Испании, отвечать на вопросы через RAG и генерировать готовые Telegram-посты.\n\n"
                     "📌 <b>Основные команды:</b>\n"
                     "• <code>/toppromt [N] [период]</code> — ТОП-N сюжетов, объединённый из всех рубрик\n"
                     "• <code>/prompts</code> — редакторские рубрики (подборка за день/3 дня/неделю/месяц)\n"
                     "• <code>/ask [вопрос]</code> — задать вопрос напрямую RAG-системе (NotebookLM)\n"
                     "• <code>/post [тема]</code> — сгенерировать ТГ-пост на вашу тему с фактами из RAG\n"
                     "• <code>/top</code> — интерактивный выбор новостей кнопками (кол-во + период)\n"
                     "• <code>/top 20</code> — 20 лучших новостей за сегодня\n"
                     "• <code>/top 15 yesterday</code> — 15 лучших за вчера\n"
                     "• <code>/queue</code> — очередь отложенных публикаций в канал @iberia_news\n"
                     "• <code>/fav</code> — избранные посты (отложенные на потом)\n"
                     "• <code>/status</code> — статус бота, подключённые блокноты NotebookLM и LLM\n"
                     "• <code>/cancel</code> — отмена текущего ввода\n"
                     "• <code>/help</code> — это справочное меню\n\n"
                     "💡 <b>Способы создания постов:</b>\n"
                     "1. <b>По любой вашей теме:</b> напишите <code>/post</code> или <code>/post Тема поста</code>, выберите стиль и блокнот.\n"
                     "2. <b>По найденным новостям:</b> нажмите кнопку <b>«📝 Сгенерировать ТГ-пост»</b> под любой новостью из <code>/top</code>.")
        return

    # 2b. Редакторские промпты (/toppromt должен проверяться раньше /prompts)
    if text_clean.startswith("/toppromt"):
        handle_toppromt_command(chat_id, text_clean[len("/toppromt"):].strip())
        return

    if text_clean.startswith("/prompts"):
        handle_prompts_command(chat_id)
        return

    # 3. Вопрос к RAG
    if text_clean.startswith("/ask") or text_clean.startswith("/rag"):
        prefix = "/ask" if text_clean.startswith("/ask") else "/rag"
        args_text = text_clean[len(prefix):].strip()
        handle_ask_command(chat_id, args_text)
        return

    # 4. Пост по теме
    if text_clean.startswith("/post") or text_clean.startswith("/create_post"):
        prefix = "/post" if text_clean.startswith("/post") else "/create_post"
        args_text = text_clean[len(prefix):].strip()
        handle_post_command(chat_id, args_text)
        return

    # 5. ТОП новостей
    if text_clean.startswith("/top"):
        args_text = text_clean[4:].strip()  # убираем '/top'
        handle_top_command(chat_id, args_text)
        return

    # 6. Очередь публикаций в канал
    if text_clean in ("/queue", "/q"):
        handle_queue_command(chat_id)
        return

    # 7. Избранные посты (на потом)
    if text_clean in ("/fav", "/favorites"):
        handle_favorites_command(chat_id)
        return

    if text == "/status":
        notebooks = _get_available_notebooks_sync()
        nb_list = "\n".join(f"  📓 {nb['month']} — {nb['title']}" for nb in notebooks) \
            if notebooks else "  ❌ Не найдены"

        # Проверка контекста
        ctx_count = 0
        if os.path.exists(ARTICLES_CONTEXT_FILE):
            try:
                with open(ARTICLES_CONTEXT_FILE, 'r', encoding='utf-8') as f:
                    ctx = json.load(f)
                ctx_count = len(ctx)
            except Exception:
                pass

        # Проверка БД
        db_scored = 0
        if os.path.exists(DB_FILE):
            try:
                conn = sqlite3.connect(DB_FILE)
                db_scored = conn.execute("SELECT COUNT(*) FROM news_scores").fetchone()[0]
                conn.close()
            except Exception:
                pass

        # Очередь публикаций и избранное
        q_data = load_queue_data()
        pending_q = [p for p in q_data.get("queue", []) if p.get("status") == "pending"]
        fav_count = len(q_data.get("favorites", []))
        last_pub = q_data.get("last_published_at")
        last_pub_str = "нет"
        if last_pub:
            try:
                last_pub_str = datetime.fromisoformat(last_pub).strftime('%d.%m %H:%M UTC')
            except Exception:
                pass

        channel_link = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}" if TELEGRAM_CHANNEL.startswith("@") else ""
        link_html = f"<a href='{channel_link}'>{TELEGRAM_CHANNEL}</a>" if channel_link else f"<b>{TELEGRAM_CHANNEL}</b>"

        send_message(chat_id,
                     f"📊 <b>Статус бота:</b>\n\n"
                     f"📓 <b>Доступные ноутбуки:</b>\n{nb_list}\n\n"
                     f"📰 Статей в контексте: <b>{ctx_count}</b>\n"
                     f"🗄 Оценено в БД: <b>{db_scored}</b>\n"
                     f"📢 Канал: {link_html} (интервал: {PUBLISH_INTERVAL_HOURS}ч)\n"
                     f"📦 В очереди: <b>{len(pending_q)}</b> постов (посл. публикация: {last_pub_str})\n"
                     f"⭐ В избранном: <b>{fav_count}</b> постов (отложены на потом)\n"
                     f"🤖 LLM: <code>{ANYMODEL_MODEL}</code>\n"
                     f"✅ Бот работает")
        return


# =================== CLI ===================


def main():
    parser = argparse.ArgumentParser(
        description="Telegram-бот для генерации ТГ-постов через NotebookLM RAG"
    )
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Расширенное логирование")
    parser.add_argument("--test-notebooks", action="store_true",
                        help="Только показать доступные ноутбуки и выйти")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.test_notebooks:
        print("📓 Проверка доступных ноутбуков NotebookLM...\n")
        notebooks = _get_available_notebooks_sync()
        if notebooks:
            for nb in notebooks:
                print(f"  📓 {nb['month']} — {nb['title']} (ID: {nb['id'][:20]}...)")
        else:
            print("  ❌ Ноутбуки не найдены. Проверьте авторизацию: notebooklm login")
        return

    run_polling()


if __name__ == '__main__':
    main()
