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
import sqlite3
import asyncio
import logging
import argparse
from datetime import datetime, timedelta, date, timezone
from typing import Optional, Dict, Any, List

import requests

# =================== КОНФИГУРАЦИЯ ===================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'telegram_bot.log')
ARTICLES_CONTEXT_FILE = os.path.join(SCRIPT_DIR, '.tg_bot_articles.json')
DB_FILE = os.path.join(SCRIPT_DIR, 'baza.db')

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

# Максимальная глубина выбора ноутбуков (месяцев назад)
MAX_NOTEBOOK_MONTHS = 4

# =================== TELEGRAM API (ЧИСТЫЙ requests) ===================


def tg_api(method: str, **kwargs) -> Optional[dict]:
    """Вызывает метод Telegram Bot API."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=kwargs, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
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


def send_message(chat_id: str, text: str, parse_mode: str = "HTML",
                 reply_markup: Optional[dict] = None,
                 disable_web_page_preview: bool = False) -> Optional[dict]:
    """Отправляет сообщение в Telegram."""
    params = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup:
        params["reply_markup"] = reply_markup
    return tg_api("sendMessage", **params)


def edit_message_text(chat_id: str, message_id: int, text: str,
                      parse_mode: str = "HTML",
                      reply_markup: Optional[dict] = None):
    """Редактирует текст существующего сообщения."""
    params = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        params["reply_markup"] = reply_markup
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


def query_notebooklm_rag(notebook_id: str, prompt: str) -> tuple[Optional[str], list[str]]:
    """
    Отправляет RAG-запрос в NotebookLM и возвращает (ответ, список_ссылок_источников).
    Синхронная обертка.
    """
    return asyncio.run(_query_rag_async(notebook_id, prompt))


async def _query_rag_async(notebook_id: str, prompt: str) -> tuple[Optional[str], list[str]]:
    """Отправляет RAG-запрос в NotebookLM через chat.ask(), возвращает ответ и найденные ссылки."""
    try:
        from notebooklm import NotebookLMClient
    except ImportError:
        log.error("❌ notebooklm-py не установлен")
        return None, []

    try:
        async with NotebookLMClient.from_storage() as client:
            result = await client.chat.ask(
                notebook_id=notebook_id,
                question=prompt,
            )
            if not result or not result.answer:
                log.warning("⚠️ NotebookLM вернул пустой ответ")
                return None, []

            log.info("✅ RAG ответ получен (%d символов)", len(result.answer))

            # Извлекаем ссылки или названия источников из references
            cited_links = []
            if hasattr(result, "references") and result.references:
                for ref in result.references:
                    # Если в тексте цитаты или атрибутах есть URL
                    cited_text = getattr(ref, "cited_text", "") or ""
                    found_urls = re.findall(r'https?://[^\s)\]"\'>]+', cited_text)
                    for u in found_urls:
                        if u not in cited_links:
                            cited_links.append(u)

            return result.answer, cited_links
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


def polish_post_with_llm(
    raw_rag_text: str,
    article_context: Dict[str, Any],
    style_key: str = "standard",
    chosen_angle: Optional[str] = None,
    extra_links: Optional[List[str]] = None
) -> str:
    """
    Прогоняет сырой RAG-ответ через AnyModel LLM для финальной полировки.

    Задачи:
    - Учесть выбранный стиль (POST_STYLES) и угол/идею публикации
    - Убрать квадратные скобки [N] из цитирований
    - Сохранить и органично встроить ВСЕ оригинальные ссылки (исходная статья + ссылки из RAG)
    - Подогнать длину ровно под 1 ТГ-пост (800-1500 символов)
    - Оформить в стиле ТГ-канала (эмодзи, форматирование, хэштеги)
    """
    if not ANYMODEL_KEY:
        log.warning("⚠️ ANYMODEL_KEY не задан. Возвращаю RAG-текст без пост-обработки.")
        return _basic_cleanup(raw_rag_text, article_context, extra_links)

    original_url = article_context.get("url", "")
    title = article_context.get("title", "")
    categories = article_context.get("categories", [])
    hashtags = " ".join(f"#{c}" for c in categories[:5]) if categories else "#испания #новости"

    style_cfg = POST_STYLES.get(style_key, POST_STYLES["standard"])
    style_instruction = style_cfg["prompt"]

    # Собираем все уникальные ссылки
    all_links = []
    if original_url:
        all_links.append(original_url)
    if extra_links:
        for lk in extra_links:
            if lk and lk not in all_links:
                all_links.append(lk)

    links_text = "\n".join(f"- {lk}" for lk in all_links) if all_links else "(нет ссылок)"

    angle_instruction = f"Фокус публикации (выбранная идея): {chosen_angle}" if chosen_angle else ""

    system_prompt = f"""Ты — главный редактор русскоязычного Telegram-канала о жизни и новостях в Испании.
Твоя задача — взять сырой материал от RAG-системы (NotebookLM) и превратить его в идеальный, готовый к публикации ТГ-пост.

{style_instruction}
{angle_instruction}

СТРОГИЕ ПРАВИЛА:
1. Пиши ТОЛЬКО на русском языке.
2. КАТЕГОРИЧЕСКИ УДАЛИ все квадратные скобки вида [1], [2], [N], [source 1] — это технические маркеры цитирования RAG. Никаких скобок в тексте!
3. ОБЯЗАТЕЛЬНО сохрани и органично добавь в пост оригинальные ссылки на первоисточники (список дан ниже). Ссылки должны быть кликабельными URL в конце поста или в контексте текста.
4. Длина поста — СТРОГО 800-1500 символов. Никаких огромных простыней! Текст должен помещаться ровно в 1 сообщение Telegram.
5. Используй продуманные эмодзи для структуры (📌, 💡, ⚠️, 📊, 🏠, 💰 и т.д.).
6. В самом конце обязательно добавь 3-5 релевантных хэштегов.
7. НЕ используй HTML теги. Выдавай чистый отформатированный текст с переносами строк и эмодзи, готовый к мгновенному копированию."""

    user_prompt = f"""Сырой текст от RAG-системы:
---
{raw_rag_text}
---

Данные статьи:
- Тема: {title}
- Выбранный угол/идея: {chosen_angle or "Общий обзор"}
- Ссылки на оригинальные статьи и источники:
{links_text}
- Рекомендуемые хэштеги: {hashtags}

Напиши готовый ТГ-пост в стиле «{style_cfg['title']}».
Требования: ровно 800-1500 символов, без квадратных скобок, с ссылками на источники в конце."""

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
        "max_tokens": 1500,
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
            # Двойная страховка: убираем любые оставшиеся квадратные скобки с цифрами
            polished = re.sub(r'\[\d+\]', '', polished)
            log.info("✅ Пост отполирован через AnyModel (%d символов, стиль: %s)", len(polished), style_key)
            return polished.strip()

    except Exception as e:
        log.warning("⚠️ Ошибка AnyModel пост-обработки: %s", e)

    return _basic_cleanup(raw_rag_text, article_context, extra_links)


def _basic_cleanup(text: str, article_context: Dict[str, Any], extra_links: Optional[List[str]] = None) -> str:
    """Базовая чистка текста без LLM (fallback)."""
    # Удаляем [N] цитирования
    cleaned = re.sub(r'\[\d+\]', '', text)
    cleaned = re.sub(r'\[source\s*\d+\]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'  +', ' ', cleaned)

    # Добавляем ссылки на оригиналы
    url = article_context.get("url", "")
    all_links = []
    if url:
        all_links.append(url)
    if extra_links:
        for lk in extra_links:
            if lk and lk not in all_links:
                all_links.append(lk)

    if all_links:
        cleaned += "\n\n🔗 Источники:\n" + "\n".join(f"• {lk}" for lk in all_links)

    # Добавляем хэштеги
    categories = article_context.get("categories", [])
    if categories:
        hashtags = " ".join(f"#{c}" for c in categories[:5])
        cleaned += f"\n\n{hashtags}"

    return cleaned.strip()


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

    # Шаг 3: Отправляем готовый пост
    send_message(chat_id,
                 f"✅ <b>Готовый ТГ-пост ({style_title}):</b>\n"
                 f"<i>(скопируйте текст ниже)</i>",
                 disable_web_page_preview=True)

    # Отправляем пост чистым текстом (для копирования)
    tg_api("sendMessage",
           chat_id=chat_id,
           text=polished_post,
           disable_web_page_preview=True)

    log.info("✅ ТГ-пост сгенерирован для статьи '%s' (длина: %d симв, стиль: %s)",
             title, len(polished_post), style_key)


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
        escape_html
    )

    send_message(chat_id,
                 f"⏳ <b>Загружаю ТОП-{count} статей</b> (с {date_label})...",
                 disable_web_page_preview=True)

    articles = load_scored_articles_from_db(
        since_date=since_date,
        top_n=count,
        min_score=0.0  # берём все, фильтрация по кол-ву
    )

    if not articles:
        send_message(chat_id,
                     f"❌ Нет оценённых статей за период <b>{date_label}</b>.\n"
                     f"Запустите скоринг: <code>python3 scorer.py --today</code>")
        return

    # Сортируем и берём ТОП
    articles.sort(key=lambda x: x.news_score, reverse=True)
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


# =================== LONG-POLLING ===================


def run_polling():
    """Запускает long-polling для обработки обновлений Telegram."""
    if not BOT_TOKEN:
        log.error("❌ Не задан TELEGA_BOT в .env. Бот не может запуститься.")
        sys.exit(1)

    log.info("🤖 Telegram-бот запущен (long-polling)...")
    log.info("📋 Слушаю callback_query для генерации ТГ-постов через NotebookLM RAG")
    log.info("📋 Команды: /top [N] [date] — отправить ТОП статей, /help — справка")

    # Регистрируем меню команд в клиенте Telegram
    try:
        tg_api("setMyCommands", commands=[
            {"command": "top", "description": "Показать ТОП новостей (кнопки или /top 20)"},
            {"command": "help", "description": "Справка и список команд"},
            {"command": "status", "description": "Статус бота, блокноты NotebookLM и LLM"},
            {"command": "start", "description": "Начало работы"}
        ])
    except Exception as e:
        log.warning("⚠️ Не удалось зарегистрировать setMyCommands: %s", e)

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
    """Маршрутизирует текстовые команды бота."""
    if not text:
        return

    if text in ("/start", "/help"):
        send_message(chat_id,
                     "🤖 <b>News Intelligence Bot — Справка</b>\n\n"
                     "Я помогаю работать с оценёнными новостями Испании и генерировать готовые Telegram-посты.\n\n"
                     "📌 <b>Основные команды:</b>\n"
                     "• <code>/top</code> — интерактивный выбор кнопками (кол-во + период)\n"
                     "• <code>/top 20</code> — прислать 20 лучших новостей за сегодня\n"
                     "• <code>/top 20 today</code> — аналогично\n"
                     "• <code>/top 15 yesterday</code> — 15 лучших за вчера\n"
                     "• <code>/top 10 3days</code> — 10 лучших за последние 3 дня\n"
                     "• <code>/top 25 week</code> — 25 лучших за неделю\n"
                     "• <code>/top 10 2026-09-02</code> — за конкретную дату YYYY-MM-DD\n"
                     "• <code>/status</code> — статус бота, подключённые блокноты NotebookLM и LLM\n"
                     "• <code>/help</code> — это справочное меню\n\n"
                     "💡 <b>Генерация постов:</b>\n"
                     "Под каждым сообщением есть кнопка <b>«📝 Сгенерировать ТГ-пост»</b>.\n"
                     "1. Нажмите кнопку под интересующей новостью\n"
                     "2. Выберите месяц блокнота NotebookLM (до 4 месяцев назад)\n"
                     "3. Бот сделает RAG-запрос, отполирует через AnyModel LLM и пришлёт чистый пост без [N], подогнанный по длине с оригинальными ссылками!")
        return

    if text.startswith("/top"):
        args_text = text[4:].strip()  # убираем '/top'
        handle_top_command(chat_id, args_text)
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

        send_message(chat_id,
                     f"📊 <b>Статус бота:</b>\n\n"
                     f"📓 <b>Доступные ноутбуки:</b>\n{nb_list}\n\n"
                     f"📰 Статей в контексте: <b>{ctx_count}</b>\n"
                     f"🗄 Оценено в БД: <b>{db_scored}</b>\n"
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
