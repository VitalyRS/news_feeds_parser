"""
Модуль отправки отобранных и оцененных новостей в Telegram бот.
Использует токен из переменной окружения TELEGA_BOT (или TELEGRAM_BOT_TOKEN).

Добавлена поддержка inline-кнопок для генерации ТГ-постов через NotebookLM RAG:
- Каждое сообщение со скоринговой статьёй получает кнопку "📝 Сгенерировать ТГ-пост"
- Контекст статей сохраняется в .tg_bot_articles.json для обработки telegram_bot.py
"""

import os
import json
import time
import logging
import requests
from typing import List, Optional, Dict, Any

log = logging.getLogger("telegram_notifier")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ARTICLES_CONTEXT_FILE = os.path.join(SCRIPT_DIR, '.tg_bot_articles.json')


def escape_html(text: str) -> str:
    """Экранирует спецсимволы для parse_mode='HTML' в Telegram."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def auto_detect_chat_id(token: str) -> Optional[str]:
    """
    Пытается автоматически обнаружить chat_id через getUpdates,
    если пользователь написал боту /start или любое сообщение.
    """
    try:
        url = f"https://api.telegram.org/bot{token}/getUpdates"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("result", [])
            if results:
                last_update = results[-1]
                msg = last_update.get("message") or last_update.get("channel_post") or last_update.get("my_chat_member")
                if msg and "chat" in msg:
                    chat_id = str(msg["chat"]["id"])
                    chat_title = msg["chat"].get("title") or msg["chat"].get("username") or msg["chat"].get("first_name")
                    log.info("📲 Автоматически обнаружен chat_id: %s (%s)", chat_id, chat_title)
                    return chat_id
    except Exception as e:
        log.warning("⚠️ Не удалось автоматически определить chat_id: %s", e)
    return None


def send_telegram_message(
    token: str,
    chat_id: str,
    text: str,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = False
) -> bool:
    """Отправляет одно текстовое сообщение в Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        else:
            log.error("❌ Ошибка отправки в Telegram (%d): %s", resp.status_code, resp.text)
            return False
    except Exception as e:
        log.error("❌ Ошибка соединения с Telegram API: %s", e)
        return False


def send_telegram_message_with_buttons(
    token: str,
    chat_id: str,
    text: str,
    inline_keyboard: List[List[Dict[str, str]]],
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = False
) -> bool:
    """Отправляет текстовое сообщение с inline-кнопками в Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
        "reply_markup": json.dumps({
            "inline_keyboard": inline_keyboard
        })
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        else:
            log.error("❌ Ошибка отправки в Telegram с кнопками (%d): %s", resp.status_code, resp.text)
            return False
    except Exception as e:
        log.error("❌ Ошибка соединения с Telegram API: %s", e)
        return False


def _save_article_context(scored_article: Any) -> None:
    """Сохраняет контекст статьи в JSON-файл для обработки telegram_bot.py."""
    # Загружаем существующий файл или создаём новый
    context = {}
    if os.path.exists(ARTICLES_CONTEXT_FILE):
        try:
            with open(ARTICLES_CONTEXT_FILE, 'r', encoding='utf-8') as f:
                context = json.load(f)
        except (json.JSONDecodeError, IOError):
            context = {}

    article_id = str(scored_article.id or hash(scored_article.title))
    ed = scored_article.editorial

    context[article_id] = {
        "title": scored_article.title,
        "url": scored_article.url or "",
        "source": scored_article.source or "",
        "published_at": scored_article.published_at or "",
        "news_score": scored_article.news_score,
        "tier": scored_article.tier,
        "categories": ed.categories,
        "audience": ed.audience,
        "formats": ed.formats,
        "editorial_angles": ed.editorial_angles,
        "why": ed.why,
        "scores": {
            "practical_value": ed.scores.practical_value,
            "resident_relevance": ed.scores.resident_relevance,
            "content_potential": ed.scores.content_potential,
        },
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # Храним максимум 200 последних статей, чтобы файл не рос бесконечно
    if len(context) > 200:
        sorted_keys = sorted(context.keys(),
                             key=lambda k: context[k].get("saved_at", ""), reverse=True)
        context = {k: context[k] for k in sorted_keys[:200]}

    try:
        with open(ARTICLES_CONTEXT_FILE, 'w', encoding='utf-8') as f:
            json.dump(context, f, ensure_ascii=False, indent=2)
    except IOError as e:
        log.warning("⚠️ Не удалось сохранить контекст статьи: %s", e)


def format_article_telegram(scored_article: Any) -> str:
    """
    Форматирует одну оцененную статью в красивый HTML пост для Telegram.
    """
    ed = scored_article.editorial
    scores = ed.scores
    title = escape_html(scored_article.title)
    url = scored_article.url or ""
    source = escape_html(scored_article.source or "Источник")
    
    # Эмодзи по тиру
    tier_emoji = {
        "🔥 TOP": "🔥",
        "🟠 HIGH": "🟠",
        "🟡 GOOD": "🟡",
        "⚪ MAYBE": "⚪",
        "❌ SKIP": "❌"
    }.get(scored_article.tier, "📰")

    categories = " ".join(f"#{c}" for c in ed.categories) if ed.categories else "#новости #испания"
    
    lines = [
        f"{tier_emoji} <b>[{scored_article.news_score:.1f}/10] {title}</b>",
        f"🌐 <i>{source}</i> | {categories}",
        f"📊 <i>Польза: {scores.practical_value}/10 | Жизнь: {scores.resident_relevance}/10 | Контент: {scores.content_potential}/10</i>",
        ""
    ]

    # Почему это важно
    if ed.why:
        lines.append("💡 <b>Почему это важно:</b>")
        for w in ed.why[:3]:
            lines.append(f"• {escape_html(w)}")
        lines.append("")

    # Редакционные углы подачи
    if ed.editorial_angles:
        lines.append("🎯 <b>Идеи для публикации:</b>")
        for angle in ed.editorial_angles[:3]:
            lines.append(f"→ {escape_html(angle)}")
        lines.append("")

    # Ссылка на первоисточник
    if url:
        lines.append(f'🔗 <a href="{url}">Читать оригинальную статью</a>')

    return "\n".join(lines)


def send_top_articles_to_telegram(
    scored_articles: List[Any],
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
    top_n: int = 10,
    min_score: float = 6.5,
    delay_between_messages: float = 1.0
) -> int:
    """
    Фильтрует ТОП статьи и отправляет их в Telegram.
    Каждое сообщение содержит inline-кнопку для генерации ТГ-поста через RAG.
    Возвращает количество успешно отправленных сообщений.
    """
    bot_token = token or os.environ.get("TELEGA_BOT") or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        log.warning("⚠️ Не указан TELEGA_BOT в .env или аргументах. Отправка в Telegram пропущена.")
        return 0

    target_chat_id = chat_id or os.environ.get("TELEGA_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")
    if not target_chat_id:
        log.info("🔍 TELEGA_CHAT_ID не задан. Пробуем определить через getUpdates...")
        target_chat_id = auto_detect_chat_id(bot_token)
        if not target_chat_id:
            log.warning("⚠️ Не найден chat_id. Напишите боту любое сообщение в Telegram или добавьте TELEGA_CHAT_ID=ваш_id в .env.")
            return 0

    # Фильтруем статьи: только прошедшие порог и сортированные по скору
    candidates = [
        art for art in scored_articles
        if art.news_score >= min_score or art.is_top
    ]
    candidates.sort(key=lambda x: x.news_score, reverse=True)
    selected = candidates[:top_n]

    if not selected:
        log.info("ℹ️ Нет статей с оценкой >= %.1f для отправки в Telegram.", min_score)
        return 0

    log.info("🚀 Отправка %d ТОП-новостей в Telegram (chat_id: %s)...", len(selected), target_chat_id)

    # Вводное заглавное сообщение
    header_msg = (
        f"🏆 <b>Дайджест ТОП-новостей Испании</b>\n"
        f"📅 Отобрано лучших: <b>{len(selected)}</b> (порог скоринга: {min_score})\n"
        f"🤖 Оценено с помощью редакционного классификатора"
    )
    send_telegram_message(bot_token, target_chat_id, header_msg)
    time.sleep(delay_between_messages)

    sent_count = 0
    for art in selected:
        # Сохраняем контекст статьи для telegram_bot.py
        _save_article_context(art)

        msg_text = format_article_telegram(art)

        # Определяем article_id для callback_data
        article_id = str(art.id or hash(art.title))

        # Inline-кнопка "Сгенерировать ТГ-пост"
        inline_keyboard = [
            [{"text": "📝 Сгенерировать ТГ-пост", "callback_data": f"sel_nb:{article_id}"}]
        ]

        success = send_telegram_message_with_buttons(
            bot_token, target_chat_id, msg_text, inline_keyboard
        )
        if success:
            sent_count += 1
        time.sleep(delay_between_messages)

    log.info("✅ Успешно отправлено в Telegram: %d/%d сообщений.", sent_count, len(selected))
    return sent_count
