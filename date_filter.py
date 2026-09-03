#!/usr/bin/env python3
"""Единый построитель SQL-фильтра по времени для всех фаз пайплайна.

Поле `scraped_at` пишется в БД как UTC через SQLite CURRENT_TIMESTAMP и хранится
строкой ISO 'YYYY-MM-DD HH:MM:SS', поэтому сравнивается лексикографически. Это
позволяет инкрементальному режиму работать с точностью до часа/минуты, а не
только по календарной дате.

Функция принимает три формы `since`:
  * 'today'                 — статьи за сегодня (по локальной дате);
  * 'YYYY-MM-DD'            — с начала дня (обратная совместимость);
  * 'YYYY-MM-DD HH:MM:SS'   — с точного момента (инкрементальный режим).

Возвращает (sql_condition, params). Если since пустой — ('', []).
"""

import re

# 'YYYY-MM-DD HH:MM[:SS]' — полный timestamp (точность до часа/минуты).
_TIMESTAMP_RE = re.compile(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$')
# 'YYYY-MM-DD' — только дата.
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def build_since_condition(since: str | None) -> tuple[str, list]:
    """Строит SQL-условие отсечения по времени сбора/публикации статьи."""
    if not since:
        return '', []

    if since == 'today':
        # scraped_at в UTC; сегодня по локали + запас в 24ч на смещение зоны.
        return (
            "(date(scraped_at, 'localtime') >= date('now', 'localtime') "
            "OR date(published, 'localtime') >= date('now', 'localtime') "
            "OR scraped_at >= datetime('now', '-24 hours'))",
            [],
        )

    if _TIMESTAMP_RE.match(since):
        # Точный момент. scraped_at хранится в UTC — сравниваем напрямую как
        # строки ISO. published может быть в своей зоне, поэтому по нему
        # оставляем сравнение по дню (грубее, но не теряет свежие статьи).
        ts = since.replace('T', ' ')
        day = ts[:10]
        return (
            "(scraped_at >= ? OR date(published) >= ?)",
            [ts, day],
        )

    if _DATE_RE.match(since):
        # Начало дня (обратная совместимость с --since YYYY-MM-DD).
        return (
            "(date(scraped_at) >= ? OR date(published) >= ?)",
            [since, since],
        )

    # Неизвестный формат — не фильтруем, чтобы не потерять данные молча.
    return '', []
