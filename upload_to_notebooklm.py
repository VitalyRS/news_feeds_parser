#!/usr/bin/env python3
"""
Модуль прямой загрузки новостей в Google NotebookLM через библиотеку notebooklm-py.

Позволяет:
1. Создавать/использовать блокнот NotebookLM для текущего месяца (например 'News Intelligence — 2026-08').
2. Загружать файлы дайджестов NewsRAG/NOTEBOOKLM/*.md в качестве источников (Sources).
3. Автоматически синхронизировать и обновлять все части дайджеста.

Первоначальная авторизация (разово):
    notebooklm login
"""

import os
import re
import sys
import glob
import asyncio
import logging
import argparse
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NOTEBOOKLM_DIR = os.path.join(SCRIPT_DIR, 'NewsRAG', 'NOTEBOOKLM')
LOG_FILE = os.path.join(SCRIPT_DIR, 'notebooklm_sync.log')

# Троттлинг загрузок: NotebookLM режет сессию при слишком частых RPC подряд.
# Пауза между файлами + повтор с экспоненциальным backoff на упавших.
UPLOAD_DELAY_SEC = float(os.getenv('NOTEBOOKLM_UPLOAD_DELAY_SEC', '2.5'))
MAX_UPLOAD_RETRIES = int(os.getenv('NOTEBOOKLM_UPLOAD_RETRIES', '4'))
UPLOAD_RETRY_BACKOFF_SEC = float(os.getenv('NOTEBOOKLM_UPLOAD_BACKOFF_SEC', '5'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)


def is_authenticated() -> bool:
    """Проверяет, авторизован ли пользователь в NotebookLM."""
    profile_path = os.path.expanduser('~/.notebooklm/profiles/default/storage_state.json')
    return os.path.exists(profile_path) and os.path.getsize(profile_path) > 0


async def upload_digest_files_async(filepaths: list[str] | None = None,
                                    notebook_name: str | None = None,
                                    month_prefix: str | None = None,
                                    since_date: str | None = None) -> bool:
    """
    Загружает и синхронизирует подневные файлы дайджеста в Google NotebookLM.

    since_date (YYYY-MM-DD): грузить только дни начиная с этой даты включительно.
    Даты берутся из имени файла, ISO-формат сравнивается лексикографически.
    """
    if not is_authenticated():
        log.warning("⚠️ Вы еще не авторизованы в NotebookLM.")
        log.info("👉 Выполните в терминале разовую команду: notebooklm login")
        return False

    try:
        from notebooklm import NotebookLMClient
    except ImportError:
        log.error("❌ Библиотека notebooklm-py не найдена. Установите: pip install notebooklm-py")
        return False

    # 1. Находим файлы для загрузки (ежемесячные дайджесты)
    if not filepaths:
        if not os.path.exists(NOTEBOOKLM_DIR):
            log.warning("⚠️ Папка %s не существует.", NOTEBOOKLM_DIR)
            return False
            
        all_files = sorted(glob.glob(os.path.join(NOTEBOOKLM_DIR, '*.md')))
        # Подневные дайджесты формата YYYY-MM-DD[_partN].md — это активные источники.
        active_files = [f for f in all_files if re.match(r'^\d{4}-\d{2}-\d{2}(_part\d+)?\.md$', os.path.basename(f))]
        if not active_files:
            active_files = all_files

        if month_prefix:
            # Префикс может быть днём (YYYY-MM-DD) или месяцем (YYYY-MM) — берём всё, что начинается с него.
            filepaths = [f for f in active_files if os.path.basename(f).startswith(month_prefix)]
        else:
            # По умолчанию грузим все подневные дайджесты (каждый день — отдельный источник).
            if not active_files:
                log.warning("⚠️ Нет файлов дайджестов в %s", NOTEBOOKLM_DIR)
                return False
            filepaths = active_files

        # Фильтр по дате: грузим только дни начиная с since_date включительно.
        # Нормализуем since_date к YYYY-MM-DD ('today' → сегодня, таймстамп → первые 10 симв.),
        # иначе '2026-08-20 15:30' лексикографически исключил бы сам файл 2026-08-20.md.
        if since_date:
            if since_date == 'today':
                norm_since = datetime.now().strftime('%Y-%m-%d')
            else:
                norm_since = since_date[:10]
            before = len(filepaths)
            filepaths = [f for f in filepaths if os.path.basename(f)[:10] >= norm_since]
            log.info("📅 Фильтр по дате since=%s: %d из %d подневных файлов", norm_since, len(filepaths), before)

    if not filepaths:
        log.warning("⚠️ Нет файлов для загрузки в NotebookLM (since=%s).", since_date)
        return False

    # Тег блокнота — по месяцу самого раннего файла (YYYY-MM), чтобы дни одного месяца
    # складывались в один блокнот как отдельные источники.
    month_tag = os.path.basename(sorted(filepaths)[0])[:7]
    title = notebook_name or f"News Intelligence — {month_tag}"

    log.info("📓 Подключение к Google NotebookLM...")
    try:
        async with NotebookLMClient.from_storage() as client:
            notebooks = await client.notebooks.list()
            
            # Ищем существующий блокнот или создаем новый
            target_notebook = None
            for nb in notebooks:
                if nb.title == title or nb.title.startswith(f"News Intelligence — {month_tag}"):
                    target_notebook = nb
                    break

            if not target_notebook:
                log.info("📁 Создаем новый блокнот в NotebookLM: '%s'...", title)
                target_notebook = await client.notebooks.create(title=title)
                log.info("✅ Блокнот создан (ID: %s)", target_notebook.id)
            else:
                log.info("📂 Используем существующий блокнот '%s' (ID: %s)", target_notebook.title, target_notebook.id)

            # Получаем существующие источники
            existing_sources = await client.sources.list(target_notebook.id)
            existing_map = {src.title: src for src in existing_sources}

            uploaded_count = 0
            failed: list[str] = []
            total = len(filepaths)
            for idx, fpath in enumerate(filepaths):
                fname = os.path.basename(fpath)
                fsize_kb = os.path.getsize(fpath) / 1024

                # Если файл уже есть в источниках — удаляем старую версию для обновления
                if fname in existing_map:
                    old_src = existing_map[fname]
                    log.info("🔄 Обновление источника '%s' (удаляем старую версию)...", fname)
                    try:
                        await client.sources.delete(target_notebook.id, old_src.id)
                    except Exception as del_err:
                        log.warning("⚠️ Не удалось удалить старую версию '%s': %s", fname, del_err)

                log.info("📤 [%d/%d] Загрузка '%s' (%.1f КБ)...", idx + 1, total, fname, fsize_kb)

                # NotebookLM троттлит сессию при слишком частых RPC подряд
                # (симптом: rpc_code=5 NOT_FOUND начинает валить и ADD_SOURCE_FILE,
                # и GET_NOTEBOOK после ~60 быстрых загрузок). Ставим паузу между
                # загрузками и повторяем с экспоненциальным backoff.
                ok = False
                for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                    try:
                        source = await client.sources.add_file(
                            notebook_id=target_notebook.id,
                            file_path=Path(fpath),
                            wait=True
                        )
                        log.info("✅ Источник '%s' успешно добавлен (ID: %s)", fname, source.id)
                        uploaded_count += 1
                        ok = True
                        break
                    except Exception as upload_err:
                        if attempt < MAX_UPLOAD_RETRIES:
                            backoff = UPLOAD_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                            log.warning("⚠️ Ошибка загрузки '%s' (попытка %d/%d): %s — повтор через %.0f с",
                                        fname, attempt, MAX_UPLOAD_RETRIES, upload_err, backoff)
                            await asyncio.sleep(backoff)
                        else:
                            log.error("❌ Не удалось загрузить '%s' после %d попыток: %s",
                                      fname, MAX_UPLOAD_RETRIES, upload_err)
                            failed.append(fname)

                # Пауза между файлами, чтобы не упереться в троттлинг сессии.
                if idx + 1 < total:
                    await asyncio.sleep(UPLOAD_DELAY_SEC)

            log.info("🎉 Синхронизация с NotebookLM завершена: успешно загружено %d/%d файлов.",
                     uploaded_count, total)
            if failed:
                log.warning("⚠️ Не загружены (%d): %s", len(failed), ", ".join(failed))
            return uploaded_count > 0

    except Exception as e:
        log.error("❌ Ошибка при работе с NotebookLM API: %s", e)
        return False


def upload_monthly_digest(filepath: str | None = None, notebook_name: str | None = None,
                          since_date: str | None = None) -> bool:
    """Синхронная обертка для вызова из main.py и других модулей.

    since_date (YYYY-MM-DD): грузить только дни начиная с этой даты включительно.
    Игнорируется, если задан конкретный filepath.
    """
    paths = [filepath] if filepath else None
    return asyncio.run(upload_digest_files_async(
        filepaths=paths, notebook_name=notebook_name, since_date=since_date))


def main():
    parser = argparse.ArgumentParser(description="Прямая загрузка сводки новостей в Google NotebookLM")
    parser.add_argument('--file', default=None, help="Путь к конкретному .md файлу")
    parser.add_argument('--name', default=None, help="Название блокнота в NotebookLM")
    parser.add_argument('--since', default='2026-08-20',
                        help="Грузить только дни начиная с этой даты (YYYY-MM-DD, по умолчанию 2026-08-20)")
    args = parser.parse_args()

    success = upload_monthly_digest(filepath=args.file, notebook_name=args.name,
                                    since_date=args.since)
    if success:
        print("\n" + "=" * 60)
        print("🎉 ФАЙЛЫ УСПЕШНО ЗАГРУЖЕНЫ В GOOGLE NOTEBOOKLM!")
        print("🌐 Откройте: https://notebooklm.google.com/")
        print("   Там появился ваш блокнот со всеми источниками новостей.")
        print("=" * 60 + "\n")
    else:
        print("\nℹ️ Не удалось загрузить. Проверьте логи или выполните: notebooklm login")


if __name__ == '__main__':
    main()
