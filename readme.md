# News Feeds Parser

Система собирает новости из RSS/Atom и HTML-страниц, извлекает полные тексты,
отбирает материалы, связанные с Испанией, создаёт структурированные саммари и
готовит Markdown-файлы для Google NotebookLM и RAG.

Основной сценарий запуска — `main.py`. Состояние обработки хранится в SQLite,
поэтому уже завершённая работа не теряется после остановки или перезапуска.

## Возможности

- параллельный сбор ссылок из `feeds.json` и прямой скрапинг сайтов из `dead_feeds.txt`;
- защита от дубликатов по уникальной ссылке;
- извлечение полных текстов через Googlebot-профиль, `curl_cffi` (Chrome TLS),
  Trafilatura и резервный Jina Reader; keep-alive сессии на поток;
- локальный отсев очевидного спама и части явно испанских/международных новостей;
- LLM-классификация неоднозначных статей пакетами по 10;
- суммаризация относящихся к Испании статей пакетами по 3;
- независимые очереди Mistral, Gemini, OpenRouter, Cerebras и AnyModel;
- немедленное сохранение результата каждой статьи в `baza.db`;
- контрольный экспорт в `NewsRAG/NOTEBOOKLM` каждые 100 статей;
- экспорт исходных текстов в `NewsRAG/RAW`;
- необязательная загрузка в Google NotebookLM и синхронизация с Google Drive.

## Архитектура

```text
feeds.json ─────────────┐
                       ├─> сбор ссылок ─> baza.db
dead_feeds.txt ─────────┘                     │
                                             v
                                    извлечение полного текста
                                             │
                                             v
                              локальная + LLM-классификация
                                             │
                              не Испания ────┴──── Испания
                                  │                   │
                               SKIP             LLM-саммари
                                  │                   │
                                  └─────────┬─────────┘
                                            v
                               немедленная запись в SQLite
                                            │
                         ┌──────────────────┴──────────────────┐
                         v                                     v
                  NewsRAG/RAW                    NewsRAG/NOTEBOOKLM
                                                               │
                                                  ┌────────────┴───────────┐
                                                  v                        v
                                           Google NotebookLM         Google Drive
```

`main.py` выполняет шесть фаз:

1. сбор ссылок из RSS/Atom и HTML;
2. скачивание полных текстов;
3. классификация и генерация LLM-саммари;
4. локальный экспорт RAW и NotebookLM Markdown;
5. загрузка месячного дайджеста в Google NotebookLM;
6. необязательная синхронизация `NewsRAG` с Google Drive.

## Требования и установка

- Python 3.10 или новее;
- доступ в интернет;
- хотя бы ключ AnyModel для текущей резервной цепочки;
- ключи остальных LLM-провайдеров — по желанию;
- учётная запись Google нужна только для NotebookLM/Drive.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Настройка `.env`

Файл `.env` читается из корня проекта. Не добавляйте его в Git и не публикуйте
ключи. Если ключ уже был раскрыт, отзовите его у провайдера и создайте новый.

Минимальная конфигурация:

```env
ANYMODEL_KEY=your_anymodel_key
ANYMODEL_BASE_URL=https://anymodel.org/v1
ANYMODEL_MODEL=am/deepseek-v4-flash
```

Полная конфигурация пяти провайдеров:

```env
# AnyModel: основной alias и бесплатные fallback-модели
ANYMODEL_KEY=your_anymodel_key
ANYMODEL_BASE_URL=https://anymodel.org/v1
ANYMODEL_MODEL=am/deepseek-v4-flash
ANYMODEL_FALLBACK_MODEL=am/deepseek-v4-flash
ANYMODEL_FREE_MODELS=am/glm-5.2,am/deepseek-v4-flash,am/deepseek-v4-pro,am/minimax-m3,am/free

# Mistral
MISTRAL_API_KEY=your_mistral_key
MISTRAL_BASE_URL=https://api.mistral.ai/v1
MISTRAL_MODEL=mistral-small-latest

# Google Gemini (нативный generateContent API)
GEMINI_API_KEY=your_gemini_key
GEMINI_BASE_URL=https://generativelanguage.googleapis.com/v1beta
GEMINI_MODEL=gemini-3.5-flash-lite
GEMINI_MODELS=gemini-3.5-flash-lite,gemini-flash-lite-latest,gemini-3.5-flash,gemini-flash-latest

# OpenRouter
OPENROUTER_API_KEY=your_openrouter_key
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_MODEL=openrouter/free

# Cerebras
CEREBRAS_API_KEY=your_cerebras_key
CEREBRAS_BASE_URL=https://api.cerebras.ai/v1
CEREBRAS_MODEL=gemma-4-31b

# Пакеты и параллельность
CLASSIFICATION_BATCH_SIZE=10
SUMMARY_BATCH_SIZE=3
PROVIDER_MAX_CONCURRENT=5
GEMINI_MAX_CONCURRENT=5
ANYMODEL_MAX_CONCURRENT=1
ANYMODEL_MIN_INTERVAL=0.5
ANYMODEL_MODEL_ATTEMPTS=5
```

Все ключи, кроме `ANYMODEL_KEY`, необязательны. AnyModel сейчас является общим
fallback: без его ключа фаза суммаризации целиком пропускается. Если у Cerebras
нет бесплатной квоты и API отвечает `402 Payment required`, провайдер отключается
до следующего запуска, а его пакет передаётся AnyModel.

### Бесплатные модели AnyModel

В `ANYMODEL_FREE_MODELS` модели перечисляются через запятую. Параллельные запросы
стартуют с разных alias; при `404`, `429`, `5xx`, тайм-ауте или пустом ответе код
быстро переходит к следующему. Текущий список конфигурации:

```text
am/glm-5.2
am/deepseek-v4-flash
am/deepseek-v4-pro
am/minimax-m3
am/free
am/nemotron-3-ultra-550b-a55b
am/gemma-4-31b-it
am/diffusiongemma-26b-a4b-it
am/gpt-oss-20b
am/inkling
am/laguna-xs-2.1
am/llama-3.1-nemotron-nano-vl-8b-v1
am/llama-3.2-11b-vision-instruct
am/mistral-nemotron
am/nemotron-3-nano-30b-a3b
am/nemotron-3-nano-omni-30b-a3b-reasoning
am/nemotron-3-super-120b-a12b
am/nemotron-3.5-content-safety
am/nemotron-3.5-lightning-30b-a3b
am/nemotron-nano-12b-v2-vl
am/nvidia-nemotron-nano-9b-v2
am/riva-translate-4b-instruct-v2
am/step-3.7-flash
```

Доступность бесплатных моделей меняется на стороне сервиса. Ошибка отдельной
модели не означает поломку всего пайплайна.

## LLM-очереди

Пакеты назначаются по кругу независимым маршрутам:

```text
Mistral -> Gemini -> OpenRouter -> Cerebras -> AnyModel -> Mistral -> ...
```

Сначала выполняются локальные эвристики. Только неоднозначные материалы уходят
назначенному провайдеру. После классификации статьи «про Испанию» суммаризируются
тем же маршрутом, по три статьи в запросе.

По умолчанию общий пул содержит 25 рабочих потоков: до пяти запросов на каждого
провайдера. AnyModel имеет отдельный лимит `ANYMODEL_MAX_CONCURRENT`; значение `1`
полезно при частых `429`. Очереди независимы: медленный провайдер не останавливает
остальные. Если назначенный провайдер не вернул пригодный результат, пакет уходит
в AnyModel.

Пример логов:

```text
Классификация: провайдер=gemini, пакет=10, через API=7, локально=3
Саммаризация: провайдер=gemini, испанских статей=6/10
```

## Запуск

Инкрементальный режим по умолчанию — обрабатывает всё с момента последнего
запуска до текущего, с точностью до часа/минуты:

```bash
python3 main.py
```

Момент последнего успешного прогона (UTC-timestamp) хранится в
`.pipeline_state.json`. Если не запускать скрипт несколько часов или дней,
следующий прогон подхватит статьи за весь пропущенный период — не переобрабатывая
уже сделанное. Точкой отсчёта служит время **старта** прогона, поэтому статьи,
собранные во время работы скрипта, не теряются. На самый первый запуск (файла
состояния ещё нет) отсчёт идёт от `2026-08-20`. Явные `--since`/`--today`/`--all-time`
— разовые прогоны, точку отсчёта они не сдвигают.

`--since` принимает как дату, так и точный момент:

```bash
python3 main.py --since 2026-08-21              # с начала дня
python3 main.py --since "2026-08-21 14:00:00"   # с 14:00 UTC
```

Полезные варианты:

```bash
python3 main.py --today
python3 main.py --since 2026-08-20
python3 main.py --all-time

# Обработать уже собранные и скачанные статьи
python3 main.py --skip-scrape --skip-text

# Без облачных загрузок
python3 main.py --skip-notebooklm --skip-drive

# Короткий тест LLM на 20 статьях
python3 main.py --skip-scrape --skip-text --summary-limit 20 --skip-notebooklm --skip-drive

# Только локальный экспорт готовых данных
python3 main.py --export-only --skip-notebooklm --skip-drive

# Включить Google Drive (по умолчанию выключен)
python3 main.py --with-drive
```

### Параметры `main.py`

| Параметр | Назначение |
|---|---|
| _(без флагов)_ | Инкрементально: с даты последнего запуска до сейчас |
| `--today` | Использовать статьи за сегодня |
| `--since YYYY-MM-DD` | Нижняя граница даты (разовый прогон) |
| `--all-time` | Не ограничивать обработку датой |
| `--skip-scrape` | Пропустить сбор ссылок |
| `--skip-text` | Пропустить извлечение полных текстов |
| `--skip-summary` | Пропустить классификацию и саммаризацию |
| `--skip-export` | Не создавать локальные Markdown-файлы |
| `--skip-notebooklm` | Не загружать файлы в Google NotebookLM |
| `--with-drive` | Включить Google Drive |
| `--skip-drive` | Явно отключить Google Drive |
| `--export-only` | Только экспорт и возможная загрузка NotebookLM |
| `--text-limit N` | Ограничить число текстов за прогон |
| `--summary-limit N` | Ограничить число статей для LLM-фазы |
| `--text-workers N` | Потоки извлечения текстов, по умолчанию 16 |
| `--summary-workers N` | Общий LLM-пул, по умолчанию 25 |
| `--scrape-workers N` | Потоки скрапинга сайтов из `dead_feeds.txt`, по умолчанию 5 |
| `--model NAME` | Модель AnyModel основного/fallback-вызова |
| `--re-summarize` | Повторно создать существующие саммари |
| `--re-export` | Повторно экспортировать Markdown |
| `--drive-dry-run` | Проверить Drive без записи |
| `--output-dir PATH` | Изменить каталог экспорта |

Осторожно используйте `--re-summarize`: он создаёт новые API-запросы.
`--re-export` не дублирует статьи в месячном дайджесте благодаря маркеру
`Article ID`.

## Отдельные модули

```bash
# Сбор ссылок и текстов
python3 news_collector.py --today
python3 news_collector.py --skip-text

# Только извлечение текстов
python3 text_downloader.py --today --limit 100 --workers 16
python3 text_downloader.py --retry-failed

# Только LLM-обработка
python3 summarizer.py --today --limit 100 --workers 25
python3 summarizer.py --all-time

# Экспорт Markdown
python3 md_exporter.py --today
python3 md_exporter.py --since 2026-08-20
python3 md_exporter.py --all-time --re-export

# Проверка RSS/Atom
python3 check_feeds.py

# Загрузка конкретного файла в NotebookLM
python3 upload_to_notebooklm.py --file NewsRAG/NOTEBOOKLM/2026-08.md

# Google Drive без фактической загрузки
python3 gdrive_sync.py --dry-run
```

## Сохранение прогресса

В LLM-фазе включены SQLite WAL и `synchronous=FULL`. Результат `done`, `skipped`
или `error` записывается и фиксируется после каждой статьи. При `Ctrl+C` уже
завершённые записи остаются в базе, ожидающие задачи отменяются.

При следующем обычном запуске:

- `done` и `skipped` не обрабатываются повторно;
- `pending` обрабатываются;
- `error` пробуются снова;
- `--re-summarize` принудительно включает завершённые записи.

Счётчик `[670/1981]` — номер обработанной статьи в текущем запуске, а не потеря
предыдущих 669 записей.

Каждые 100 сохранённых результатов вызывается контрольный экспорт всех ещё не
экспортированных `done` и `skipped`. Саммари дописываются в месячный файл, `SKIP`
только отмечаются экспортированными. После нормального завершения фаза 4
экспортирует оставшийся хвост, даже если он меньше 100.

## База данных и экспорт

Основная таблица — `news` в `baza.db`:

| Поле | Содержание |
|---|---|
| `id` | Внутренний ID статьи |
| `source`, `source_type` | Источник и способ сбора (`rss`/`scrape`) |
| `title`, `link`, `description` | Метаданные; `link` уникален |
| `published`, `scraped_at` | Дата публикации и время сбора |
| `full_text`, `download_status` | Извлечённый текст и статус загрузки |
| `llm_summary` | Испаноязычное саммари или `SKIP` |
| `summary_status` | `pending`, `done`, `skipped` или `error` |
| `exported_raw` | Признак RAW-экспорта |
| `exported_notebooklm` | Признак обработки для дайджеста |

```text
NewsRAG/
├── RAW/<source>/<YYYY>/<MM>/<DD>/<id>_<slug>.md
└── NOTEBOOKLM/
    ├── 2026-08.md
    ├── 2026-08_part2.md
    └── ...
```

NotebookLM-файлы являются месячными, а не суточными. Новая часть создаётся при
достижении примерно 20 МБ. В дайджест попадают только `done`; исходные тексты и
нерелевантные `SKIP` туда не добавляются.

## Google NotebookLM

Разовая авторизация:

```bash
notebooklm login
```

Затем `main.py` ищет файлы текущего месяца, создаёт блокнот вида
`News Intelligence — YYYY-MM` или использует существующий и обновляет источники.
`--skip-notebooklm` отключает сетевую загрузку, но не локальный экспорт в
`NewsRAG/NOTEBOOKLM`.

## Google Drive

Drive по умолчанию отключён. Поддерживаются:

1. `service_account.json` в корне проекта;
2. OAuth-файлы `credentials.json` и `token.json`;
3. Application Default Credentials от `gcloud`.

Для Service Account предоставьте его адресу доступ к папке `NewsRAG`. Проверка:

```bash
python3 main.py --with-drive --drive-dry-run
```

## Диагностика

### HTTP 429

Провайдер ограничил частоту запросов. Уменьшите параллельность:

```env
ANYMODEL_MAX_CONCURRENT=1
ANYMODEL_MIN_INTERVAL=2.0
PROVIDER_MAX_CONCURRENT=2
GEMINI_MAX_CONCURRENT=2
```

После изменения `.env` перезапустите процесс: параметры читаются при импорте.

### HTTP 404 / `model_not_found`

Модель снята с обслуживания или недоступна ключу. Удалите её из списка или
поставьте рабочую первой. AnyModel перебирает следующие бесплатные alias, Gemini
пробует до трёх моделей из `GEMINI_MODELS`.

### Gemini HTTP 503

Это временная перегрузка модели. `GEMINI_MODELS` позволяет перейти к следующей.
Если 503 повторяются, уменьшите `GEMINI_MAX_CONCURRENT` до `1`–`2`.

### Cerebras HTTP 402

Ключ распознан, но на аккаунте нет квоты. Cerebras отключается до перезапуска,
а пакет продолжает обработку через AnyModel.

### Тайм-ауты AnyModel

AnyModel использует короткий тайм-аут и меняет модель вместо долгого ожидания.
Рост параллельности при тайм-аутах может только усилить `429`.

### Нет файлов в `NewsRAG/NOTEBOOKLM`

Проверьте:

- не передан ли `--skip-export`;
- есть ли записи `summary_status='done'`;
- достигнуты ли 100 результатов или завершилась ли фаза 3;
- правильный ли каталог указан через `--output-dir`;
- не классифицированы ли статьи как `SKIP`;
- нет ли ошибок в `pipeline.log` или `summarizer.log`.

```bash
sqlite3 baza.db "SELECT COALESCE(summary_status,'NULL'), COUNT(*) FROM news GROUP BY summary_status;"
```

## Логи

| Файл | Содержание |
|---|---|
| `pipeline.log` | Все фазы `main.py` |
| `summarizer.log` | LLM-маршруты, модели, ошибки API |
| `notebooklm_sync.log` | Загрузка источников в NotebookLM |
| `gdrive_sync.log` | Google Drive |

```bash
tail -f pipeline.log
```

## Структура проекта

```text
news_feeds_parser/
├── main.py                   # оркестратор шести фаз
├── news_collector.py         # RSS и прямой HTML-скрапинг
├── text_downloader.py        # извлечение полных текстов
├── summarizer.py             # классификация, LLM и SQLite checkpoint
├── md_exporter.py            # RAW и месячные NotebookLM Markdown
├── upload_to_notebooklm.py   # загрузка месячного дайджеста
├── gdrive_sync.py            # синхронизация с Drive
├── check_feeds.py            # проверка RSS/Atom
├── feeds.json                # список RSS/Atom URL
├── dead_feeds.txt            # сайты прямого скрапинга
├── requirements.txt          # Python-зависимости
├── baza.db                   # рабочая SQLite-база
├── NewsRAG/                  # локальный экспорт
├── .env                      # секреты и параметры, не публиковать
└── readme.md                 # эта документация
```

## Рекомендуемый рабочий цикл

Сначала короткая проверка без сбора и облачных загрузок:

```bash
python3 main.py --skip-scrape --skip-text --summary-limit 20 --skip-notebooklm --skip-drive
```

После проверки логов запускайте основной объём:

```bash
python3 main.py --skip-scrape --skip-text --skip-notebooklm --skip-drive
```

Экспорт и облачную синхронизацию можно повторять отдельно, не создавая саммари
заново.
