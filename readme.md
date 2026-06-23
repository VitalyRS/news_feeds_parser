# 📰 RSS/Atom Feed Parser

Автоматизированный парсер RSS и Atom каналов с сохранением в MySQL. Скрипт работает через `cron`, автоматически создаёт базу данных, защищает от дублей и добавляет только новые статьи.

---

## ✨ Возможности

- 🔄 **Автоматическое создание БД** — база данных и таблица создаются при первом запуске
- 🚫 **Защита от дублей** — одинаковые статьи (определяется по ссылке) не записываются повторно
- 🆕 **Только новые статьи** — в базу попадают исключительно свежие записи
- 📡 **Поддержка RSS и Atom** — `feedparser` автоматически определяет формат канала
- 🕐 **Гибкое расписание** — настраивается через системный `cron` (каждые N минут/часов/дней)
- 📝 **Подробное логирование** — статистика по новым статьям и дублям
- 🐧 **Production-ready** — работает через `cron` на любом Unix-подобном сервере

---

## 📋 Требования

- **Python** 3.7+
- **MySQL** 5.7+ / MariaDB 10.3+
- **ОС**: Linux / macOS / любой Unix с поддержкой `cron`

### Python-пакеты:
```
feedparser
mysql-connector-python
requests
```

---

## 🚀 Установка

### 1. Клонируйте репозиторий или создайте файл
```bash
mkdir feed-parser && cd feed-parser
touch feed_parser.py
```

### 2. Установите зависимости
```bash
pip install feedparser mysql-connector-python requests
```

### 3. Настройте конфигурацию в `feed_parser.py`
Откройте файл и заполните следующие параметры:

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_db_user',      # ← Ваш MySQL-пользователь
    'password': 'your_db_pass',  # ← Ваш MySQL-пароль
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

DATABASE_NAME = 'feed_parser_db'  # Имя БД (создастся автоматически)

RSS_FEEDS = [
    'https://feeds.bbci.co.uk/news/rss.xml',
    # Добавьте ваши RSS-каналы
]

ATOM_FEEDS = [
    'https://github.com/blog.atom',
    # Добавьте ваши Atom-каналы
]
```

### 4. Проверьте работу вручную
```bash
python3 feed_parser.py
```

Вы должны увидеть примерно такой вывод:
```
2026-06-23 15:00:01 [INFO] 🚀 Запуск парсера (cron-запуск)...
2026-06-23 15:00:01 [INFO] ✅ База данных 'feed_parser_db' проверена/создана.
2026-06-23 15:00:01 [INFO] ✅ Таблица 'feed_items' проверена/создана.
2026-06-23 15:00:01 [INFO] 🌐 Обработка фида: https://feeds.bbci.co.uk/news/rss.xml
2026-06-23 15:00:03 [INFO] ✅ Цикл завершён. Новых статей: 5 | Пропущено дублей: 45
2026-06-23 15:00:03 [INFO] 🏁 Завершение работы парсера.
```

---

## ⏰ Настройка расписания (cron)

### Откройте crontab для редактирования:
```bash
crontab -e
```

### Добавьте строку с нужным интервалом:

| Интервал | Строка crontab |
|----------|----------------|
| Каждые 15 минут | `*/15 * * * * /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |
| Каждые 30 минут | `*/30 * * * * /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |
| Каждый час | `0 * * * * /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |
| Каждые 2 часа | `0 */2 * * * /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |
| Каждый день в 9:00 | `0 9 * * * /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |
| Каждый понедельник в 8:00 | `0 8 * * 1 /usr/bin/python3 /path/to/feed_parser.py >> /path/to/cron.log 2>&1` |

> ⚠️ **Важно:** Укажите **абсолютные пути** к Python и скрипту. Узнать путь к Python: `which python3`

### Сохраните файл и проверьте активные задачи:
```bash
crontab -l
```

---

## 🗄️ Структура базы данных

Скрипт автоматически создаёт таблицу `feed_items` со следующей структурой:

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | INT (PK, AUTO_INCREMENT) | Уникальный идентификатор |
| `title` | VARCHAR(500) | Заголовок статьи |
| `link` | VARCHAR(1000) **UNIQUE** | Ссылка на статью (защищает от дублей) |
| `description` | TEXT | Краткое описание / анонс |
| `published` | DATETIME | Дата публикации статьи |
| `source_feed` | VARCHAR(500) | URL фида-источника |
| `fetched_at` | TIMESTAMP | Время добавления в БД |

**Индексы:**
- `UNIQUE(link)` — защита от дублей
- `idx_link(link(255))` — быстрый поиск по ссылке
- `idx_published(published)` — сортировка по дате

### Полезные SQL-запросы:

```sql
-- Показать все записи
SELECT * FROM feed_items ORDER BY fetched_at DESC LIMIT 50;

-- Подсчёт статей по источнику
SELECT source_feed, COUNT(*) as total 
FROM feed_items 
GROUP BY source_feed;

-- Удалить старые записи (старше 30 дней)
DELETE FROM feed_items 
WHERE fetched_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Полный сброс таблицы
TRUNCATE TABLE feed_items;
```

---

## 📁 Структура проекта

```
feed-parser/
├── feed_parser.py        # Основной скрипт парсера
├── feed_parser.log       # Лог работы (создаётся автоматически)
├── cron.log              # Лог от cron (если указан в crontab)
└── README.md             # Этот файл
```

---

## 🔍 Отладка и логи

### Логи скрипта
Все операции записываются в `feed_parser.log`:
```bash
tail -f feed_parser.log
```

### Логи cron
Если вы указали `>> /path/to/cron.log` в crontab:
```bash
tail -f cron.log
```

### Системные логи cron (если что-то не запускается):
```bash
# Ubuntu/Debian
grep CRON /var/log/syslog

# CentOS/RHEL/Fedora
grep CRON /var/log/cron
```

---

## ❓ Частые проблемы (FAQ)

### Скрипт не запускается через cron
**Причина:** Cron использует ограниченные переменные окружения.
**Решение:** Указывайте абсолютные пути ко всему:
```cron
0 * * * * /usr/bin/python3 /home/user/feed_parser.py
```

### Не подключается к MySQL из cron
**Причина:** Cron работает с другим `PATH` и сокетом.
**Решение:** Добавьте `host='127.0.0.1'` вместо `'localhost'` в `DB_CONFIG`.

### Не пишет в лог-файл
**Причина:** Нет прав на запись в директорию.
**Решение:** Проверьте права: `chmod 755 /path/to/project/`

### Некоторые сайты отдают 403 Forbidden
**Причина:** Сайт блокирует автоматические запросы.
**Решение:** Измените `User-Agent` в скрипте на более "человечный":
```python
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
```

### Как добавить новый канал?
Добавьте URL в массив `RSS_FEEDS` или `ATOM_FEEDS` в конфигурации. Парсер сам определит формат.

### Как изменить интервал проверки?
Измените строку в `crontab -e`. Например, `*/5 * * * *` — каждые 5 минут.

---

## 📜 Лицензия

MIT — используйте свободно.

---

## 🤝 Контрибьютинг

Если у вас есть идеи по улучшению (добавление прокси, Telegram-уведомлений о новых статьях, экспорта в JSON и т.д.) — создавайте issue или pull request!