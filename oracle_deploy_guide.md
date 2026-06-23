# 🚀 Развёртывание news_collector на Oracle Cloud Free Tier

## Шаг 1 — Создание бесплатного сервера Oracle

> [!IMPORTANT]
> Oracle Free Tier даёт **навсегда бесплатно**: 2 VM (AMD) с 1 GB RAM каждая, или 1 VM (Ampere ARM) с до 4 CPU и 24 GB RAM.

### 1.1 Регистрация
1. Иди на [cloud.oracle.com](https://cloud.oracle.com) → **Sign Up**
2. Нужна банковская карта (не списывают, только верификация)
3. Выбери регион ближе к Испании: **eu-madrid-1** или **eu-frankfurt-1**

### 1.2 Создание VM
1. В консоли Oracle Cloud: **Compute → Instances → Create Instance**
2. Настройки:

| Параметр | Значение |
|----------|----------|
| **Name** | `news-collector` |
| **Image** | Ubuntu 22.04 (или Oracle Linux 8) |
| **Shape** | `VM.Standard.E2.1.Micro` (Always Free) |
| **OCPU / RAM** | 1 / 1 GB (достаточно) |
| **Boot volume** | 47 GB (бесплатно) |
| **SSH key** | Загрузи свой публичный ключ |

3. Нажми **Create** — VM запустится через ~2 минуты.

### 1.3 SSH-ключ (если нет)
```bash
# На своём компьютере:
ssh-keygen -t ed25519 -C "oracle-news" -f ~/.ssh/oracle_key

# Публичный ключ для Oracle:
cat ~/.ssh/oracle_key.pub
```

---

## Шаг 2 — Подключение к серверу

После создания VM Oracle покажет **Public IP**. Подключаемся:

```bash
# Для Ubuntu-образа:
ssh -i ~/.ssh/oracle_key ubuntu@<PUBLIC_IP>

# Для Oracle Linux:
ssh -i ~/.ssh/oracle_key opc@<PUBLIC_IP>
```

> [!TIP]
> Добавь в `~/.ssh/config` для удобства:
> ```
> Host oracle
>     HostName <PUBLIC_IP>
>     User ubuntu
>     IdentityFile ~/.ssh/oracle_key
> ```
> После этого: `ssh oracle`

---

## Шаг 3 — Загрузка проекта на сервер

С **твоего компьютера** выполни:

```bash
# Копируем все нужные файлы одной командой
scp -i ~/.ssh/oracle_key \
    news_collector.py \
    feeds.json \
    dead_feeds.txt \
    requirements.txt \
    setup_server.sh \
    ubuntu@<PUBLIC_IP>:~/
```

Или через rsync (удобнее при обновлениях):
```bash
rsync -avz -e "ssh -i ~/.ssh/oracle_key" \
    news_collector.py feeds.json dead_feeds.txt \
    requirements.txt setup_server.sh \
    ubuntu@<PUBLIC_IP>:~/
```

---

## Шаг 4 — Установка (один раз)

На **сервере**:

```bash
# Запускаем скрипт автоустановки — он сделает всё сам:
bash ~/setup_server.sh
```

Скрипт автоматически:
- ✅ Установит Python3 + venv
- ✅ Создаст `~/news_collector/` с виртуальным окружением
- ✅ Установит зависимости (feedparser, requests, beautifulsoup4)
- ✅ Настроит cron на запуск **каждый час**
- ✅ Выполнит тестовый запуск

---

## Шаг 5 — Проверяем что всё работает

```bash
# Проверить cron:
crontab -l
# Должно показать: 0 * * * * /home/ubuntu/news_collector/run.sh

# Запустить вручную:
~/news_collector/run.sh

# Посмотреть логи:
tail -50 ~/news_collector/logs/cron.log

# Проверить БД:
sqlite3 ~/news_collector/baza.db "SELECT COUNT(*) FROM news;"

# Посмотреть статьи по источникам:
sqlite3 ~/news_collector/baza.db \
  "SELECT source_type, COUNT(*) FROM news GROUP BY source_type;"

# Последние 5 статей:
sqlite3 ~/news_collector/baza.db \
  "SELECT title, source FROM news ORDER BY scraped_at DESC LIMIT 5;"
```

---

## Структура на сервере

```
~/news_collector/
├── news_collector.py     # главный скрипт
├── feeds.json            # RSS-ленты
├── dead_feeds.txt        # сайты для прямого скрапинга
├── requirements.txt      # зависимости Python
├── run.sh                # обёртка для cron
├── baza.db               # база данных (создаётся автоматически)
├── news_collector.log    # лог парсера
├── venv/                 # виртуальное окружение
└── logs/
    └── cron.log          # лог cron-запусков
```

---

## Полезные команды

| Задача | Команда |
|--------|---------|
| Запуск вручную | `~/news_collector/run.sh` |
| Логи в реальном времени | `tail -f ~/news_collector/logs/cron.log` |
| Сколько статей | `sqlite3 ~/news_collector/baza.db "SELECT COUNT(*) FROM news;"` |
| Статей за сегодня | `sqlite3 ~/news_collector/baza.db "SELECT COUNT(*) FROM news WHERE scraped_at >= date('now');"` |
| Изменить частоту cron | `crontab -e` |
| Остановить cron | `crontab -r` (удалит ВСЕ задачи!) |
| Обновить скрипт | `scp news_collector.py ubuntu@<IP>:~/news_collector/` |
| Добавить фиды | `nano ~/news_collector/feeds.json` |

---

## Настройка частоты cron

```bash
crontab -e
```

| Расписание | cron-выражение |
|------------|----------------|
| Каждый час | `0 * * * *` |
| Каждые 30 мин | `*/30 * * * *` |
| Каждые 2 часа | `0 */2 * * *` |
| 3 раза в день (8, 14, 20) | `0 8,14,20 * * *` |
| Раз в день в 6 утра | `0 6 * * *` |

---

## ⚠️ Oracle Free Tier — важные нюансы

> [!WARNING]
> **Idle Instance Reclamation** — Oracle может **остановить** VM если она простаивает (CPU < 10%) больше 7 дней подряд. Наш cron каждый час создаёт нагрузку, так что это **не проблема** — но знай об этом.

> [!NOTE]
> **Firewall** — для парсера **не нужно** открывать порты. Исходящие HTTP-запросы работают из коробки. Порты открывать нужно только если захочешь позже сделать веб-интерфейс.

> [!TIP]
> **Мониторинг** — если хочешь получать уведомления при ошибках, можно позже добавить Telegram-бота в скрипт для отправки отчётов.
