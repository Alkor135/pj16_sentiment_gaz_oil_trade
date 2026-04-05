# RSS Scraper (MSK, SQLite по месяцам)

Краткое: набор Python-скриптов для асинхронного сбора новостей из Interfax, 1Prime и Investing, сохранения в SQLite-базы по месяцам и ротации логов в московском времени (MSK).

## Фичи
- Парсинг RSS для провайдеров: `Interfax`, `1Prime`, `Investing`
- Конвертация времени в MSK
- Сохранение в месячные SQLite-файлы (`rss_news_YYYY_MM.db`) в директории из `settings.yaml`
- Удаление дубликатов и `VACUUM` для оптимизации
- Логирование в файл и консоль / ротация логов

## Требования
- Python 3.10+ (рекомендуется)
- pip
- Файл зависимостей: `beget/server/requirements.txt`

## Быстрый старт
1. Клонировать репозиторий и перейти в папку проекта.
2. Создать виртуальное окружение и активировать его:
   - Windows (PowerShell):
     - `python -m venv .venv`
     - `.venv\Scripts\Activate.ps1`
3. Установить зависимости:
   - `pip install -r beget/server/requirements.txt`

## Конфигурация
Настройки сохраняются в `beget/server/settings.yaml`:
- `base_dir` — директория для SQLite-файлов (например `/home/user/rss_scraper/db_data`)

Пример: файл уже включён в репозиторий — редактируйте при необходимости.

## Запуск
- Ручной запуск отдельных скриптов:
  - `python beget/server/rss_scraper_investing_to_db_month_msk.py`
  - `python beget/server/rss_scraper_prime_to_db_month_msk.py`
  - `python beget/server/rss_scraper_interfax_to_db_month_msk.py`
  - или единым скриптом `python beget/server/rss_scraper_all_providers_to_db_month_msk.py`

## Логирование
- Логи хранятся в папке `log` рядом со скриптами (настраивается в коде).
- Формат времени логов принудительно приводится к `Europe/Moscow`.

## Структура баз данных
- Каждая запись содержит: `loaded_at`, `date`, `title`, `provider` (в общем скрипте) или `date`, `title` (в специализированных скриптах).
- Файлы БД находятся в папке `base_dir` и имеют имя вида `rss_news_YYYY_MM.db` (или с префиксом `rss_news_investing_`, `rss_news_prime_`, `rss_news_interfax_` в соответствующих скриптах).

## Автоматизация
через cron / systemd timer. 
Пример cron (ежеминутно):
```cron
* * * * * /home/user/rss_scraper/venv/bin/python /home/user/rss_scraper/rss_scraper_all_providers_to_db_month_msk.py
```
```cron
* * * * * /usr/bin/flock -n /tmp/rss_scraper.lock timeout 55s /home/user/rss_scraper/venv/bin/python /home/user/rss_scraper/rss_scraper_all_providers_to_db_month_msk.py > /home/user/rss_scraper/cron.log 2>&1
```
## Отладка и распространённые проблемы
- Сетевая проблема: проверьте доступность RSS-URL и таймауты в коде (aiohttp.ClientTimeout).
- Проблемы с парсингом дат: логируются ошибки парсинга; проверьте формат `pubDate`.
- Права доступа: убедитесь, что пользователь имеет права на запись в `base_dir` и директорию логов.

## Полезные пути в репозитории
- Основной объединённый скрипт: `beget/server/rss_scraper_all_providers_to_db_month_msk.py`
- Скрипты по провайдерам: 
  - `beget/server/rss_scraper_investing_to_db_month_msk.py`, 
  - `beget/server/rss_scraper_interfax_to_db_month_msk.py`, 
  - `beget/server/rss_scraper_prime_to_db_month_msk.py`
- Конфиг: `beget/server/settings.yaml`
- Зависимости: `beget/server/requirements.txt`
