# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Обзор проекта

Система торговли на Московской бирже (MOEX) по настроению рынка — фьючерсы RTS, Brent (BR), Природный газ (NG). Система собирает новости через RSS, преобразует их в эмбеддинги через Ollama, и проводит бэктест стратегии, предсказывающей направление следующей сессии на основе косинусного сходства чанков новостных эмбеддингов.

## Архитектура

Три идентичных пайплайна по инструментам (`rts/`, `br/`, `ng/`) и серверный сборщик новостей (`beget/`).

### Пайплайн данных (на примере `rts/`)

Скрипты запускаются последовательно. Все читают конфигурацию из `settings.yaml` в своей директории.

1. **`download_minutes_to_db.py`** — Скачивает минутные свечи с MOEX ISS API в SQLite (`{TICKER}_futures_minute_2025.db`). Обрабатывает пагинацию, ролловер между контрактами, инкрементальные обновления.

2. **`convert_minutes_to_days.py`** — Агрегирует минутные бары в дневные свечи с нестандартным окном сессии: с 21:00 предыдущего дня до 20:59:59 текущего (МСК). Корректирует цены при ролловере (gap correction).

3. **`create_markdown_files.py`** — Читает новости из SQLite БД (`rss_news_*.db`), фильтрует по ключевым словам ("нефт"/"газ") и провайдеру, группирует по торговым интервалам, создаёт один `.md` файл на торговый день.

4. **`create_embedding.py`** — Строит кэш эмбеддингов (pickle). Разбивает текст на чанки по параграфам с учётом лимита токенов, генерирует эмбеддинги через Ollama (`OllamaEmbeddingFunction`), L2-нормализует вектора. Инкрементально: пропускает неизменённые файлы по MD5.

5. **`simulate_trade.py`** — Бэктест стратегии. Для каждой даты перебирает окна k=3..30, ищет наиболее похожий исторический день по косинусному сходству чанков, формирует P/L по совпадению направлений. Выбирает лучшее k по скользящей сумме P/L за test_days дней. Применяет инверсию стратегии (зеркальный P/L). Выводит Excel, графики и предсказание на следующую сессию.

6. **`strategy_analysis.py`** — Генерирует интерактивный HTML-отчёт на Plotly: дневной/недельный/месячный/годовой P/L, drawdown, скользящие средние, ключевые метрики (Sharpe, Sortino, Calmar, Profit Factor, Recovery Factor).

### Серверная часть (`beget/`)

- **`beget/server/`** — Асинхронные RSS-скраперы (Interfax, 1Prime, Investing) на Linux-сервере (хостинг Beget). Сохраняют в помесячные SQLite-файлы. Конфиг: `beget/server/settings.yaml`. Деплой через cron.
- **`beget/sync_files.py`** — Синхронизация SQLite БД и логов с удалённого сервера на локальный Windows через WSL rsync.
- **`beget/collect_rss_links_to_yaml.py`** — Сбор RSS-ссылок с Investing.com в YAML.

### Утилиты

- **`rts/check_pkl.py`** — Инспекция pickle-кэша эмбеддингов (количество чанков, размерность, пример текста).

## Конфигурация

Каждая директория инструмента имеет свой `settings.yaml`:
- `ticker` / `ticker_close` / `ticker_open` — идентификаторы фьючерсных контрактов
- `model_name` — модель эмбеддингов Ollama (`embeddinggemma`, `bge-m3`, `qwen3-embedding:0.6b`)
- `provider` — фильтр источников новостей (`investing`, `prime_interfax`, `investing_prime_interfax`)
- Пути к БД, markdown-файлам, предсказаниям и кэшу эмбеддингов (плейсхолдеры `{ticker}` / `{ticker_lc}`)
- `time_start` / `time_end` — границы торговой сессии (по умолчанию: 21:00:00 / 20:59:59)
- `test_days`, `start_date_test`, `start_date_download_minutes` — параметры бэктеста

## Основные команды

```bash
# Активация виртуального окружения
.venv/Scripts/activate    # Windows PowerShell
source .venv/bin/activate # WSL/Linux

# Установка зависимостей
pip install -r requirements.txt

# Запуск пайплайна для инструмента (пример: RTS)
python rts/download_minutes_to_db.py
python rts/convert_minutes_to_days.py
python rts/create_markdown_files.py
python rts/create_embedding.py
python rts/simulate_trade.py
python rts/strategy_analysis.py

# Проверка кэша эмбеддингов
python rts/check_pkl.py

# Синхронизация БД новостей с сервера
python beget/sync_files.py

# Запуск RSS-скрапера (на сервере)
python beget/server/rss_scraper_all_providers_to_db_month_msk.py
```

## Ключевые технические детали

- **Эмбеддинги**: Ollama запущен локально (`http://localhost:11434/api/embeddings`). Вектора L2-нормализованы. Используется `OllamaEmbeddingFunction` из ChromaDB.
- **Сходство**: Косинусное сходство чанк-к-чанку через матричное умножение (`A @ B.T`), усреднение по top-k.
- **Базы данных**: Все данные в SQLite. Минутные бары, дневные бары и новости — в отдельных `.db` файлах. Пути в `settings.yaml`.
- **Логирование**: Каждый скрипт создаёт лог-файл с таймстемпом в `<инструмент>/log/`, автоматическая ротация (хранятся 3 последних).
- **Инверсия стратегии**: Финальный P/L умножается на -1 (контрарный сигнал).
- **Окно сессии**: Дневные свечи строятся с 21:00 предыдущего дня до 20:59:59 текущего (МСК), а не от полуночи до полуночи.
