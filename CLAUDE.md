# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Обзор проекта

Система торговли на Московской бирже (MOEX) по настроению рынка — фьючерсы (RTS, MIX и т.п.). Система собирает новости через RSS, локальная LLM (Ollama) оценивает настроение каждого торгового дня числом от −10 до +10, а далее по правилам из `rules.yaml` формируется направление сделки на следующую сессию. Направление записывается в текстовый файл предсказания, торговый скрипт сравнивает текущее и предыдущее предсказания и формирует .tri-файл для QUIK. Стратегия проверяется бэктестом и walk-forward валидацией.

## Архитектура

Параллельные пайплайны по инструментам (`rts/`, `mix/`, ...), торговые скрипты (`trade/`), серверный сборщик новостей (`beget/`) и мастер-скрипт `run_all.py` в корне. Каждая папка инструмента — самодостаточна: скрипты не импортируют друг друга, все общие помощники инлайн. Общие параметры каждой папки — в её `settings.yaml`.

### Пайплайн данных (на примере `rts/`)

Скрипты запускаются последовательно. Все читают конфигурацию из `settings.yaml` в своей директории.

1. **`download_minutes_to_db.py`** — Скачивает минутные свечи с MOEX ISS API в SQLite. Пагинация, ролловер между контрактами, инкрементальные обновления.

2. **`convert_minutes_to_days.py`** — Агрегирует минутные бары в дневные свечи с нестандартным окном сессии: с 21:00 предыдущего дня до 20:59:59 текущего (МСК). Корректирует цены при ролловере (gap correction).

3. **`create_markdown_files.py`** — Читает новости из SQLite БД (`rss_news_*.db`), фильтрует по ключевым словам ("нефт"/"газ") и провайдеру, группирует по торговым интервалам, создаёт один `.md` файл на торговый день.

4. **`sentiment_analysis.py`** — Для каждого `.md` строит промпт и вызывает Ollama через HTTP API `/api/generate` c детерминированными параметрами (`temperature=0, top_p=1, top_k=1, seed=42`). Модель берётся из `settings.yaml:sentiment_model`. Результат — число от −10 до +10. Гарантирует принцип «одна дата — одна строка» (дедупликация по `source_date`). Обогащает pkl колонками `date`, `body`, `next_body` из дневной SQLite-БД. Resume по `file_path`.

5. **`sentiment_to_predict.py`** — Читает pkl, берёт строку за сегодня, применяет `rules.yaml` → записывает файл `<predict_path>/YYYY-MM-DD.txt` с направлением (`up`/`down`). На `skip` или при отсутствии данных файл не создаётся. Идемпотентен: если файл за дату уже есть — не перезаписывает.

6. **`sentiment_group_stats.py`** — Сырая сводка по значениям sentiment: `count_pos / count_neg / total_pnl` при базовой follow-стратегии. НЕ использует `rules.yaml`; выход — материал для ручного написания правил. Окно дат через `stats_date_from/stats_date_to` или CLI. Сохраняет `group_stats/sentiment_group_stats_<from>_<to>.xlsx`.

7. **`sentiment_backtest.py`** — Бэктест по правилам из `rules.yaml` (`follow`/`invert`/`skip`, матч по первому совпадению). P/L считается только по `next_body` из pkl, без обращения к SQLite. Окно дат через `backtest_date_from/backtest_date_to` или CLI. Выдаёт xlsx со сделками и богатый HTML-отчёт: equity, drawdown, распределения, таблицы статистики и коэффициентов (Sharpe, Sortino, Calmar, PF, RF, Payoff, Expectancy).

8. **`sentiment_walk_forward.py`** — Walk-forward валидация (rolling). На train-окне для каждого целого sentiment вычисляется `sum(sign(v) * next_body)`: положительная сумма → follow, отрицательная → invert, ниже `min_trades`/`threshold` → skip. Применяется на следующем test-окне. Сравнение с in-sample правилами из `rules.yaml` и buy&hold. Выход — xlsx + HTML.

9. **`sentiment_walk_forward_analysis.py`** — Читает xlsx от walk-forward и генерирует расширенный HTML-отчёт (subplot grid, таблицы статистики и коэффициентов).

### Торговля (`trade/`)

- **`trade/trade_rts_tri_SPBFUT192yc_ebs.py`** / **`trade/trade_mix_tri_SPBFUT192yc_ebs.py`** — Читают файл предсказания за сегодня и предыдущий день, сравнивают направления. При смене направления — формируют рыночные заявки в .tri-файл для QUIK (закрытие старой позиции + открытие новой). Поддержка ролловера (смена контракта). Защита от двойной записи через маркер `state/{ticker}_{date}.done`. Конфигурация тикеров из `<инструмент>/settings.yaml`, количество контрактов/путь к QUIK/торговый счёт из `trade/settings.yaml` (аккаунт `ebs`).
- **`trade/settings.yaml`** — Аккаунты (`accounts → ebs/iis`), каждый содержит `trade_path`, `trade_account` и количество контрактов по инструментам (`rts`/`mix` → `quantity_close`/`quantity_open`).

### Утилиты

- **`check_pkl.py`** — Просмотр содержимого `sentiment_scores.pkl` в консоли (shape, колонки, период, сам df).
- **`html_open.py`** — Открытие HTML-отчётов бэктестов в новом окне Google Chrome.
- **`run_all.py`** — Мастер-скрипт для последовательного запуска всего пайплайна по всем тикерам. Останавливается при первой ошибке. Запускается Планировщиком задач.

### Серверная часть (`beget/`)

- **`beget/server/`** — Асинхронные RSS-скраперы (Interfax, 1Prime, Investing) на Linux-сервере (хостинг Beget). Сохраняют в помесячные SQLite-файлы. Конфиг: `beget/server/settings.yaml`. Деплой через cron.
- **`beget/sync_files.py`** — Синхронизация SQLite БД и логов с удалённого сервера на локальный Windows через WSL rsync.
- **`beget/collect_rss_links_to_yaml.py`** — Сбор RSS-ссылок с Investing.com в YAML.

## Конфигурация

Каждая директория инструмента имеет свой `settings.yaml`:
- `ticker` / `ticker_lc` / `ticker_close` / `ticker_open` — идентификаторы фьючерсных контрактов
- `quantity_test` — количество контрактов для бэктеста и аналитики
- `sentiment_model` — модель Ollama для оценки настроения (например, `gemma3:12b`)
- `provider` — фильтр источников новостей (`investing`, `prime_interfax`, `investing_prime_interfax`)
- Пути к БД, markdown-файлам, pkl и predict-папке (плейсхолдеры `{ticker}` / `{ticker_lc}`)
- `time_start` / `time_end` — границы торговой сессии (по умолчанию: 21:00:00 / 20:59:59)
- `stats_date_from` / `stats_date_to` — окно для `sentiment_group_stats.py`
- `backtest_date_from` / `backtest_date_to` — окно для `sentiment_backtest.py`

`rules.yaml` — список правил `{min, max, action}`, где `action ∈ {follow, invert, skip}`. Матчинг по первому совпадению.

`trade/settings.yaml` — аккаунты (`accounts → ebs/iis`), каждый содержит `trade_path`, `trade_account` и количество контрактов по инструментам.

## Основные команды

```bash
# Активация виртуального окружения
.venv/Scripts/activate    # Windows PowerShell
source .venv/bin/activate # WSL/Linux

pip install -r requirements.txt

# Полный пайплайн (все тикеры)
python run_all.py

# Запуск пайплайна для инструмента (пример: RTS)
python rts/download_minutes_to_db.py
python rts/convert_minutes_to_days.py
python rts/create_markdown_files.py
python rts/sentiment_analysis.py
python rts/sentiment_to_predict.py
python rts/sentiment_group_stats.py         # → rules.yaml (вручную)
python rts/sentiment_backtest.py
python rts/sentiment_walk_forward.py
python rts/sentiment_walk_forward_analysis.py

# Торговля
python trade/trade_rts_tri_SPBFUT192yc_ebs.py

# Просмотр pkl
python rts/check_pkl.py

# Открытие HTML-отчётов
python html_open.py

# Синхронизация БД новостей с сервера
python beget/sync_files.py
```

## Ключевые технические детали

- **Sentiment-оценка**: Ollama HTTP API (`http://localhost:11434/api/generate`), детерминированные параметры (`temperature=0, top_p=1, top_k=1, seed=42`). Модель — из `settings.yaml:sentiment_model`.
- **Одна дата — одна строка**: `sentiment_analysis.py` дедуплицирует pkl по `source_date`. Downstream-скрипты (`backtest`, `group_stats`, `walk_forward`, `to_predict`) проверяют уникальность и падают с ошибкой при дубликатах.
- **Самодостаточные папки**: Каждая папка инструмента содержит полный набор скриптов и конфигов, кросс-импортов между папками нет.
- **Базы данных**: Все данные в SQLite. Минутные бары, дневные бары и новости — в отдельных `.db` файлах. Пути в `settings.yaml`.
- **Торговля через TRI**: Рыночные заявки формируются в .tri-файл, который QUIK читает и отправляет на биржу. Защита от двойной записи через файл-маркер `trade/state/{ticker}_{date}.done`.
- **Логирование**: Скрипты создают лог-файлы с таймстемпом в `<папка>/log/`, автоматическая ротация (хранятся 3 последних).
- **Окно сессии**: Дневные свечи строятся с 21:00 предыдущего дня до 20:59:59 текущего (МСК), а не от полуночи до полуночи.
- **Рекомендуемый цикл**: обновить данные → `sentiment_analysis.py` → `sentiment_to_predict.py` → `sentiment_group_stats.py` → вручную правила в `rules.yaml` → `sentiment_backtest.py` (визуально оценить equity) → `sentiment_walk_forward.py` для проверки устойчивости → `trade_*_tri.py` для реальной торговли.
