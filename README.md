# pj16_sentiment_gaz_oil_trade

Система торговли фьючерсами на Московской бирже (MOEX) по настроению рынка.

## Как это работает

1. RSS-скраперы собирают новости (Interfax, 1Prime, Investing) в SQLite
2. Локальная LLM (Ollama, `gemma3:12b`) оценивает sentiment каждого торгового дня от −10 до +10
3. Правила из `rules.yaml` преобразуют sentiment в направление: `follow` / `invert` / `skip`
4. Скрипт `sentiment_to_predict.py` записывает файл предсказания `up` / `down` на текущую дату
5. Торговый скрипт `trade_*_tri.py` формирует рыночную заявку в QUIK через .tri-файл

## Структура проекта

```
rts/                        # Пайплайн для фьючерса RTS (RI)
mix/                        # Пайплайн для фьючерса MIX (MX)
trade/                      # Торговые скрипты (TRI для QUIK)
beget/                      # Серверные RSS-скраперы и синхронизация
run_all.py                  # Мастер-скрипт запуска всего пайплайна
html_open.py                # Открытие HTML-отчётов в Chrome
```

## Пайплайн (на примере RTS)

```
download_minutes_to_db.py   → минутные свечи MOEX ISS API → SQLite
convert_minutes_to_days.py  → дневные свечи (окно 21:00–20:59 МСК)
create_markdown_files.py    → .md файл с новостями за торговый день
sentiment_analysis.py       → Ollama → sentiment score → pkl
sentiment_to_predict.py     → pkl + rules.yaml → predict/{date}.txt
sentiment_group_stats.py    → сводка для ручного составления правил
sentiment_backtest.py       → бэктест по rules.yaml → xlsx + HTML
sentiment_walk_forward.py   → walk-forward валидация → xlsx + HTML
trade/trade_rts_tri.py      → predict/{date}.txt → .tri → QUIK
```

## Быстрый старт

```bash
.venv/Scripts/activate
pip install -r requirements.txt

# Полный пайплайн
python run_all.py

# Или поштучно
python rts/sentiment_analysis.py
python rts/sentiment_to_predict.py
python rts/sentiment_backtest.py
```
