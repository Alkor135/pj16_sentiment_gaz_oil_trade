# RTS — пайплайн sentiment-стратегии

Торговля фьючерсом RTS на основе настроения рынка по новостям. Все скрипты читают параметры из `settings.yaml` в этой же папке.

## Порядок запуска

```bash
# 1. Подготовка котировок
python rts/download_minutes_to_db.py     # минутные свечи с MOEX ISS -> SQLite
python rts/convert_minutes_to_days.py    # агрегация в дневные (окно сессии 21:00..20:59:59 МСК)

# 2. Подготовка новостей
python rts/create_markdown_files.py      # фильтр новостей из rss_news БД -> один .md на торговый день

# 3. Оценка настроения
python rts/sentiment_analysis.py         # Ollama (модель из settings.yaml:sentiment_model) ставит
                                         # sentiment -10..+10 на каждый день (HTTP API, temperature=0, seed=42)
                                         # параллельно обогащает pkl колонками date/body/next_body

# 4. Анализ и бэктест
python rts/sentiment_group_stats.py      # сырая статистика по значениям sentiment -> xlsx
                                         # использовать для ручного написания rules.yaml
python rts/sentiment_backtest.py         # бэктест по rules.yaml -> xlsx + HTML-отчёт
python rts/sentiment_walk_forward.py     # walk-forward валидация (auto-fit правил) -> xlsx + HTML
python rts/sentiment_walk_forward_analysis.py  # расширенный HTML-отчёт по walk-forward
```

## Описание скриптов

| Скрипт | Назначение | Вход | Выход |
|---|---|---|---|
| `download_minutes_to_db.py` | Скачивает минутные свечи с MOEX ISS API, поддерживает ролловер и инкрементальные обновления | MOEX ISS | `RTS_futures_minute_*.db` |
| `convert_minutes_to_days.py` | Собирает дневные свечи из минутных с коррекцией gap при ролловере | минутная БД | дневная БД |
| `create_markdown_files.py` | Группирует новости из RSS БД по торговым интервалам, фильтрует по ключевым словам и провайдеру | `rss_news_*.db` | `.md` файлы |
| `sentiment_analysis.py` | Ollama-LLM (HTTP API, детерминированно) оценивает настроение каждого дня, pkl обогащается `body`/`next_body` из дневной БД | `.md` + дневная БД | `sentiment_scores.pkl` |
| `sentiment_group_stats.py` | Сырая сводка по значениям sentiment (count_pos/count_neg/total_pnl) для ручного составления правил | `sentiment_scores.pkl` | `group_stats/sentiment_group_stats_<from>_<to>.xlsx` |
| `sentiment_backtest.py` | Бэктест по правилам из `rules.yaml` (follow/invert/skip), богатый HTML-отчёт со статистикой | `sentiment_scores.pkl`, `rules.yaml` | `sentiment_backtest_results.xlsx`, `plots/sentiment_backtest.html` |
| `sentiment_walk_forward.py` | Walk-forward с авто-подбором правил на train-окне и применением на test-окне | `sentiment_scores.pkl` | `sentiment_walk_forward_results.xlsx`, `sentiment_walk_forward_folds.xlsx`, `plots/sentiment_walk_forward.html` |
| `sentiment_walk_forward_analysis.py` | Расширенный HTML-отчёт по результатам walk-forward (subplot grid, коэффициенты) | xlsx от walk-forward | `plots/sentiment_walk_forward_analysis.html` |
| `check_pkl.py` | Просмотр содержимого `sentiment_scores.pkl` в консоли | `sentiment_scores.pkl` | stdout |

## Конфигурация

- `settings.yaml` — тикер, пути к БД/pkl/md, `sentiment_model` (Ollama), провайдер новостей, окно торговой сессии, `stats_date_from`/`stats_date_to` (окно для `sentiment_group_stats.py`), `backtest_date_from`/`backtest_date_to` (окно для `sentiment_backtest.py`).
- `rules.yaml` — список правил `{min, max, action}`, где `action ∈ {follow, invert, skip}`. Матчинг по первому совпадению.

## Рекомендуемый цикл работы

1. Обновить данные (шаги 1–3).
2. Запустить `sentiment_group_stats.py`, посмотреть xlsx — какие значения sentiment дают устойчивый + или −.
3. Отредактировать `rules.yaml` под наблюдаемую статистику.
4. Запустить `sentiment_backtest.py`, визуально оценить equity curve в HTML-отчёте.
5. Проверить устойчивость через `sentiment_walk_forward.py` + `sentiment_walk_forward_analysis.py`.
