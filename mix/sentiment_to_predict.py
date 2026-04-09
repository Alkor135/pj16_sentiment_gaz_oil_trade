"""
Генерирует файл предсказания направления цены на текущую торговую дату.

Читает sentiment_scores.pkl, берёт строку(и) за сегодня, агрегирует sentiment как mean
(совместимо с sentiment_backtest.py), применяет правила из rules.yaml и пишет
текстовый файл <predict_path>/YYYY-MM-DD.txt в формате:

    Дата: 2026-04-09
    Sentiment: -4.00
    Action: invert
    Предсказанное направление: up

Логика:
- action == follow: sentiment > 0 -> up, < 0 -> down, == 0 -> skip
- action == invert: sentiment > 0 -> down, < 0 -> up, == 0 -> skip
- action == skip:   skip
На skip файл не создаётся. Если файл за сегодня уже есть — не перезаписываем.
"""

from __future__ import annotations

import logging
import pickle
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml


VALID_ACTIONS = {"follow", "invert", "skip"}


def cleanup_old_logs(log_dir: Path, max_files: int = 3) -> None:
    log_files = sorted(
        log_dir.glob("sentiment_to_predict_*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in log_files[max_files:]:
        try:
            old.unlink()
        except Exception as exc:
            print(f"Не удалось удалить старый лог {old}: {exc}")


def setup_logging() -> logging.Logger:
    log_dir = Path(__file__).parent / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"sentiment_to_predict_{timestamp}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    cleanup_old_logs(log_dir)
    return logging.getLogger(__name__)


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_rules(path: Path) -> list[dict]:
    data = load_yaml(path)
    rules = data.get("rules") or []
    if not isinstance(rules, list) or not rules:
        raise ValueError(f"В {path} нет списка 'rules' или он пустой")
    for i, rule in enumerate(rules):
        for key in ("min", "max", "action"):
            if key not in rule:
                raise ValueError(f"Правило #{i} без поля '{key}': {rule}")
        if rule["action"] not in VALID_ACTIONS:
            raise ValueError(
                f"Правило #{i}: action должен быть одним из {sorted(VALID_ACTIONS)}, "
                f"получено {rule['action']!r}"
            )
        if float(rule["min"]) > float(rule["max"]):
            raise ValueError(f"Правило #{i}: min > max ({rule})")
    return rules


def match_action(sentiment: float, rules: list[dict]) -> str:
    for rule in rules:
        if float(rule["min"]) <= sentiment <= float(rule["max"]):
            return rule["action"]
    return "skip"


def resolve_direction(sentiment: float, action: str) -> str:
    if action == "skip" or sentiment == 0.0:
        return "skip"
    if action == "follow":
        return "up" if sentiment > 0 else "down"
    if action == "invert":
        return "down" if sentiment > 0 else "up"
    return "skip"


def get_today_sentiment(pkl_path: Path, today: date) -> float | None:
    """Возвращает значение sentiment за сегодня или None, если строки нет.
    В pkl должна быть одна строка на дату (см. sentiment_analysis.py)."""
    if not pkl_path.exists():
        raise FileNotFoundError(f"Файл sentiment PKL не найден: {pkl_path}")
    with pkl_path.open("rb") as f:
        data = pickle.load(f)

    df = pd.DataFrame(data)
    if "source_date" not in df.columns or "sentiment" not in df.columns:
        raise ValueError(
            f"PKL не содержит обязательные колонки 'source_date'/'sentiment': {pkl_path}"
        )

    df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce").dt.date
    df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce")
    df = df.dropna(subset=["source_date", "sentiment"])

    today_rows = df[df["source_date"] == today]
    if today_rows.empty:
        return None
    if len(today_rows) > 1:
        raise ValueError(
            f"В pkl несколько строк за {today}: ожидалась одна. "
            "Перегенерируй pkl: sentiment_analysis.py теперь хранит одну строку на дату."
        )
    return float(today_rows["sentiment"].iloc[0])


def main() -> int:
    logger = setup_logging()

    folder = Path(__file__).parent
    settings = load_yaml(folder / "settings.yaml")
    rules = load_rules(folder / "rules.yaml")

    ticker_lc = settings.get("ticker_lc", folder.name)
    predict_path = Path(settings["predict_path"].format(ticker_lc=ticker_lc))
    predict_path.mkdir(parents=True, exist_ok=True)

    pkl_name = settings.get("sentiment_output_pkl", "sentiment_scores.pkl")
    pkl_path = Path(pkl_name)
    if not pkl_path.is_absolute():
        pkl_path = folder / pkl_path

    today = date.today()
    out_file = predict_path / f"{today.strftime('%Y-%m-%d')}.txt"

    if out_file.exists():
        logger.info(f"Файл {out_file} уже существует — пропуск.")
        return 0

    sentiment = get_today_sentiment(pkl_path, today)
    if sentiment is None:
        logger.info(f"В pkl нет записи за {today}. Файл предсказания не создаётся.")
        return 0

    action = match_action(sentiment, rules)
    direction = resolve_direction(sentiment, action)

    logger.info(f"{today}: sentiment={sentiment:.2f}, action={action}, direction={direction}")

    if direction == "skip":
        logger.info("Направление = skip. Файл предсказания не создаётся.")
        return 0

    content = (
        f"Дата: {today.strftime('%Y-%m-%d')}\n"
        f"Sentiment: {sentiment:.2f}\n"
        f"Action: {action}\n"
        f"Предсказанное направление: {direction}\n"
    )

    tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
    tmp_file.write_text(content, encoding="utf-8")
    tmp_file.replace(out_file)

    logger.info(f"Записан файл предсказания: {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
