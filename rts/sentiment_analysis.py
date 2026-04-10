"""
Собирает sentiment-оценки новостных markdown-файлов через локальную модель Ollama.
Для каждого файла строит промпт, вызывает Ollama HTTP API (/api/generate)
с детерминированными параметрами (temperature=0, top_p=1, top_k=1, seed=42)
и парсит число от -10 до +10. Модель берётся из settings.yaml:sentiment_model.
Результаты копятся в pickle (resume по file_path), колонки: file_path, source_date,
ticker, model, prompt, prompt_tokens, raw_response, sentiment, processed_at.
Гарантирует принцип «одна дата — одна строка» (дедупликация по source_date, keep=last).
Дополнительно обогащает датафрейм колонками date (дата из имени md-файла),
body (CLOSE-OPEN за ту же дату) и next_body (body следующей торговой сессии),
подтягивая котировки из SQLite `path_db_day` для быстрого downstream-анализа.
"""

from __future__ import annotations

import logging
import pickle
import re
import sqlite3
import subprocess

import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import tiktoken
import typer
import yaml

app = typer.Typer(help="Собирает sentiment оценки новостей через локальную модель Ollama.")

DEFAULT_PROMPT_TEMPLATE = (
    "Оцени влияние на {ticker} от -10 до +10.\n\n"
    "Текст новости:\n\n{news_text}\n\n"
    "Верни только одно число от -10 до +10 без пояснений."
)

DEFAULT_TOKEN_LIMIT = 16000
ENC = tiktoken.get_encoding("cl100k_base")
SENTIMENT_REGEX = re.compile(r"(-?\d+(?:[.,]\d+)?)")


def cleanup_old_logs(log_dir: Path, max_files: int = 3) -> None:
    log_files = sorted(log_dir.glob("sentiment_analysis_*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if len(log_files) > max_files:
        for old_file in log_files[max_files:]:
            try:
                old_file.unlink()
            except Exception as exc:
                print(f"Не удалось удалить старый лог {old_file}: {exc}")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    log_dir = Path(__file__).parent / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"sentiment_analysis_{timestamp}.txt"
    log_file.touch(exist_ok=True)
    cleanup_old_logs(log_dir, max_files=3)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.info("Запуск sentiment_analysis. Лог: %s", log_file)


def load_settings() -> dict:
    settings_file = Path(__file__).parent / "settings.yaml"
    with settings_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_md_files(md_dir: Path) -> list[Path]:
    return sorted(p for p in md_dir.rglob("*.md") if p.is_file())


def read_markdown(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace").strip()


def build_prompt(ticker: str, prompt_template: str, news_text: str) -> str:
    return prompt_template.format(ticker=ticker, news_text=news_text)


def get_token_count(text: str) -> int:
    return len(ENC.encode(text))


def warn_if_token_limit_exceeded(prompt: str, token_limit: int, file_name: str) -> int:
    prompt_tokens = get_token_count(prompt)
    if prompt_tokens > token_limit:
        logging.warning(
            "Prompt для %s содержит %s токенов, превышает порог %s. Возможно обрезание или плохой ответ.",
            file_name,
            prompt_tokens,
            token_limit,
        )
    return prompt_tokens


def parse_sentiment(response: str) -> Optional[float]:
    if not response:
        return None
    match = SENTIMENT_REGEX.search(response)
    if not match:
        return None
    value = match.group(1).replace(",", ".")
    try:
        score = float(value)
    except ValueError:
        return None
    return max(min(score, 10.0), -10.0)


def extract_date_from_path(path: Path) -> Optional[str]:
    text = str(path)
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else None


def run_ollama(model: str, prompt: str, keepalive: Optional[str] = None, timeout: int = 600) -> str:
    """Детерминированный вызов Ollama через HTTP API: temperature=0, seed=42."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "top_p": 1,
            "top_k": 1,
            "seed": 42,
        },
    }
    if keepalive:
        payload["keep_alive"] = keepalive
    logging.debug("Ollama HTTP request: model=%s", model)
    response = requests.post(
        "http://localhost:11434/api/generate",
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    return (response.json().get("response") or "").strip()


def load_existing_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    with path.open("rb") as f:
        return pd.DataFrame(pickle.load(f))


def enrich_with_quotes(df: pd.DataFrame, quotes_path: Path) -> pd.DataFrame:
    """Добавляет колонки date/body/next_body по дневным котировкам из SQLite."""
    if df.empty:
        return df
    if not quotes_path.exists():
        logging.warning("Файл котировок не найден: %s — пропускаю enrich", quotes_path)
        return df

    with sqlite3.connect(str(quotes_path)) as conn:
        q = pd.read_sql_query(
            "SELECT TRADEDATE, OPEN, CLOSE FROM Futures",
            conn,
            parse_dates=["TRADEDATE"],
        )

    q = q.dropna(subset=["TRADEDATE", "OPEN", "CLOSE"]).sort_values("TRADEDATE").reset_index(drop=True)
    q["body"] = q["CLOSE"] - q["OPEN"]
    q["date_only"] = q["TRADEDATE"].dt.date

    q_dates = np.array(q["date_only"].tolist())
    q_bodies = q["body"].to_numpy()

    def body_for(d):
        if d is None:
            return None
        idx = np.searchsorted(q_dates, d)
        if idx < len(q_dates) and q_dates[idx] == d:
            return float(q_bodies[idx])
        return None

    def next_body_for(d):
        if d is None:
            return None
        idx = np.searchsorted(q_dates, d, side="right")
        if idx < len(q_dates):
            return float(q_bodies[idx])
        return None

    def parse_date(value):
        if value is None:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").date()
        except ValueError:
            return None

    df = df.copy()
    df["date"] = df["source_date"].apply(parse_date)
    df["body"] = df["date"].apply(body_for)
    df["next_body"] = df["date"].apply(next_body_for)
    return df


def save_results(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(df, f)
    logging.info("Saved %s records to %s", len(df), path)


@app.command()
def main(
    output_pkl: Optional[Path] = typer.Option(
        None,
        help="Файл для сохранения sentiment оценок. Если не задан, берётся из settings.yaml.",
    ),
    model: Optional[str] = typer.Option(
        None,
        help="Локальная модель Ollama. По умолчанию берётся из settings.yaml:sentiment_model.",
    ),
    keepalive: str = typer.Option(
        "5m",
        help="Удерживать модель Ollama загруженной между запросами.",
    ),
    token_limit: int = typer.Option(
        DEFAULT_TOKEN_LIMIT,
        help="Порог токенов для предупреждения о длинном prompt.",
    ),
    prompt_template: str = typer.Option(
        DEFAULT_PROMPT_TEMPLATE,
        help="Шаблон промпта для модели.",
    ),
    resume: bool = typer.Option(
        True,
        help="Пропускать уже обработанные файлы, если PKL существует.",
    ),
    verbose: bool = typer.Option(False, help="Включить подробный лог."),
) -> None:
    setup_logging(verbose)
    settings = load_settings()

    ticker = settings.get("ticker", "RTS")
    if model is None:
        model = settings.get("sentiment_model", "gemma3:12b")
    logging.info("Sentiment model: %s", model)
    md_path = Path(settings.get("md_path", "."))
    sentiment_output = Path(settings.get("sentiment_output_pkl", "sentiment_scores.pkl"))
    if output_pkl is None:
        output_pkl = sentiment_output
    if not output_pkl.is_absolute():
        output_pkl = Path(__file__).parent / output_pkl

    logging.info("Sentiment output PKL: %s", output_pkl)

    if not md_path.exists():
        raise typer.BadParameter(f"Папка markdown файлов не найдена: {md_path}")

    files = find_md_files(md_path)
    if not files:
        raise typer.Exit(code=1, err="В папке не найдено markdown файлов.")

    logging.info("Found %s markdown files in %s", len(files), md_path)

    existing_df = load_existing_results(output_pkl) if resume else pd.DataFrame()
    processed_paths = set(existing_df["file_path"].tolist()) if not existing_df.empty else set()

    rows = existing_df.to_dict("records") if not existing_df.empty else []

    for md_file in files:
        md_file_path = str(md_file.resolve())
        if md_file_path in processed_paths:
            logging.info("Skipping already processed file: %s", md_file.name)
            continue

        logging.info("Processing file: %s", md_file.name)
        news_text = read_markdown(md_file)
        prompt = build_prompt(ticker, prompt_template, news_text)
        prompt_tokens = warn_if_token_limit_exceeded(prompt, token_limit, md_file.name)

        try:
            raw_response = run_ollama(model=model, prompt=prompt, keepalive=keepalive)
            sentiment = parse_sentiment(raw_response)
        except Exception as exc:
            logging.error("Error processing %s: %s", md_file.name, exc)
            raw_response = str(exc)
            sentiment = None

        logging.info(
            "Result %s: sentiment=%s, prompt_tokens=%s",
            md_file.name,
            sentiment,
            prompt_tokens,
        )
        rows.append(
            {
                "file_path": md_file_path,
                "source_date": extract_date_from_path(md_file),
                "ticker": ticker,
                "model": model,
                "prompt": prompt,
                "prompt_tokens": prompt_tokens,
                "raw_response": raw_response,
                "sentiment": sentiment,
                "processed_at": datetime.now(timezone.utc),
            }
        )

    df = pd.DataFrame(rows)

    # Принцип "одна дата — одна строка": если несколько md-файлов дали одну source_date,
    # оставляем последнюю обработанную (она же — самая свежая запись в rows).
    if not df.empty and "source_date" in df.columns:
        before = len(df)
        df = (
            df.sort_values("source_date", kind="stable")
            .drop_duplicates(subset="source_date", keep="last")
            .reset_index(drop=True)
        )
        if len(df) < before:
            logging.info("Дедуп по source_date: %s -> %s строк", before, len(df))

    ticker_lc = settings.get("ticker_lc", ticker.lower())
    path_db_day_tmpl = settings.get("path_db_day", "")
    if path_db_day_tmpl:
        quotes_path = Path(path_db_day_tmpl.format(ticker=ticker, ticker_lc=ticker_lc))
        df = enrich_with_quotes(df, quotes_path)

    save_results(output_pkl, df)
    typer.echo(f"Готово: {len(df)} записей сохранено в {output_pkl}")

    console_cols = ["source_date", "ticker", "model", "sentiment", "body", "next_body", "prompt_tokens"]
    console_df = df[[c for c in console_cols if c in df.columns]]
    typer.echo("\nРезультаты:")
    typer.echo(console_df.to_string(index=False))


if __name__ == "__main__":
    app()
