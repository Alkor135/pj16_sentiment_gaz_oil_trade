from __future__ import annotations

import logging
import pickle
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
    command = ["ollama", "run", model]
    if keepalive:
        command.extend(["--keepalive", keepalive])
    command.append(prompt)
    logging.debug("Ollama command: %s", command)
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Ollama returned code {result.returncode}: {error_message}")

    return result.stdout.strip()


def load_existing_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    with path.open("rb") as f:
        return pd.DataFrame(pickle.load(f))


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
    model: str = typer.Option("gemma3:12b", help="Локальная модель Ollama."),
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
    save_results(output_pkl, df)
    typer.echo(f"Готово: {len(df)} записей сохранено в {output_pkl}")

    console_df = df[["file_path", "source_date", "ticker", "model", "sentiment", "prompt_tokens"]]
    typer.echo("\nРезультаты:")
    typer.echo(console_df.to_string(index=False))


if __name__ == "__main__":
    app()
