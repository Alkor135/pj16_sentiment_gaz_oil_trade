"""
Группирует значения настроений рынка по базовой follow-стратегии и считает по каждому значению:
- count_pos: количество прибыльных дней (next_open_to_open в нужную сторону),
- count_neg: количество убыточных дней,
- total_pnl: суммарный P/L при follow (LONG если s>0, SHORT если s<0).

НЕ использует rules.yaml. Его задача — дать сырую сводку, по которой
пользователь сам составляет правила в rules.yaml. Положительный total_pnl → follow,
отрицательный → invert, ~0 или мало сделок → skip.

P/L берётся из колонки next_open_to_open обогащённого pkl (sentiment_analysis.py).
Поддерживает фильтр по дате: --date-from / --date-to (переопределяют settings.yaml).
"""

import pickle
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
import yaml


def load_yaml_settings(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_sentiment_pkl(settings: dict, folder: Path) -> Path:
    sentiment_path = Path(settings.get("sentiment_output_pkl", "sentiment_scores.pkl"))
    return sentiment_path if sentiment_path.is_absolute() else folder / sentiment_path


def load_sentiment(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise typer.BadParameter(f"Файл sentiment PKL не найден: {path}")
    with path.open("rb") as f:
        data = pickle.load(f)
    df = pd.DataFrame(data)
    required = {"source_date", "sentiment", "next_open_to_open"}
    missing = required - set(df.columns)
    if missing:
        raise typer.BadParameter(
            f"PKL не содержит обязательные колонки: {missing}. "
            "Запусти sentiment_analysis.py, чтобы дополнить pkl колонкой next_open_to_open."
        )
    df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce").dt.date
    df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce")
    df["next_open_to_open"] = pd.to_numeric(df["next_open_to_open"], errors="coerce")
    return df.dropna(subset=["source_date", "sentiment", "next_open_to_open"])


def index_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Индексирует pkl по source_date. В pkl уже один ряд на дату (см. sentiment_analysis.py)."""
    if df["source_date"].duplicated().any():
        dups = df.loc[df["source_date"].duplicated(keep=False), "source_date"].unique()
        raise typer.BadParameter(
            f"В pkl несколько строк за одну дату: {sorted(dups)[:5]}... "
            "Перегенерируй pkl: sentiment_analysis.py теперь хранит одну строку на дату."
        )
    return (
        df.set_index("source_date")[["sentiment", "next_open_to_open"]]
        .sort_index()
    )

app = typer.Typer(help="Сырая группировка sentiment-сделок по значению настроения.")


def _parse_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return pd.to_datetime(str(value)).date()


def build_follow_trades(aggregated: pd.DataFrame, quantity: int) -> pd.DataFrame:
    """Создаёт DataFrame сделок по базовой follow-стратегии (sentiment ≠ 0)."""
    rows = []
    for source_date, row in aggregated.iterrows():
        sentiment = float(row["sentiment"])
        if sentiment == 0.0:
            continue
        next_oto = float(row["next_open_to_open"])
        direction = "LONG" if sentiment > 0 else "SHORT"
        pnl = next_oto * quantity if direction == "LONG" else -next_oto * quantity
        rows.append(
            {
                "source_date": source_date,
                "sentiment": sentiment,
                "direction": direction,
                "next_open_to_open": next_oto,
                "pnl": pnl,
            }
        )
    return pd.DataFrame(rows)


def group_by_sentiment(trades: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        trades.groupby("sentiment")
        .agg(
            count_pos=("pnl", lambda s: int((s > 0).sum())),
            count_neg=("pnl", lambda s: int((s < 0).sum())),
            total_pnl=("pnl", "sum"),
            trades=("pnl", "size"),
        )
        .reset_index()
    )
    full = pd.DataFrame({"sentiment": [float(s) for s in range(-10, 11) if s != 0]})
    grouped = full.merge(grouped, on="sentiment", how="left").fillna(
        {"count_pos": 0, "count_neg": 0, "total_pnl": 0.0, "trades": 0}
    )
    for col in ("count_pos", "count_neg", "trades"):
        grouped[col] = grouped[col].astype(int)
    return grouped.sort_values("sentiment").reset_index(drop=True)


@app.command()
def main(
    settings_yaml: Path = typer.Option(
        Path(__file__).parent / "settings.yaml",
        exists=True,
        help="Локальный settings.yaml для тикера.",
    ),
    quantity: Optional[int] = typer.Option(
        None,
        help="Количество контрактов на сделку. По умолчанию — quantity_test из settings.yaml.",
    ),
    date_from: Optional[str] = typer.Option(
        None,
        "--date-from",
        help="Нижняя граница окна (YYYY-MM-DD). Переопределяет settings.yaml:stats_date_from.",
    ),
    date_to: Optional[str] = typer.Option(
        None,
        "--date-to",
        help="Верхняя граница окна (YYYY-MM-DD). Переопределяет settings.yaml:stats_date_to.",
    ),
) -> None:
    folder = Path(__file__).parent
    settings = load_yaml_settings(settings_yaml)
    ticker = settings.get("ticker", folder.name.upper())

    sentiment_pkl = resolve_sentiment_pkl(settings, folder)
    if quantity is None:
        quantity = int(settings.get("quantity_test", 1))

    # Окно дат: CLI приоритет над settings.yaml
    d_from = _parse_date(date_from if date_from is not None else settings.get("stats_date_from"))
    d_to = _parse_date(date_to if date_to is not None else settings.get("stats_date_to"))

    df = load_sentiment(sentiment_pkl)
    aggregated = index_by_date(df)

    if d_from is not None:
        aggregated = aggregated[aggregated.index >= d_from]
    if d_to is not None:
        aggregated = aggregated[aggregated.index <= d_to]

    if aggregated.empty:
        typer.echo("После фильтра по дате не осталось записей. Проверьте окно.")
        raise typer.Exit(code=1)

    trades = build_follow_trades(aggregated, quantity)
    if trades.empty:
        typer.echo("Нет торгуемых дней (все sentiment == 0?).")
        raise typer.Exit(code=1)

    grouped = group_by_sentiment(trades)

    actual_from = aggregated.index.min()
    actual_to = aggregated.index.max()
    output_dir = folder / "group_stats"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_xlsx = output_dir / f"sentiment_group_stats_{actual_from}_{actual_to}.xlsx"
    grouped.to_excel(output_xlsx, index=False)

    period = f"{aggregated.index.min()} .. {aggregated.index.max()}"
    with pd.option_context(
        "display.width", 1000,
        "display.max_columns", 10,
        "display.max_colwidth", 30,
        "display.float_format", "{:,.2f}".format,
    ):
        typer.echo(
            f"\n{ticker}: follow-статистика по значениям sentiment | период: {period}"
        )
        typer.echo(grouped.to_string(index=False))

    typer.echo(f"\nИтого сделок: {len(trades)}")
    typer.echo(f"Суммарный P/L (чистый follow): {trades['pnl'].sum():.2f}")
    typer.echo(f"XLSX сохранён: {output_xlsx}")
    typer.echo(
        "\nПодсказка: total_pnl > 0 -> в rules.yaml ставь 'follow', "
        "< 0 -> 'invert', ~0 или мало сделок -> 'skip'."
    )


if __name__ == "__main__":
    app()
