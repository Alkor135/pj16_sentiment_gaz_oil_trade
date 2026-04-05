from __future__ import annotations

import math
import pickle
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import typer
import yaml

app = typer.Typer(help="""Backtest sentiment-стратегии для следующей торговой сессии.""")


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
    if "source_date" not in df.columns or "sentiment" not in df.columns:
        raise typer.BadParameter("PKL должен содержать колонки source_date и sentiment")

    df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce").dt.date
    df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce")
    return df.dropna(subset=["source_date", "sentiment"])


def aggregate_sentiment(df: pd.DataFrame, method: str) -> pd.DataFrame:
    methods = {
        "mean": df.groupby("source_date")["sentiment"].mean,
        "median": df.groupby("source_date")["sentiment"].median,
        "max": df.groupby("source_date")["sentiment"].max,
        "min": df.groupby("source_date")["sentiment"].min,
    }
    if method not in methods:
        raise typer.BadParameter("Метод агрегации должен быть: mean, median, max или min")

    aggregated = methods[method]()
    return aggregated.to_frame("sentiment").sort_index()


def load_quotes(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise typer.BadParameter(f"Файл дневных котировок не найден: {path}")

    with sqlite3.connect(str(path)) as conn:
        df = pd.read_sql_query(
            "SELECT TRADEDATE, OPEN, CLOSE FROM Futures",
            conn,
            parse_dates=["TRADEDATE"],
        )

    df = df.dropna(subset=["TRADEDATE", "OPEN", "CLOSE"]).sort_values("TRADEDATE")
    return df.set_index("TRADEDATE")


def find_next_trade_date(quotes: pd.DatetimeIndex, current_date: date) -> Optional[pd.Timestamp]:
    candidates = quotes[quotes > pd.Timestamp(current_date)]
    return candidates[0] if len(candidates) else None


def format_db_path(db_template: str, ticker: str, ticker_lc: str) -> Path:
    return Path(db_template.format(ticker=ticker, ticker_lc=ticker_lc))


def build_backtest(aggregated: pd.DataFrame, quotes: pd.DataFrame, quantity: int) -> pd.DataFrame:
    rows = []
    for source_date, row in aggregated.itertuples():
        sentiment = float(row)
        if sentiment == 0.0:
            continue

        trade_date = find_next_trade_date(quotes.index, source_date)
        if trade_date is None:
            continue

        quote = quotes.loc[trade_date]
        points = float(quote["CLOSE"] - quote["OPEN"])
        pnl = points * quantity if sentiment > 0 else -points * quantity
        direction = "LONG" if sentiment > 0 else "SHORT"

        rows.append(
            {
                "source_date": source_date,
                "trade_date": trade_date.date(),
                "sentiment": sentiment,
                "direction": direction,
                "open": float(quote["OPEN"]),
                "close": float(quote["CLOSE"]),
                "points": points,
                "quantity": quantity,
                "pnl": pnl,
            }
        )

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values(["trade_date", "source_date"]).reset_index(drop=True)
    result["cum_pnl"] = result["pnl"].cumsum()
    result["return_%"] = result["points"] / result["open"] * 100.0
    return result


def build_report(result: pd.DataFrame, ticker: str, output_html: Path) -> None:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=result["trade_date"],
            y=result["pnl"],
            name="PnL на сделку",
            marker_color="#4f81bd",
            opacity=0.6,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=result["trade_date"],
            y=result["cum_pnl"],
            mode="lines+markers",
            name="Кумулятивный PnL",
            line=dict(color="#c0504d", width=3),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=f"{ticker}: sentiment backtest",
        xaxis=dict(title="Дата торговли"),
        yaxis=dict(title="PnL на сделку"),
        yaxis2=dict(title="Кумулятивный PnL", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.01),
        template="plotly_white",
        margin=dict(l=40, r=40, t=60, b=40),
    )

    stats = {
        "Всего сделок": len(result),
        "Общий PnL": f"{result['pnl'].sum():.2f}",
        "Средний PnL": f"{result['pnl'].mean():.2f}",
        "Профитных": int((result['pnl'] > 0).sum()),
        "Убыточных": int((result['pnl'] < 0).sum()),
        "Winrate": f"{(result['pnl'] > 0).mean() * 100:.1f}%",
        "Макс. просадка": f"{_max_drawdown(result):.2f}",
    }

    table = go.Figure(
        go.Table(
            header=dict(
                values=[f"<b>{k}</b>" for k in stats.keys()],
                fill_color="#1565c0",
                font=dict(color="white", size=13),
                align="left",
                height=36,
            ),
            cells=dict(
                values=[[v for v in stats.values()]],
                fill_color="#f9f9f9",
                align="left",
                font=dict(color="#111", size=12),
                height=40,
            ),
        )
    )
    table.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=220)

    output_html.parent.mkdir(parents=True, exist_ok=True)
    charts_html = fig.to_html(include_plotlyjs="cdn", full_html=False)

    with output_html.open("w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
        f.write(f"<title>{ticker} sentiment backtest</title>\n</head><body>\n")
        f.write(f"<h2>{ticker}: sentiment backtest</h2>\n")
        f.write(charts_html)
        f.write("\n<hr style='margin:24px 0; border:1px solid #ccc'>\n")
        f.write(table.to_html(include_plotlyjs=False, full_html=False))
        f.write("\n</body></html>")


def _max_drawdown(result: pd.DataFrame) -> float:
    series = result["cum_pnl"]
    peak = series.cummax()
    drawdown = series - peak
    return float(drawdown.min())


@app.command()
def main(
    settings_yaml: Path = typer.Option(
        Path(__file__).parent / "settings.yaml",
        exists=True,
        help="Локальный settings.yaml для тикера.",
    ),
    aggregate_method: str = typer.Option(
        "mean",
        help="Метод агрегации sentiment по дате: mean, median, max, min.",
    ),
    quantity: Optional[int] = typer.Option(
        None,
        help="Количество контрактов на одну сделку. Если не задано, берётся из settings.yaml quantity_open.",
    ),
) -> None:
    folder = Path(__file__).parent
    settings = load_yaml_settings(settings_yaml)
    ticker = settings.get("ticker", folder.name.upper())
    ticker_lc = settings.get("ticker_lc", ticker.lower())

    sentiment_pkl = resolve_sentiment_pkl(settings, folder)
    if quantity is None:
        quantity = int(settings.get("quantity_open", 1))

    db_path = format_db_path(settings.get("path_db_day", ""), ticker=ticker, ticker_lc=ticker_lc)
    result = build_backtest(
        aggregate_sentiment(load_sentiment(sentiment_pkl), aggregate_method),
        load_quotes(db_path),
        quantity,
    )

    if result.empty:
        typer.echo("Нет доступных сделок для бэктеста. Проверьте даты и sentiment-проекты.")
        raise typer.Exit(code=1)

    report_folder = folder / "plots"
    output_html = report_folder / "sentiment_backtest.html"
    output_csv = folder / "sentiment_backtest_results.csv"
    result.to_csv(output_csv, index=False)
    build_report(result, ticker, output_html)

    typer.echo(f"Готово: {output_csv} и {output_html}")
    typer.echo(f"Всего сделок: {len(result)}")
    typer.echo(f"Общий PnL: {result['pnl'].sum():.2f}")
    typer.echo(f"Winrate: {(result['pnl'] > 0).mean() * 100:.1f}%")
    typer.echo(f"Макс. просадка: {_max_drawdown(result):.2f}")


if __name__ == "__main__":
    app()
