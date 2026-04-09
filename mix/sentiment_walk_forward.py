"""
Walk-forward валидация sentiment-стратегии (rolling).
На train-окне для каждого целого значения sentiment вычисляется sum(sign(v) * next_body):
положительная сумма → follow, отрицательная → invert, ниже min_trades или threshold → skip.
Выведенные правила применяются к следующему test-окну. Окно скользит с заданным шагом.
Out-of-sample сделки всех test-окон склеиваются в единую equity curve.
Сравнивает walk-forward P/L с in-sample правилами из rules.yaml и с buy & hold next_body.
Читает обогащённый pkl (sentiment_analysis.py), ничего не джойнит с SQLite.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import typer
import yaml


VALID_ACTIONS = {"follow", "invert", "skip"}


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
    required = {"source_date", "sentiment", "next_body"}
    missing = required - set(df.columns)
    if missing:
        raise typer.BadParameter(
            f"PKL не содержит обязательные колонки: {missing}. "
            "Запусти sentiment_analysis.py, чтобы дополнить pkl колонками body/next_body."
        )
    df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce").dt.date
    df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce")
    df["next_body"] = pd.to_numeric(df["next_body"], errors="coerce")
    return df.dropna(subset=["source_date", "sentiment", "next_body"])


def index_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Индексирует pkl по source_date. В pkl уже один ряд на дату (см. sentiment_analysis.py)."""
    if df["source_date"].duplicated().any():
        dups = df.loc[df["source_date"].duplicated(keep=False), "source_date"].unique()
        raise typer.BadParameter(
            f"В pkl несколько строк за одну дату: {sorted(dups)[:5]}... "
            "Перегенерируй pkl: sentiment_analysis.py теперь хранит одну строку на дату."
        )
    return (
        df.set_index("source_date")[["sentiment", "next_body"]]
        .sort_index()
    )


def load_rules(path: Path) -> list[dict]:
    if not path.exists():
        raise typer.BadParameter(f"Rules-yaml не найден: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    rules = data.get("rules") or []
    if not isinstance(rules, list) or not rules:
        raise typer.BadParameter(f"В {path} нет списка 'rules' или он пустой")
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise typer.BadParameter(f"Правило #{i} должно быть объектом: {rule}")
        for key in ("min", "max", "action"):
            if key not in rule:
                raise typer.BadParameter(f"Правило #{i} без поля '{key}': {rule}")
        if rule["action"] not in VALID_ACTIONS:
            raise typer.BadParameter(
                f"Правило #{i}: action должен быть одним из {sorted(VALID_ACTIONS)}, получено {rule['action']!r}"
            )
        if float(rule["min"]) > float(rule["max"]):
            raise typer.BadParameter(f"Правило #{i}: min > max ({rule})")
    return rules


def match_action(sentiment: float, rules: list[dict]) -> str:
    for rule in rules:
        if float(rule["min"]) <= sentiment <= float(rule["max"]):
            return rule["action"]
    return "skip"


def build_backtest(aggregated: pd.DataFrame, quantity: int, rules: list[dict]) -> pd.DataFrame:
    rows = []
    for source_date, row in aggregated.iterrows():
        sentiment = float(row["sentiment"])
        next_body = float(row["next_body"])
        action = match_action(sentiment, rules)
        if action == "skip" or sentiment == 0.0:
            continue
        if action == "follow":
            direction = "LONG" if sentiment > 0 else "SHORT"
        else:
            direction = "SHORT" if sentiment > 0 else "LONG"
        pnl = next_body * quantity if direction == "LONG" else -next_body * quantity
        rows.append({
            "source_date": source_date,
            "sentiment": sentiment,
            "action": action,
            "direction": direction,
            "next_body": next_body,
            "quantity": quantity,
            "pnl": pnl,
        })
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows).sort_values("source_date").reset_index(drop=True)
    result["cum_pnl"] = result["pnl"].cumsum()
    return result

app = typer.Typer(help="Walk-forward валидация sentiment-стратегии.")


def fit_rules(train_df: pd.DataFrame, min_trades: int, threshold: float) -> dict[float, str]:
    """По train-окну решает для каждого целого sentiment: follow, invert или skip."""
    fitted: dict[float, str] = {}
    for s_value, group in train_df.groupby("sentiment"):
        if s_value == 0.0:
            continue
        if len(group) < min_trades:
            continue
        direction_sign = 1.0 if s_value > 0 else -1.0
        sum_follow = float((direction_sign * group["next_body"]).sum())
        if sum_follow > threshold:
            fitted[float(s_value)] = "follow"
        elif sum_follow < -threshold:
            fitted[float(s_value)] = "invert"
        # иначе не включаем (skip)
    return fitted


def apply_fitted_rules(test_df: pd.DataFrame, fitted: dict[float, str], quantity: int) -> pd.DataFrame:
    rows = []
    for source_date, row in test_df.iterrows():
        s = float(row["sentiment"])
        nb = float(row["next_body"])
        action = fitted.get(s, "skip")
        if action == "skip" or s == 0.0:
            continue
        if action == "follow":
            direction = "LONG" if s > 0 else "SHORT"
        else:
            direction = "SHORT" if s > 0 else "LONG"
        pnl = nb * quantity if direction == "LONG" else -nb * quantity
        rows.append(
            {
                "source_date": source_date,
                "sentiment": s,
                "action": action,
                "direction": direction,
                "next_body": nb,
                "quantity": quantity,
                "pnl": pnl,
            }
        )
    return pd.DataFrame(rows)


def walk_forward(
    aggregated: pd.DataFrame,
    train_size: int,
    test_size: int,
    step: int,
    min_trades: int,
    threshold: float,
    quantity: int,
) -> tuple[pd.DataFrame, list[dict]]:
    n = len(aggregated)
    folds: list[dict] = []
    test_trades: list[pd.DataFrame] = []

    start = 0
    fold_idx = 0
    while start + train_size + test_size <= n:
        fold_idx += 1
        train = aggregated.iloc[start : start + train_size]
        test = aggregated.iloc[start + train_size : start + train_size + test_size]

        fitted = fit_rules(train, min_trades, threshold)
        test_result = apply_fitted_rules(test, fitted, quantity)

        folds.append(
            {
                "fold": fold_idx,
                "train_from": train.index[0],
                "train_to": train.index[-1],
                "test_from": test.index[0],
                "test_to": test.index[-1],
                "n_rules": len(fitted),
                "n_trades": len(test_result),
                "pnl": float(test_result["pnl"].sum()) if not test_result.empty else 0.0,
                "fitted_rules": dict(sorted(fitted.items())),
            }
        )

        if not test_result.empty:
            test_result = test_result.copy()
            test_result["fold"] = fold_idx
            test_trades.append(test_result)

        start += step

    if test_trades:
        combined = pd.concat(test_trades, ignore_index=True).sort_values("source_date").reset_index(drop=True)
        combined["cum_pnl"] = combined["pnl"].cumsum()
    else:
        combined = pd.DataFrame()

    return combined, folds


def summarize(result: pd.DataFrame, label: str) -> dict:
    if result.empty:
        return {"label": label, "trades": 0, "pnl": 0.0, "winrate": 0.0, "max_dd": 0.0}
    pnl = result["pnl"]
    cum = pnl.cumsum()
    dd = float((cum - cum.cummax()).min())
    return {
        "label": label,
        "trades": int(len(result)),
        "pnl": float(pnl.sum()),
        "winrate": float((pnl > 0).mean() * 100),
        "max_dd": dd,
    }


def build_report(
    wf_result: pd.DataFrame,
    is_result: pd.DataFrame,
    bh_series: pd.Series,
    ticker: str,
    output_html: Path,
    folds: list[dict],
    params: dict,
) -> None:
    fig = go.Figure()

    if not wf_result.empty:
        fig.add_trace(
            go.Scatter(
                x=wf_result["source_date"],
                y=wf_result["cum_pnl"],
                mode="lines+markers",
                name="Walk-forward (out-of-sample)",
                line=dict(color="#1b5e20", width=3),
            )
        )

    if not is_result.empty:
        is_sorted = is_result.sort_values("source_date").reset_index(drop=True)
        is_sorted["cum_pnl"] = is_sorted["pnl"].cumsum()
        fig.add_trace(
            go.Scatter(
                x=is_sorted["source_date"],
                y=is_sorted["cum_pnl"],
                mode="lines",
                name="In-sample rules.yaml",
                line=dict(color="#1565c0", width=2, dash="dot"),
            )
        )

    if not bh_series.empty:
        bh_cum = bh_series.cumsum()
        fig.add_trace(
            go.Scatter(
                x=bh_cum.index,
                y=bh_cum.values,
                mode="lines",
                name="Buy & hold next_body",
                line=dict(color="#9e9e9e", width=2, dash="dash"),
            )
        )

    title = (
        f"{ticker}: walk-forward — train={params['train_size']} / test={params['test_size']} / "
        f"step={params['step']} / min_trades={params['min_trades']} / threshold={params['threshold']}"
    )

    fig.update_layout(
        title=title,
        xaxis=dict(title="Дата"),
        yaxis=dict(title="Кумулятивный PnL"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.01),
        template="plotly_white",
        margin=dict(l=40, r=40, t=70, b=40),
    )

    folds_rows = []
    for f in folds:
        rules_str = ", ".join(
            f"{int(k) if k.is_integer() else k}:{v[0]}" for k, v in f["fitted_rules"].items()
        )
        folds_rows.append(
            [
                f["fold"],
                str(f["train_from"]),
                str(f["train_to"]),
                str(f["test_from"]),
                str(f["test_to"]),
                f["n_rules"],
                f["n_trades"],
                f"{f['pnl']:.2f}",
                rules_str,
            ]
        )

    folds_table = go.Figure(
        go.Table(
            header=dict(
                values=[
                    "<b>Fold</b>",
                    "<b>Train from</b>",
                    "<b>Train to</b>",
                    "<b>Test from</b>",
                    "<b>Test to</b>",
                    "<b># rules</b>",
                    "<b># trades</b>",
                    "<b>PnL</b>",
                    "<b>Fitted rules (v:action[0])</b>",
                ],
                fill_color="#1565c0",
                font=dict(color="white", size=12),
                align="left",
                height=32,
            ),
            cells=dict(
                values=list(map(list, zip(*folds_rows))) if folds_rows else [[]] * 9,
                fill_color="#f9f9f9",
                align="left",
                font=dict(color="#111", size=11),
                height=28,
            ),
        )
    )
    folds_table.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=max(120, 60 + 30 * len(folds_rows)))

    output_html.parent.mkdir(parents=True, exist_ok=True)
    with output_html.open("w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
        f.write(f"<title>{ticker} walk-forward</title>\n</head><body>\n")
        f.write(f"<h2>{title}</h2>\n")
        f.write(fig.to_html(include_plotlyjs="cdn", full_html=False))
        f.write("\n<hr style='margin:24px 0; border:1px solid #ccc'>\n")
        f.write("<h3>Folds</h3>\n")
        f.write(folds_table.to_html(include_plotlyjs=False, full_html=False))
        f.write("\n</body></html>")


@app.command()
def main(
    settings_yaml: Path = typer.Option(
        Path(__file__).parent / "settings.yaml",
        exists=True,
        help="Локальный settings.yaml для тикера.",
    ),
    quantity: Optional[int] = typer.Option(
        None,
        help="Количество контрактов на сделку. По умолчанию — quantity_open из settings.yaml.",
    ),
    rules_yaml: Path = typer.Option(
        Path(__file__).parent / "rules.yaml",
        "--rules-yaml",
        help="YAML-файл с in-sample правилами для сравнения.",
    ),
    train_size: int = typer.Option(60, help="Размер train-окна (в торговых днях/строках)."),
    test_size: int = typer.Option(20, help="Размер test-окна."),
    step: int = typer.Option(20, help="Шаг скольжения окна."),
    min_trades: int = typer.Option(3, help="Минимум сделок с данным sentiment в train-окне, чтобы вывести правило."),
    threshold: float = typer.Option(0.0, help="Порог |sum(follow)| для принятия решения follow/invert."),
) -> None:
    folder = Path(__file__).parent
    settings = load_yaml_settings(settings_yaml)
    ticker = settings.get("ticker", folder.name.upper())

    sentiment_pkl = resolve_sentiment_pkl(settings, folder)
    if quantity is None:
        quantity = int(settings.get("quantity_open", 1))

    df = load_sentiment(sentiment_pkl)
    aggregated = index_by_date(df)

    if len(aggregated) < train_size + test_size:
        typer.echo(
            f"Недостаточно данных: {len(aggregated)} строк, нужно хотя бы {train_size + test_size}."
        )
        raise typer.Exit(code=1)

    # Walk-forward
    wf_result, folds = walk_forward(
        aggregated,
        train_size=train_size,
        test_size=test_size,
        step=step,
        min_trades=min_trades,
        threshold=threshold,
        quantity=quantity,
    )

    # In-sample сравнение: применяем rules.yaml ко всем датам, попавшим в test-окна
    rules = load_rules(rules_yaml)
    is_full = build_backtest(aggregated, quantity, rules)
    if folds:
        test_dates = set()
        for f in folds:
            mask_dates = aggregated.loc[f["test_from"] : f["test_to"]].index
            test_dates.update(mask_dates)
        is_result = is_full[is_full["source_date"].isin(test_dates)].copy()
    else:
        is_result = pd.DataFrame()

    # Buy & hold на тех же test-датах
    if folds:
        bh_dates = sorted(test_dates)
        bh_series = aggregated.loc[bh_dates, "next_body"] * quantity
    else:
        bh_series = pd.Series(dtype=float)

    # Сводка
    summaries = [
        summarize(wf_result, "Walk-forward (OOS)"),
        summarize(is_result, "In-sample rules.yaml (на тех же датах)"),
    ]
    if not bh_series.empty:
        summaries.append(
            {
                "label": "Buy & hold next_body",
                "trades": int(len(bh_series)),
                "pnl": float(bh_series.sum()),
                "winrate": float((bh_series > 0).mean() * 100),
                "max_dd": float((bh_series.cumsum() - bh_series.cumsum().cummax()).min()),
            }
        )

    typer.echo(f"\n{ticker}: walk-forward")
    typer.echo(
        f"params: train={train_size} test={test_size} step={step} "
        f"min_trades={min_trades} threshold={threshold} quantity={quantity}"
    )
    typer.echo(f"folds: {len(folds)}")
    typer.echo("")
    typer.echo(f"{'Strategy':<42} {'Trades':>8} {'PnL':>14} {'Winrate':>10} {'MaxDD':>14}")
    for s in summaries:
        typer.echo(
            f"{s['label']:<42} {s['trades']:>8} {s['pnl']:>14.2f} "
            f"{s['winrate']:>9.1f}% {s['max_dd']:>14.2f}"
        )

    typer.echo("\nПравила по фолдам:")
    for f in folds:
        rules_str = ", ".join(
            f"{int(k) if k.is_integer() else k}:{v}" for k, v in sorted(f["fitted_rules"].items())
        ) or "(пусто)"
        typer.echo(
            f"  fold {f['fold']}: train {f['train_from']}..{f['train_to']} -> "
            f"test {f['test_from']}..{f['test_to']} | trades={f['n_trades']} pnl={f['pnl']:.2f} | {rules_str}"
        )

    # Сохранения
    if not wf_result.empty:
        xlsx_path = folder / "sentiment_walk_forward_results.xlsx"
        wf_result.to_excel(xlsx_path, index=False)
        typer.echo(f"\nOOS сделки: {xlsx_path}")

    folds_xlsx = folder / "sentiment_walk_forward_folds.xlsx"
    pd.DataFrame(
        [
            {
                "fold": f["fold"],
                "train_from": f["train_from"],
                "train_to": f["train_to"],
                "test_from": f["test_from"],
                "test_to": f["test_to"],
                "n_rules": f["n_rules"],
                "n_trades": f["n_trades"],
                "pnl": f["pnl"],
                "fitted_rules": yaml.safe_dump(f["fitted_rules"], allow_unicode=True, default_flow_style=True).strip(),
            }
            for f in folds
        ]
    ).to_excel(folds_xlsx, index=False)
    typer.echo(f"Folds: {folds_xlsx}")

    html_path = folder / "plots" / "sentiment_walk_forward.html"
    params = {
        "train_size": train_size,
        "test_size": test_size,
        "step": step,
        "min_trades": min_trades,
        "threshold": threshold,
    }
    build_report(wf_result, is_result, bh_series, ticker, html_path, folds, params)
    typer.echo(f"HTML-отчёт: {html_path}")


if __name__ == "__main__":
    app()
