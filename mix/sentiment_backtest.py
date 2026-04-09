"""
Backtest sentiment-стратегии: P/L считается по next_body из pkl (ничего не джойнится с SQLite).
Требует --rules-yaml (по умолчанию rules.yaml рядом со скриптом) со списком правил
вида {min, max, action}, где action ∈ {follow, invert, skip}.
follow — LONG при sentiment>0, SHORT при sentiment<0; invert — наоборот; skip — не торговать.
Правила матчатся по первому совпадению; диапазоны, не покрытые правилами, пропускаются.
Пользователь может добавлять/удалять строки в yaml-файле без изменения кода.
"""

from __future__ import annotations

import pickle
from datetime import date
from pathlib import Path
from typing import Optional


def _parse_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return pd.to_datetime(str(value)).date()

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import typer
import yaml

app = typer.Typer(help="Backtest sentiment-стратегии для следующей торговой сессии (из pkl).")


VALID_ACTIONS = {"follow", "invert", "skip"}


def load_yaml_settings(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_sentiment_pkl(settings: dict, folder: Path) -> Path:
    sentiment_path = Path(settings.get("sentiment_output_pkl", "sentiment_scores.pkl"))
    return sentiment_path if sentiment_path.is_absolute() else folder / sentiment_path


def load_sentiment(path: Path) -> pd.DataFrame:
    """Читает pkl, построенный sentiment_analysis.py. Требует колонки sentiment и next_body."""
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
    """Загружает список правил из yaml."""
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
    """Возвращает action из первого подходящего правила, иначе skip."""
    for rule in rules:
        if float(rule["min"]) <= sentiment <= float(rule["max"]):
            return rule["action"]
    return "skip"


def build_backtest(
    aggregated: pd.DataFrame,
    quantity: int,
    rules: list[dict],
) -> pd.DataFrame:
    rows = []
    for source_date, row in aggregated.iterrows():
        sentiment = float(row["sentiment"])
        next_body = float(row["next_body"])

        action = match_action(sentiment, rules)
        if action == "skip":
            continue
        if sentiment == 0.0:
            continue  # направление не определено

        if action == "follow":
            direction = "LONG" if sentiment > 0 else "SHORT"
        else:  # invert
            direction = "SHORT" if sentiment > 0 else "LONG"

        pnl = next_body * quantity if direction == "LONG" else -next_body * quantity

        rows.append(
            {
                "source_date": source_date,
                "sentiment": sentiment,
                "action": action,
                "direction": direction,
                "next_body": next_body,
                "quantity": quantity,
                "pnl": pnl,
            }
        )

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values("source_date").reset_index(drop=True)
    result["cum_pnl"] = result["pnl"].cumsum()
    return result


def _max_drawdown(result: pd.DataFrame) -> float:
    series = result["cum_pnl"]
    peak = series.cummax()
    drawdown = series - peak
    return float(drawdown.min())


def _max_consecutive(series: pd.Series, condition: int) -> int:
    streaks = (series != condition).cumsum()
    filtered = series[series == condition]
    if filtered.empty:
        return 0
    return int(filtered.groupby(streaks[series == condition]).size().max())


def _drawdown_duration(drawdown: pd.Series) -> int:
    max_dd_duration = 0
    current_dd_start = None
    for i in range(len(drawdown)):
        if drawdown.iloc[i] < 0:
            if current_dd_start is None:
                current_dd_start = i
        else:
            if current_dd_start is not None:
                duration = i - current_dd_start
                if duration > max_dd_duration:
                    max_dd_duration = duration
                current_dd_start = None
    if current_dd_start is not None:
        duration = len(drawdown) - current_dd_start
        if duration > max_dd_duration:
            max_dd_duration = duration
    return max_dd_duration


def build_report(result: pd.DataFrame, ticker: str, output_html: Path, rules_path: Path) -> None:
    df = result.copy()
    df["source_date"] = pd.to_datetime(df["source_date"])
    df = df.sort_values("source_date").reset_index(drop=True)

    pl = df["pnl"].astype(float)
    cum = pl.cumsum()

    # ── Агрегации ─────────────────────────────────────────────────────────
    day_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in pl]

    df["Неделя"] = df["source_date"].dt.to_period("W")
    weekly = df.groupby("Неделя", as_index=False)["pnl"].sum()
    weekly["dt"] = weekly["Неделя"].apply(lambda p: p.start_time)
    week_colors = ["#d32f2f" if v < 0 else "#00838f" for v in weekly["pnl"]]

    df["Месяц"] = df["source_date"].dt.to_period("M")
    monthly = df.groupby("Месяц", as_index=False)["pnl"].sum()
    monthly["dt"] = monthly["Месяц"].dt.to_timestamp()
    month_colors = ["#d32f2f" if v < 0 else "#1565c0" for v in monthly["pnl"]]

    running_max = cum.cummax()
    drawdown = cum - running_max

    for w in (5, 10, 20):
        df[f"MA{w}"] = pl.rolling(w, min_periods=1).mean()

    sent_stats = (
        df.groupby("sentiment")
        .agg(trades=("pnl", "size"), pnl=("pnl", "sum"))
        .reset_index()
        .sort_values("sentiment")
    )
    action_stats = (
        df.groupby("action")
        .agg(
            trades=("pnl", "size"),
            pnl=("pnl", "sum"),
            winrate=("pnl", lambda s: (s > 0).mean() * 100),
        )
        .reset_index()
    )

    # ── Метрики ───────────────────────────────────────────────────────────
    total_profit = float(cum.iloc[-1])
    total_trades = len(df)
    win_trades = int((pl > 0).sum())
    loss_trades = int((pl < 0).sum())
    win_rate = win_trades / max(total_trades, 1) * 100
    max_dd = float(drawdown.min())
    best_trade = float(pl.max())
    worst_trade = float(pl.min())
    avg_trade = float(pl.mean())
    median_trade = float(pl.median())
    std_trade = float(pl.std()) if total_trades > 1 else 0.0

    gross_profit = float(pl[pl > 0].sum())
    gross_loss = float(abs(pl[pl < 0].sum()))
    avg_win = float(pl[pl > 0].mean()) if win_trades else 0.0
    avg_loss = float(abs(pl[pl < 0].mean())) if loss_trades else 0.0

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")
    recovery_factor = total_profit / abs(max_dd) if max_dd != 0 else float("inf")
    expectancy = (win_rate / 100) * avg_win - (1 - win_rate / 100) * avg_loss
    sharpe = (avg_trade / std_trade) * np.sqrt(252) if std_trade > 0 else 0.0

    downside = pl[pl < 0]
    downside_std = float(downside.std()) if len(downside) > 1 else 0.0
    sortino = (avg_trade / downside_std) * np.sqrt(252) if downside_std > 0 else 0.0

    date_range_days = (df["source_date"].max() - df["source_date"].min()).days or 1
    annual_profit = total_profit * 365 / date_range_days
    calmar = annual_profit / abs(max_dd) if max_dd != 0 else float("inf")

    signs = pl.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    max_consec_wins = _max_consecutive(signs, 1)
    max_consec_losses = _max_consecutive(signs, -1)
    max_dd_duration = _drawdown_duration(drawdown)
    volatility = std_trade * np.sqrt(252)

    stats_text = (
        f"Итого: {total_profit:,.0f} | Сделок: {total_trades} | "
        f"Win: {win_trades} ({win_rate:.0f}%) | Loss: {loss_trades} | "
        f"PF: {profit_factor:.2f} | RF: {recovery_factor:.2f} | "
        f"Sharpe: {sharpe:.2f} | MaxDD: {max_dd:,.0f}"
    )

    # ── Графики ───────────────────────────────────────────────────────────
    fig = make_subplots(
        rows=5, cols=2,
        subplot_titles=(
            "P/L по сделкам",
            "Накопленная прибыль (equity)",
            "P/L по неделям",
            "P/L по месяцам",
            "Drawdown от максимума",
            "Распределение P/L сделок",
            "Скользящие средние P/L (5/10/20)",
            "P/L по action (follow/invert)",
            "P/L по значениям sentiment",
            "Кол-во сделок по sentiment",
        ),
        specs=[
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "histogram"}],
            [{"type": "scatter"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "bar"}],
        ],
        vertical_spacing=0.07,
        horizontal_spacing=0.08,
    )

    fig.add_trace(
        go.Bar(
            x=df["source_date"], y=pl, marker_color=day_colors,
            name="P/L сделки",
            hovertemplate="%{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["source_date"], y=cum,
            mode="lines+markers", fill="tozeroy",
            line=dict(color="#2e7d32", width=2),
            fillcolor="rgba(46,125,50,0.15)",
            name="Equity",
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
        ),
        row=1, col=2,
    )
    fig.add_trace(
        go.Bar(
            x=weekly["dt"], y=weekly["pnl"], marker_color=week_colors,
            name="P/L неделя",
            hovertemplate="Нед. %{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Bar(
            x=monthly["dt"], y=monthly["pnl"], marker_color=month_colors,
            name="P/L месяц",
            text=[f"{v:,.0f}" for v in monthly["pnl"]],
            textposition="outside",
            hovertemplate="%{x|%Y-%m}<br>P/L: %{y:,.0f}<extra></extra>",
        ),
        row=2, col=2,
    )
    fig.add_trace(
        go.Scatter(
            x=df["source_date"], y=drawdown,
            mode="lines", fill="tozeroy",
            line=dict(color="#d32f2f", width=1.5),
            fillcolor="rgba(211,47,47,0.2)",
            name="Drawdown",
            hovertemplate="%{x|%Y-%m-%d}<br>DD: %{y:,.0f}<extra></extra>",
        ),
        row=3, col=1,
    )
    pl_pos = pl[pl > 0]
    pl_neg = pl[pl < 0]
    fig.add_trace(
        go.Histogram(x=pl_pos, marker_color="#2e7d32", opacity=0.7, name="Прибыль", nbinsx=20),
        row=3, col=2,
    )
    fig.add_trace(
        go.Histogram(x=pl_neg, marker_color="#d32f2f", opacity=0.7, name="Убыток", nbinsx=20),
        row=3, col=2,
    )
    for w, color in [(5, "#1565c0"), (10, "#ff6f00"), (20, "#7b1fa2")]:
        fig.add_trace(
            go.Scatter(
                x=df["source_date"], y=df[f"MA{w}"],
                mode="lines", line=dict(color=color, width=1.5),
                name=f"MA{w}",
                hovertemplate=f"MA{w}: " + "%{y:,.0f}<extra></extra>",
            ),
            row=4, col=1,
        )
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=4, col=1)

    action_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in action_stats["pnl"]]
    fig.add_trace(
        go.Bar(
            x=action_stats["action"], y=action_stats["pnl"],
            marker_color=action_colors,
            text=[f"{v:,.0f}<br>{t} сд.<br>win {w:.0f}%"
                  for v, t, w in zip(action_stats["pnl"], action_stats["trades"], action_stats["winrate"])],
            textposition="outside",
            name="P/L по action",
            hovertemplate="%{x}<br>P/L: %{y:,.0f}<extra></extra>",
        ),
        row=4, col=2,
    )

    sent_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in sent_stats["pnl"]]
    fig.add_trace(
        go.Bar(
            x=sent_stats["sentiment"], y=sent_stats["pnl"],
            marker_color=sent_colors,
            text=[f"{v:,.0f}" for v in sent_stats["pnl"]],
            textposition="outside",
            name="P/L по sentiment",
            hovertemplate="sentiment: %{x}<br>P/L: %{y:,.0f}<extra></extra>",
        ),
        row=5, col=1,
    )
    fig.add_trace(
        go.Bar(
            x=sent_stats["sentiment"], y=sent_stats["trades"],
            marker_color="#1565c0",
            name="Кол-во сделок",
            hovertemplate="sentiment: %{x}<br>сделок: %{y}<extra></extra>",
        ),
        row=5, col=2,
    )

    title = f"{ticker}: sentiment backtest — правила: {rules_path.name}"
    fig.update_layout(
        height=2200,
        width=1500,
        title_text=f"{title}<br><sub>{stats_text}</sub>",
        title_x=0.5,
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
        template="plotly_white",
        hovermode="x unified",
    )
    for r, c in [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (4, 1), (4, 2), (5, 1), (5, 2)]:
        fig.update_yaxes(tickformat=",", row=r, col=c)

    # ── Таблица статистики ────────────────────────────────────────────────
    sec1 = [
        ["<b>ДОХОДНОСТЬ</b>", ""],
        ["Чистая прибыль", f"{total_profit:,.0f}"],
        ["Годовая прибыль (экстрапол.)", f"{annual_profit:,.0f}"],
        ["Средний P/L на сделку", f"{avg_trade:,.0f}"],
        ["Медианный P/L на сделку", f"{median_trade:,.0f}"],
        ["Лучшая сделка", f"{best_trade:,.0f}"],
        ["Худшая сделка", f"{worst_trade:,.0f}"],
    ]
    sec2 = [
        ["<b>РИСК</b>", ""],
        ["Max Drawdown", f"{max_dd:,.0f}"],
        ["Длит. макс. просадки", f"{max_dd_duration} сделок"],
        ["Волатильность (год.)", f"{volatility:,.0f}"],
        ["Std сделки", f"{std_trade:,.0f}"],
        ["VaR 95%", f"{np.percentile(pl, 5):,.0f}"],
        ["CVaR 95%", f"{pl[pl <= np.percentile(pl, 5)].mean():,.0f}"],
    ]
    sec3 = [
        ["<b>СТАТИСТИКА СДЕЛОК</b>", ""],
        ["Всего сделок", f"{total_trades}"],
        ["Win / Loss", f"{win_trades} / {loss_trades}"],
        ["Win rate", f"{win_rate:.1f}%"],
        ["Ср. выигрыш / проигрыш", f"{avg_win:,.0f} / {avg_loss:,.0f}"],
        ["Макс. серия побед", f"{max_consec_wins}"],
        ["Макс. серия убытков", f"{max_consec_losses}"],
    ]

    num_rows = max(len(sec1), len(sec2), len(sec3))
    for sec in (sec1, sec2, sec3):
        while len(sec) < num_rows:
            sec.append(["", ""])

    cols_values = [[], [], [], [], [], []]
    tbl_colors = [[], [], []]
    for i in range(num_rows):
        for j, sec in enumerate((sec1, sec2, sec3)):
            n, v = sec[i]
            is_hdr = v == "" and n.startswith("<b>")
            cols_values[j * 2].append(n)
            cols_values[j * 2 + 1].append(f"<b>{v}</b>" if v and not is_hdr else v)
            if is_hdr:
                tbl_colors[j].append("#e3f2fd")
            else:
                tbl_colors[j].append("#f5f5f5" if i % 2 == 0 else "white")

    fig_stats = go.Figure(
        go.Table(
            columnwidth=[200, 130, 180, 120, 220, 120],
            header=dict(
                values=["<b>Показатель</b>", "<b>Значение</b>"] * 3,
                fill_color="#1565c0",
                font=dict(color="white", size=14),
                align="left",
                height=32,
            ),
            cells=dict(
                values=cols_values,
                fill_color=[tbl_colors[0], tbl_colors[0], tbl_colors[1], tbl_colors[1],
                            tbl_colors[2], tbl_colors[2]],
                font=dict(size=13, color="#212121"),
                align=["left", "right", "left", "right", "left", "right"],
                height=26,
            ),
        )
    )
    fig_stats.update_layout(
        title_text=f"<b>{ticker} — Backtest: статистика стратегии</b>",
        title_x=0.5,
        title_font_size=18,
        height=32 + num_rows * 26 + 80,
        width=1500,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    # ── Таблица коэффициентов ─────────────────────────────────────────────
    coefficients = [
        {
            "name": "Recovery Factor", "formula": "Чистая прибыль / |Max Drawdown|",
            "value": f"{recovery_factor:.2f}",
            "description": "Коэффициент восстановления — во сколько раз прибыль превышает максимальную просадку. RF > 1 — стратегия заработала больше, чем потеряла в худший период.",
        },
        {
            "name": "Profit Factor", "formula": "Валовая прибыль / Валовый убыток",
            "value": f"{profit_factor:.2f}",
            "description": "Фактор прибыли. PF > 1 — прибыльность, 1.5–2.0 хорошо, > 2.0 отлично.",
        },
        {
            "name": "Payoff Ratio", "formula": "Средний выигрыш / Средний проигрыш",
            "value": f"{payoff_ratio:.2f}",
            "description": "При высоком payoff стратегия остаётся прибыльной даже при win rate < 50%.",
        },
        {
            "name": "Sharpe Ratio", "formula": "(Ср. P/L / Std) × √252",
            "value": f"{sharpe:.2f}",
            "description": "Отношение доходности к риску, приведённое к году. > 1 хорошо, > 2 отлично, > 3 исключительно.",
        },
        {
            "name": "Sortino Ratio", "formula": "(Ср. P/L / Downside Std) × √252",
            "value": f"{sortino:.2f}",
            "description": "Модификация Шарпа, учитывающая только нисходящую волатильность.",
        },
        {
            "name": "Calmar Ratio", "formula": "Годовая доходность / |Max Drawdown|",
            "value": f"{calmar:.2f}",
            "description": "Отношение годовой прибыли к макс. просадке. > 1 — прибыль превышает худшую просадку, > 3 отлично.",
        },
        {
            "name": "Expectancy", "formula": "Win% × Ср.выигрыш − Loss% × Ср.проигрыш",
            "value": f"{expectancy:,.0f}",
            "description": "Матожидание на одну сделку. Положительное — стратегия имеет преимущество (edge).",
        },
    ]

    fig_table = go.Figure(
        go.Table(
            columnwidth=[150, 250, 80, 450],
            header=dict(
                values=["<b>Коэффициент</b>", "<b>Формула</b>", "<b>Значение</b>", "<b>Расшифровка</b>"],
                fill_color="#1565c0",
                font=dict(color="white", size=14),
                align="left",
                height=36,
            ),
            cells=dict(
                values=[
                    [f"<b>{c['name']}</b>" for c in coefficients],
                    [c["formula"] for c in coefficients],
                    [f"<b>{c['value']}</b>" for c in coefficients],
                    [c["description"] for c in coefficients],
                ],
                fill_color=[["#f5f5f5" if i % 2 == 0 else "white" for i in range(len(coefficients))]] * 4,
                font=dict(size=13, color="#212121"),
                align=["left", "left", "center", "left"],
                height=60,
            ),
        )
    )
    fig_table.update_layout(
        title_text=f"<b>{ticker} — Backtest: ключевые коэффициенты</b>",
        title_x=0.5,
        title_font_size=18,
        height=560,
        width=1500,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    # ── Сохранение ────────────────────────────────────────────────────────
    output_html.parent.mkdir(parents=True, exist_ok=True)
    with output_html.open("w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
        f.write(f"<title>{ticker} sentiment backtest</title>\n</head><body>\n")
        f.write(fig.to_html(include_plotlyjs="cdn", full_html=False))
        f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
        f.write(fig_stats.to_html(include_plotlyjs=False, full_html=False))
        f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
        f.write(fig_table.to_html(include_plotlyjs=False, full_html=False))
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
        help="YAML-файл с правилами {min,max,action}. action ∈ follow/invert/skip. Первое совпадение.",
    ),
    date_from: Optional[str] = typer.Option(
        None,
        "--date-from",
        help="Нижняя граница окна (YYYY-MM-DD). Переопределяет settings.yaml:backtest_date_from.",
    ),
    date_to: Optional[str] = typer.Option(
        None,
        "--date-to",
        help="Верхняя граница окна (YYYY-MM-DD). Переопределяет settings.yaml:backtest_date_to.",
    ),
) -> None:
    folder = Path(__file__).parent
    settings = load_yaml_settings(settings_yaml)
    ticker = settings.get("ticker", folder.name.upper())

    sentiment_pkl = resolve_sentiment_pkl(settings, folder)
    if quantity is None:
        quantity = int(settings.get("quantity_open", 1))

    rules = load_rules(rules_yaml)

    d_from = _parse_date(date_from if date_from is not None else settings.get("backtest_date_from"))
    d_to = _parse_date(date_to if date_to is not None else settings.get("backtest_date_to"))

    df = load_sentiment(sentiment_pkl)
    aggregated = index_by_date(df)

    if d_from is not None:
        aggregated = aggregated[aggregated.index >= d_from]
    if d_to is not None:
        aggregated = aggregated[aggregated.index <= d_to]

    if aggregated.empty:
        typer.echo("После фильтра по дате не осталось записей. Проверьте окно.")
        raise typer.Exit(code=1)

    result = build_backtest(aggregated, quantity, rules)

    if result.empty:
        typer.echo("Нет доступных сделок для бэктеста. Проверьте pkl и правила.")
        raise typer.Exit(code=1)

    report_folder = folder / "plots"
    output_html = report_folder / "sentiment_backtest.html"
    output_xlsx = folder / "sentiment_backtest_results.xlsx"
    result.to_excel(output_xlsx, index=False)
    build_report(result, ticker, output_html, rules_yaml)

    typer.echo(f"Готово: {output_xlsx} и {output_html}")
    typer.echo(f"Правила: {rules_yaml}")
    typer.echo(f"Всего сделок: {len(result)}")
    typer.echo(f"Общий PnL: {result['pnl'].sum():.2f}")
    typer.echo(f"Winrate: {(result['pnl'] > 0).mean() * 100:.1f}%")
    typer.echo(f"Макс. просадка: {_max_drawdown(result):.2f}")


if __name__ == "__main__":
    app()
