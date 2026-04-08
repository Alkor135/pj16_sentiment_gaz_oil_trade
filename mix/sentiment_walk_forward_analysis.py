"""
Интерактивный анализ результатов walk-forward валидации (Plotly).
Читает sentiment_walk_forward_results.xlsx и sentiment_walk_forward_folds.xlsx.
Графики: дневной/недельный/месячный P/L, накопленная прибыль, drawdown,
скользящие средние, распределение сделок, разбивка по fold'ам и по action.
Таблицы: статистика стратегии и ключевые коэффициенты (Sharpe, Sortino, Calmar и др.).
Конфигурация через settings.yaml (ticker). Сохраняет HTML-отчёт в plots/.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import yaml
from plotly.subplots import make_subplots

SETTINGS_FILE = Path(__file__).parent / "settings.yaml"
with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
    settings = yaml.safe_load(f)

ticker = settings["ticker"]
SAVE_PATH = Path(__file__).parent
TRADES_FILE = SAVE_PATH / "sentiment_walk_forward_results.xlsx"
FOLDS_FILE = SAVE_PATH / "sentiment_walk_forward_folds.xlsx"

# ── Загрузка данных ─────────────────────────────────────────────────────
df = pd.read_excel(TRADES_FILE)
df["source_date"] = pd.to_datetime(df["source_date"])
df = df.sort_values("source_date").reset_index(drop=True)

folds_df = pd.read_excel(FOLDS_FILE)
folds_df["train_from"] = pd.to_datetime(folds_df["train_from"])
folds_df["train_to"] = pd.to_datetime(folds_df["train_to"])
folds_df["test_from"] = pd.to_datetime(folds_df["test_from"])
folds_df["test_to"] = pd.to_datetime(folds_df["test_to"])

pl = df["pnl"].astype(float)
cum = pl.cumsum()

# ── Агрегации ───────────────────────────────────────────────────────────
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

# По фолдам
fold_stats = df.groupby("fold").agg(
    trades=("pnl", "size"),
    pnl=("pnl", "sum"),
    winrate=("pnl", lambda s: (s > 0).mean() * 100),
).reset_index()

# По action
action_stats = df.groupby("action").agg(
    trades=("pnl", "size"),
    pnl=("pnl", "sum"),
    winrate=("pnl", lambda s: (s > 0).mean() * 100),
).reset_index()

# По значению sentiment
sent_stats = df.groupby("sentiment").agg(
    trades=("pnl", "size"),
    pnl=("pnl", "sum"),
    pos=("pnl", lambda s: int((s > 0).sum())),
    neg=("pnl", lambda s: int((s < 0).sum())),
).reset_index().sort_values("sentiment")

# ── Метрики ─────────────────────────────────────────────────────────────
total_profit = cum.iloc[-1]
total_trades = len(df)
win_trades = int((pl > 0).sum())
loss_trades = int((pl < 0).sum())
win_rate = win_trades / max(total_trades, 1) * 100
max_dd = drawdown.min()
best_trade = pl.max()
worst_trade = pl.min()
avg_trade = pl.mean()
median_trade = pl.median()
std_trade = pl.std()

gross_profit = pl[pl > 0].sum()
gross_loss = abs(pl[pl < 0].sum())
avg_win = pl[pl > 0].mean() if win_trades else 0
avg_loss = abs(pl[pl < 0].mean()) if loss_trades else 0

profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
payoff_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")
recovery_factor = total_profit / abs(max_dd) if max_dd != 0 else float("inf")
expectancy = (win_rate / 100) * avg_win - (1 - win_rate / 100) * avg_loss
sharpe = (avg_trade / std_trade) * np.sqrt(252) if std_trade > 0 else 0

downside = pl[pl < 0]
downside_std = downside.std() if len(downside) > 1 else 0
sortino = (avg_trade / downside_std) * np.sqrt(252) if downside_std > 0 else 0

date_range_days = (df["source_date"].max() - df["source_date"].min()).days or 1
annual_profit = total_profit * 365 / date_range_days
calmar = annual_profit / abs(max_dd) if max_dd != 0 else float("inf")


def max_consecutive(series, condition):
    streaks = (series != condition).cumsum()
    filtered = series[series == condition]
    if filtered.empty:
        return 0
    return filtered.groupby(streaks[series == condition]).size().max()


signs = pl.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
max_consec_wins = max_consecutive(signs, 1)
max_consec_losses = max_consecutive(signs, -1)

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

volatility = std_trade * np.sqrt(252)

stats_text = (
    f"Итого: {total_profit:,.0f} | Сделок: {total_trades} | "
    f"Win: {win_trades} ({win_rate:.0f}%) | Loss: {loss_trades} | "
    f"PF: {profit_factor:.2f} | RF: {recovery_factor:.2f} | "
    f"Sharpe: {sharpe:.2f} | MaxDD: {max_dd:,.0f}"
)

# ── Построение графиков ─────────────────────────────────────────────────
fig = make_subplots(
    rows=5, cols=2,
    subplot_titles=(
        "P/L по сделкам (дата)",
        "Накопленная прибыль (OOS equity)",
        "P/L по неделям",
        "P/L по месяцам",
        "Drawdown от максимума",
        "Распределение P/L сделок",
        "Скользящие средние P/L (5/10/20 сделок)",
        "Разбивка по fold'ам",
        "P/L по значениям sentiment",
        "P/L по action (follow / invert)",
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

# 1) P/L по сделкам
fig.add_trace(
    go.Bar(
        x=df["source_date"], y=pl, marker_color=day_colors,
        name="P/L сделки",
        hovertemplate="%{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=1, col=1,
)

# 2) Накопленная прибыль
fig.add_trace(
    go.Scatter(
        x=df["source_date"], y=cum,
        mode="lines+markers", fill="tozeroy",
        line=dict(color="#2e7d32", width=2),
        fillcolor="rgba(46,125,50,0.15)",
        name="OOS equity",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    ),
    row=1, col=2,
)

# 3) Недельный P/L
fig.add_trace(
    go.Bar(
        x=weekly["dt"], y=weekly["pnl"], marker_color=week_colors,
        name="P/L неделя",
        hovertemplate="Нед. %{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=2, col=1,
)

# 4) Месячный P/L
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

# 5) Drawdown
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

# 6) Распределение
pl_pos = pl[pl > 0]
pl_neg = pl[pl < 0]
fig.add_trace(
    go.Histogram(
        x=pl_pos, marker_color="#2e7d32", opacity=0.7,
        name="Прибыль", nbinsx=15,
        hovertemplate="P/L: %{x:,.0f}<br>Кол-во: %{y}<extra></extra>",
    ),
    row=3, col=2,
)
fig.add_trace(
    go.Histogram(
        x=pl_neg, marker_color="#d32f2f", opacity=0.7,
        name="Убыток", nbinsx=15,
        hovertemplate="P/L: %{x:,.0f}<br>Кол-во: %{y}<extra></extra>",
    ),
    row=3, col=2,
)

# 7) MA P/L
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

# 8) По фолдам
fold_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in fold_stats["pnl"]]
fig.add_trace(
    go.Bar(
        x=[f"Fold {f}" for f in fold_stats["fold"]],
        y=fold_stats["pnl"],
        marker_color=fold_colors,
        text=[f"{v:,.0f}<br>{t} сд." for v, t in zip(fold_stats["pnl"], fold_stats["trades"])],
        textposition="outside",
        name="Fold P/L",
        hovertemplate="%{x}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=4, col=2,
)

# 9) По sentiment
sent_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in sent_stats["pnl"]]
fig.add_trace(
    go.Bar(
        x=sent_stats["sentiment"], y=sent_stats["pnl"],
        marker_color=sent_colors,
        text=[f"{v:,.0f}<br>({t})" for v, t in zip(sent_stats["pnl"], sent_stats["trades"])],
        textposition="outside",
        name="P/L по sentiment",
        hovertemplate="sentiment: %{x}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=5, col=1,
)

# 10) По action
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
    row=5, col=2,
)

fig.update_layout(
    height=2200,
    width=1500,
    title_text=(
        f"{ticker} Walk-Forward Analysis (out-of-sample)"
        f"<br><sub>{stats_text}</sub>"
    ),
    title_x=0.5,
    showlegend=True,
    legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
    template="plotly_white",
    hovermode="x unified",
)

for row, col in [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (4, 1), (4, 2), (5, 1), (5, 2)]:
    fig.update_yaxes(tickformat=",", row=row, col=col)

# ── Таблица статистики ───────────────────────────────────────────────────
sec1 = [
    ["<b>ДОХОДНОСТЬ</b>", ""],
    ["Чистая прибыль (OOS)", f"{total_profit:,.0f}"],
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
    ["Fold'ов", f"{len(folds_df)}"],
    ["Win / Loss", f"{win_trades} / {loss_trades}"],
    ["Win rate", f"{win_rate:.1f}%"],
    ["Ср. выигрыш / проигрыш", f"{avg_win:,.0f} / {avg_loss:,.0f}"],
    ["Макс. серия побед / убытков", f"{max_consec_wins} / {max_consec_losses}"],
]

num_rows = max(len(sec1), len(sec2), len(sec3))
for sec in (sec1, sec2, sec3):
    while len(sec) < num_rows:
        sec.append(["", ""])

cols = [[], [], [], [], [], []]
tbl_colors = [[], [], []]
for i in range(num_rows):
    for j, sec in enumerate((sec1, sec2, sec3)):
        n, v = sec[i]
        is_hdr = v == "" and n.startswith("<b>")
        cols[j * 2].append(n)
        cols[j * 2 + 1].append(f"<b>{v}</b>" if v and not is_hdr else v)
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
            values=cols,
            fill_color=[tbl_colors[0], tbl_colors[0], tbl_colors[1], tbl_colors[1],
                        tbl_colors[2], tbl_colors[2]],
            font=dict(size=13, color="#212121"),
            align=["left", "right", "left", "right", "left", "right"],
            height=26,
        ),
    )
)
table_height = 32 + num_rows * 26 + 80
fig_stats.update_layout(
    title_text=f"<b>{ticker} — Walk-Forward: статистика OOS-стратегии</b>",
    title_x=0.5,
    title_font_size=18,
    height=table_height,
    width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# ── Таблица коэффициентов ────────────────────────────────────────────────
coefficients = [
    {
        "name": "Recovery Factor",
        "formula": "Чистая прибыль / |Max Drawdown|",
        "value": f"{recovery_factor:.2f}",
        "description": (
            "Коэффициент восстановления — показывает, во сколько раз OOS-прибыль "
            "превышает максимальную просадку. RF > 1 означает, что стратегия "
            "заработала больше, чем потеряла в худший период."
        ),
    },
    {
        "name": "Profit Factor",
        "formula": "Валовая прибыль / Валовый убыток",
        "value": f"{profit_factor:.2f}",
        "description": (
            "Фактор прибыли — отношение суммы прибыльных сделок к сумме убыточных. "
            "PF > 1 — прибыльность, 1.5–2.0 хорошо, > 2.0 отлично."
        ),
    },
    {
        "name": "Payoff Ratio",
        "formula": "Средний выигрыш / Средний проигрыш",
        "value": f"{payoff_ratio:.2f}",
        "description": (
            "Коэффициент выплат — отношение среднего размера прибыли к среднему "
            "размеру убытка. При высоком payoff стратегия остаётся прибыльной даже "
            "при win rate < 50%."
        ),
    },
    {
        "name": "Sharpe Ratio",
        "formula": "(Ср. P/L / Std) × √252",
        "value": f"{sharpe:.2f}",
        "description": (
            "Коэффициент Шарпа — отношение доходности к риску, приведённое к году. "
            "Sharpe > 1.0 — хорошо, > 2.0 — отлично, > 3.0 — исключительно."
        ),
    },
    {
        "name": "Sortino Ratio",
        "formula": "(Ср. P/L / Downside Std) × √252",
        "value": f"{sortino:.2f}",
        "description": (
            "Модификация Шарпа, учитывающая только нисходящую волатильность. "
            "Не штрафует за положительные всплески."
        ),
    },
    {
        "name": "Calmar Ratio",
        "formula": "Годовая доходность / |Max Drawdown|",
        "value": f"{calmar:.2f}",
        "description": (
            "Отношение годовой прибыли к максимальной просадке. "
            "Calmar > 1 — годовая прибыль превышает худшую просадку. "
            "Calmar > 3 — отличное соотношение доходности и риска."
        ),
    },
    {
        "name": "Expectancy",
        "formula": "Win% × Ср.выигрыш − Loss% × Ср.проигрыш",
        "value": f"{expectancy:,.0f}",
        "description": (
            "Математическое ожидание на одну сделку. "
            "Положительное значение означает, что стратегия имеет преимущество (edge)."
        ),
    },
]

fig_table = go.Figure(
    go.Table(
        columnwidth=[150, 250, 80, 450],
        header=dict(
            values=["<b>Коэффициент</b>", "<b>Формула</b>",
                    "<b>Значение</b>", "<b>Расшифровка</b>"],
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
            fill_color=[
                ["#f5f5f5" if i % 2 == 0 else "white" for i in range(len(coefficients))]
            ] * 4,
            font=dict(size=13, color="#212121"),
            align=["left", "left", "center", "left"],
            height=60,
        ),
    )
)
fig_table.update_layout(
    title_text=f"<b>{ticker} — Walk-Forward: ключевые коэффициенты</b>",
    title_x=0.5,
    title_font_size=18,
    height=560,
    width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# ── Таблица фолдов ───────────────────────────────────────────────────────
folds_values = [
    folds_df["fold"].tolist(),
    folds_df["train_from"].dt.strftime("%Y-%m-%d").tolist(),
    folds_df["train_to"].dt.strftime("%Y-%m-%d").tolist(),
    folds_df["test_from"].dt.strftime("%Y-%m-%d").tolist(),
    folds_df["test_to"].dt.strftime("%Y-%m-%d").tolist(),
    folds_df["n_rules"].tolist(),
    folds_df["n_trades"].tolist(),
    [f"{v:,.0f}" for v in folds_df["pnl"]],
    folds_df["fitted_rules"].astype(str).tolist(),
]

fig_folds = go.Figure(
    go.Table(
        columnwidth=[40, 90, 90, 90, 90, 60, 60, 90, 500],
        header=dict(
            values=["<b>Fold</b>", "<b>Train from</b>", "<b>Train to</b>",
                    "<b>Test from</b>", "<b>Test to</b>", "<b># rules</b>",
                    "<b># trades</b>", "<b>PnL</b>", "<b>Fitted rules</b>"],
            fill_color="#1565c0",
            font=dict(color="white", size=13),
            align="left",
            height=32,
        ),
        cells=dict(
            values=folds_values,
            fill_color=[["#f5f5f5" if i % 2 == 0 else "white" for i in range(len(folds_df))]] * 9,
            font=dict(size=12, color="#212121"),
            align="left",
            height=30,
        ),
    )
)
fig_folds.update_layout(
    title_text=f"<b>{ticker} — Walk-Forward: детали фолдов</b>",
    title_x=0.5,
    title_font_size=18,
    height=200 + 32 * len(folds_df),
    width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# ── Сохранение ───────────────────────────────────────────────────────────
output = SAVE_PATH / "plots" / "sentiment_walk_forward_analysis.html"
output.parent.mkdir(parents=True, exist_ok=True)

with open(output, "w", encoding="utf-8") as f:
    f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
    f.write(f"<title>{ticker} Walk-Forward Analysis</title>\n</head><body>\n")
    f.write(fig.to_html(include_plotlyjs="cdn", full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_stats.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_table.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_folds.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n</body></html>")

print(f"Отчёт сохранён: {output}")
print(f"\nИтого OOS: сделок={total_trades}, PnL={total_profit:,.0f}, "
      f"Winrate={win_rate:.1f}%, MaxDD={max_dd:,.0f}")
print(f"Sharpe={sharpe:.2f}, Sortino={sortino:.2f}, PF={profit_factor:.2f}, RF={recovery_factor:.2f}")
