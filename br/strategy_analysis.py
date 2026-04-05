"""
Интерактивный анализ стратегии на основе df_rez_output.xlsx (Plotly).
Генерирует HTML-файл с графиками: дневной/недельный/месячный/годовой P/L,
накопленная прибыль, распределение P/L, drawdown, скользящие средние.
Таблицы: статистика стратегии и ключевые коэффициенты.
Конфигурация через settings.yaml (ticker, provider, model_name).
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import yaml

# ── Конфигурация ─────────────────────────────────────────────────────────
SETTINGS_FILE = Path(__file__).parent / "settings.yaml"
with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
    settings = yaml.safe_load(f)

ticker = settings["ticker"]
model_name = settings.get("model_name", "embeddinggemma")
provider = settings.get("provider", "")

SAVE_PATH = Path(__file__).parent
INPUT_FILE = SAVE_PATH / "df_rez_output.xlsx"

# ── Загрузка данных ──────────────────────────────────────────────────────
df = pd.read_excel(INPUT_FILE)
df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])
df = df.sort_values("TRADEDATE").reset_index(drop=True)

pl = df["P/L"].astype(float)
cum = pl.cumsum()

# ── Агрегации ────────────────────────────────────────────────────────────
# Дневные цвета
day_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in pl]

# Недельная
df["Неделя"] = df["TRADEDATE"].dt.to_period("W")
weekly = df.groupby("Неделя", as_index=False)["P/L"].sum()
weekly["dt"] = weekly["Неделя"].apply(lambda p: p.start_time)
week_colors = ["#d32f2f" if v < 0 else "#00838f" for v in weekly["P/L"]]

# Месячная
df["Месяц"] = df["TRADEDATE"].dt.to_period("M")
monthly = df.groupby("Месяц", as_index=False)["P/L"].sum()
monthly["dt"] = monthly["Месяц"].dt.to_timestamp()
month_colors = ["#d32f2f" if v < 0 else "#1565c0" for v in monthly["P/L"]]

# Годовая
df["Год"] = df["TRADEDATE"].dt.to_period("Y")
yearly = df.groupby("Год", as_index=False)["P/L"].sum()
yearly["dt"] = yearly["Год"].dt.to_timestamp()
year_colors = ["#d32f2f" if v < 0 else "#4a148c" for v in yearly["P/L"]]

# Drawdown
running_max = cum.cummax()
drawdown = cum - running_max

# Скользящие средние
for w in (7, 14, 30):
    df[f"MA{w}"] = pl.rolling(w, min_periods=1).mean()

# ── Метрики стратегии ────────────────────────────────────────────────────
total_profit = cum.iloc[-1]
total_days = len(df)
win_days = int((pl > 0).sum())
loss_days = int((pl < 0).sum())
zero_days = int((pl == 0).sum())
trade_days = win_days + loss_days
win_rate = win_days / max(trade_days, 1) * 100
max_dd = drawdown.min()
best_day = pl.max()
worst_day = pl.min()
avg_day = pl.mean()
median_day = pl.median()
std_day = pl.std()

gross_profit = pl[pl > 0].sum()
gross_loss = abs(pl[pl < 0].sum())
avg_win = pl[pl > 0].mean() if win_days else 0
avg_loss = abs(pl[pl < 0].mean()) if loss_days else 0

profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
payoff_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")
recovery_factor = total_profit / abs(max_dd) if max_dd != 0 else float("inf")
expectancy = (win_rate / 100) * avg_win - (1 - win_rate / 100) * avg_loss
sharpe = (avg_day / std_day) * np.sqrt(252) if std_day > 0 else 0

downside = pl[pl < 0]
downside_std = downside.std() if len(downside) > 1 else 0
sortino = (avg_day / downside_std) * np.sqrt(252) if downside_std > 0 else 0

date_range_days = (df["TRADEDATE"].max() - df["TRADEDATE"].min()).days or 1
annual_profit = total_profit * 365 / date_range_days
calmar = annual_profit / abs(max_dd) if max_dd != 0 else float("inf")

# Макс. серии побед / убытков
def max_consecutive(series, condition):
    streaks = (series != condition).cumsum()
    filtered = series[series == condition]
    if filtered.empty:
        return 0
    return filtered.groupby(streaks[series == condition]).size().max()

signs = pl.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
max_consec_wins = max_consecutive(signs, 1)
max_consec_losses = max_consecutive(signs, -1)

# Длительность макс. просадки
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

volatility = std_day * np.sqrt(252)

win_months = int((monthly["P/L"] > 0).sum())
loss_months = int((monthly["P/L"] < 0).sum())

# Строка для заголовка
stats_text = (
    f"Итого: {total_profit:,.0f} | Дней: {total_days} | "
    f"Win: {win_days} ({win_rate:.0f}%) | Loss: {loss_days} | "
    f"PF: {profit_factor:.2f} | RF: {recovery_factor:.2f} | "
    f"Sharpe: {sharpe:.2f} | MaxDD: {max_dd:,.0f}"
)

# ── Построение графиков ──────────────────────────────────────────────────
fig = make_subplots(
    rows=5, cols=2,
    subplot_titles=(
        "P/L по дням",
        "Накопленная прибыль",
        "P/L по неделям",
        "P/L по месяцам",
        "P/L по годам",
        "Распределение дневных P/L",
        "Drawdown от максимума",
        "Скользящие средние P/L (7/14/30 дней)",
        "Best Window (k) по дням",
        "Recovery Factor (скользящий)",
    ),
    specs=[
        [{"type": "bar"}, {"type": "scatter"}],
        [{"type": "bar"}, {"type": "bar"}],
        [{"type": "bar"}, {"type": "histogram"}],
        [{"type": "scatter"}, {"type": "scatter"}],
        [{"type": "bar"}, {"type": "scatter"}],
    ],
    vertical_spacing=0.06,
    horizontal_spacing=0.06,
)

# 1) Дневной P/L
fig.add_trace(
    go.Bar(
        x=df["TRADEDATE"], y=pl, marker_color=day_colors,
        name="P/L день",
        hovertemplate="%{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=1, col=1,
)

# 2) Накопленная прибыль
fig.add_trace(
    go.Scatter(
        x=df["TRADEDATE"], y=cum,
        mode="lines", fill="tozeroy",
        line=dict(color="#2e7d32", width=2),
        fillcolor="rgba(46,125,50,0.15)",
        name="Накопл. прибыль",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    ),
    row=1, col=2,
)

# 3) Недельный P/L
fig.add_trace(
    go.Bar(
        x=weekly["dt"], y=weekly["P/L"], marker_color=week_colors,
        name="P/L неделя",
        hovertemplate="Нед. %{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=2, col=1,
)

# 4) Месячный P/L
fig.add_trace(
    go.Bar(
        x=monthly["dt"], y=monthly["P/L"], marker_color=month_colors,
        name="P/L месяц",
        text=[f"{v:,.0f}" for v in monthly["P/L"]],
        textposition="outside",
        hovertemplate="%{x|%Y-%m}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=2, col=2,
)

# 5) Годовой P/L
fig.add_trace(
    go.Bar(
        x=yearly["dt"], y=yearly["P/L"], marker_color=year_colors,
        name="P/L год",
        text=[f"{v:,.0f}" for v in yearly["P/L"]],
        textposition="outside",
        hovertemplate="%{x|%Y}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=3, col=1,
)

# 6) Распределение P/L
pl_pos = pl[pl > 0]
pl_neg = pl[pl < 0]
fig.add_trace(
    go.Histogram(
        x=pl_pos, marker_color="#2e7d32", opacity=0.7,
        name="Прибыль", nbinsx=30,
        hovertemplate="P/L: %{x:,.0f}<br>Кол-во: %{y}<extra></extra>",
    ),
    row=3, col=2,
)
fig.add_trace(
    go.Histogram(
        x=pl_neg, marker_color="#d32f2f", opacity=0.7,
        name="Убыток", nbinsx=30,
        hovertemplate="P/L: %{x:,.0f}<br>Кол-во: %{y}<extra></extra>",
    ),
    row=3, col=2,
)

# 7) Drawdown
fig.add_trace(
    go.Scatter(
        x=df["TRADEDATE"], y=drawdown,
        mode="lines", fill="tozeroy",
        line=dict(color="#d32f2f", width=1.5),
        fillcolor="rgba(211,47,47,0.2)",
        name="Drawdown",
        hovertemplate="%{x|%Y-%m-%d}<br>DD: %{y:,.0f}<extra></extra>",
    ),
    row=4, col=1,
)

# 8) Скользящие средние P/L
for w, color in [(7, "#1565c0"), (14, "#ff6f00"), (30, "#7b1fa2")]:
    fig.add_trace(
        go.Scatter(
            x=df["TRADEDATE"], y=df[f"MA{w}"],
            mode="lines", line=dict(color=color, width=1.5),
            name=f"MA{w}",
            hovertemplate=f"MA{w}: " + "%{y:,.0f}<extra></extra>",
        ),
        row=4, col=2,
    )
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=4, col=2)

# 9) Best Window (k) по дням
k_colors = ["#7b1fa2" if v <= 8 else "#1565c0" for v in df["max"]]
fig.add_trace(
    go.Bar(
        x=df["TRADEDATE"], y=df["max"], marker_color=k_colors,
        name="Best k",
        hovertemplate="%{x|%Y-%m-%d}<br>k = %{y}<extra></extra>",
    ),
    row=5, col=1,
)

# 10) Recovery Factor (скользящий)
rf_rolling = pd.Series(dtype=float, index=df.index)
for i in range(len(df)):
    dd_so_far = (cum.iloc[:i + 1] - cum.iloc[:i + 1].cummax()).min()
    rf_rolling.iloc[i] = cum.iloc[i] / abs(dd_so_far) if dd_so_far != 0 else 0
fig.add_trace(
    go.Scatter(
        x=df["TRADEDATE"], y=rf_rolling,
        mode="lines", line=dict(color="#00695c", width=2),
        name="Recovery Factor",
        hovertemplate="%{x|%Y-%m-%d}<br>RF: %{y:.2f}<extra></extra>",
    ),
    row=5, col=2,
)
fig.add_hline(y=1, line_dash="dash", line_color="gray", row=5, col=2,
              annotation_text="RF=1")

# ── Оформление ───────────────────────────────────────────────────────────
fig.update_layout(
    height=2200,
    width=1500,
    title_text=(
        f"{ticker} Strategy Analysis — {model_name} / {provider}"
        f"<br><sub>{stats_text}</sub>"
    ),
    title_x=0.5,
    showlegend=True,
    legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
    template="plotly_white",
    hovermode="x unified",
)

for row, col in [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (4, 1), (4, 2), (5, 1)]:
    fig.update_yaxes(tickformat=",", row=row, col=col)
fig.update_xaxes(tickformat="%Y-%m-%d", row=3, col=2, title_text="P/L")
fig.update_xaxes(tickformat="%Y", dtick="M12", row=3, col=1)

# ── Таблица статистики (3 секции: Доходность | Риск | Статистика сделок) ──
sec1 = [
    ["<b>ДОХОДНОСТЬ</b>", ""],
    ["Чистая прибыль", f"{total_profit:,.0f}"],
    ["Годовая прибыль (экстрапол.)", f"{annual_profit:,.0f}"],
    ["Средний P/L в день", f"{avg_day:,.0f}"],
    ["Медианный P/L в день", f"{median_day:,.0f}"],
    ["Лучший день", f"{best_day:,.0f}"],
    ["Худший день", f"{worst_day:,.0f}"],
]
sec2 = [
    ["<b>РИСК</b>", ""],
    ["Max Drawdown", f"{max_dd:,.0f}"],
    ["Длит. макс. просадки", f"{max_dd_duration} дней"],
    ["Волатильность (год.)", f"{volatility:,.0f}"],
    ["Std дневного P/L", f"{std_day:,.0f}"],
    ["VaR 95%", f"{np.percentile(pl, 5):,.0f}"],
    ["CVaR 95%", f"{pl[pl <= np.percentile(pl, 5)].mean():,.0f}"],
]
sec3 = [
    ["<b>СТАТИСТИКА СДЕЛОК</b>", ""],
    ["Торговых дней", f"{total_days}"],
    ["Win / Loss / Zero", f"{win_days} / {loss_days} / {zero_days}"],
    ["Win rate", f"{win_rate:.1f}%"],
    ["Ср. выигрыш / проигрыш", f"{avg_win:,.0f} / {avg_loss:,.0f}"],
    ["Макс. серия побед", f"{max_consec_wins}"],
    ["Макс. серия убытков", f"{max_consec_losses}"],
    ["Прибыльных месяцев", f"{win_months} / {win_months + loss_months}"],
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
        columnwidth=[200, 130, 180, 120, 200, 140],
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
    title_text=f"<b>{ticker} — Статистика стратегии</b>",
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
            "Коэффициент восстановления — показывает, во сколько раз чистая прибыль "
            "превышает максимальную просадку. RF > 1 означает, что стратегия "
            "заработала больше, чем потеряла в худший период."
        ),
    },
    {
        "name": "Profit Factor",
        "formula": "Валовая прибыль / Валовый убыток",
        "value": f"{profit_factor:.2f}",
        "description": (
            "Фактор прибыли — отношение суммы всех прибыльных дней к сумме всех "
            "убыточных. PF > 1 означает прибыльность. "
            "Значения 1.5–2.0 считаются хорошими, > 2.0 — отличными."
        ),
    },
    {
        "name": "Payoff Ratio",
        "formula": "Средний выигрыш / Средний проигрыш",
        "value": f"{payoff_ratio:.2f}",
        "description": (
            "Коэффициент выплат — отношение среднего размера прибыльной сделки к среднему "
            "размеру убыточной. Даже при win rate < 50% стратегия может быть прибыльной "
            "при высоком Payoff."
        ),
    },
    {
        "name": "Sharpe Ratio",
        "formula": "(Ср. дневной P/L / Std) × √252",
        "value": f"{sharpe:.2f}",
        "description": (
            "Коэффициент Шарпа — отношение доходности к риску, приведённое к году. "
            "Sharpe > 1.0 — хорошо, > 2.0 — отлично, > 3.0 — исключительно."
        ),
    },
    {
        "name": "Sortino Ratio",
        "formula": "(Ср. дневной P/L / Downside Std) × √252",
        "value": f"{sortino:.2f}",
        "description": (
            "Модификация Шарпа, учитывающая только нисходящую волатильность. "
            "Не штрафует за положительные всплески. Обычно Sortino > Sharpe."
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
    title_text=f"<b>{ticker} — Ключевые коэффициенты торговой стратегии</b>",
    title_x=0.5,
    title_font_size=18,
    height=560,
    width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# ── Сохранение в HTML ────────────────────────────────────────────────────
output = SAVE_PATH / "plots" / "strategy_analysis.html"

charts_html = fig.to_html(include_plotlyjs="cdn", full_html=False)

with open(output, "w", encoding="utf-8") as f:
    f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
    f.write(f"<title>{ticker} Strategy Analysis</title>\n</head><body>\n")
    f.write(charts_html)
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_stats.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_table.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n</body></html>")

print(f"Отчёт сохранён: {output}")
