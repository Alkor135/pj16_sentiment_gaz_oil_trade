"""
Сравнение двух стратегий (pj16 sentiment vs pj14 embedding) по RTS.
Строит HTML-отчёт: три equity-кривые (каждая стратегия + комбинация),
подробный отчёт по комбинированной стратегии (P/L по сделкам, недели, месяцы,
drawdown, распределение, скользящие средние, таблицы статистики и коэффициентов).
"""

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- Пути к файлам ---
PJ16_XLSX = Path(__file__).parent / "sentiment_backtest_results.xlsx"
PJ14_XLSX = Path(r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\rts\df_rez_output.xlsx")
OUTPUT_HTML = Path(__file__).parent / "plots" / "compare_strategies.html"

# --- Загрузка данных ---
df16 = pd.read_excel(PJ16_XLSX)
df14 = pd.read_excel(PJ14_XLSX)

df16["date"] = pd.to_datetime(df16["source_date"]).dt.date
df14["date"] = pd.to_datetime(df14["TRADEDATE"]).dt.date

# Объединение по дате
merged = pd.merge(
    df16[["date", "pnl"]].rename(columns={"pnl": "pnl_16"}),
    df14[["date", "P/L"]].rename(columns={"P/L": "pnl_14"}),
    on="date",
    how="outer",
).sort_values("date").reset_index(drop=True)

merged["pnl_16"] = merged["pnl_16"].fillna(0)
merged["pnl_14"] = merged["pnl_14"].fillna(0)
# Комбинация нормируется на 1 контракт: среднее от двух сигналов,
# чтобы масштаб совпадал с отдельными стратегиями.
merged["pnl_combined"] = (merged["pnl_16"] + merged["pnl_14"]) / 2

merged["cum_16"] = merged["pnl_16"].cumsum()
merged["cum_14"] = merged["pnl_14"].cumsum()
merged["cum_combined"] = merged["pnl_combined"].cumsum()

merged["date_dt"] = pd.to_datetime(merged["date"])


# --- Вспомогательные функции ---
def _max_consecutive(signs: pd.Series, target: int) -> int:
    best = current = 0
    for s in signs:
        if s == target:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _drawdown_duration(drawdown: pd.Series) -> int:
    max_dd_duration = 0
    current_dd_start = None
    for i, dd in enumerate(drawdown):
        if dd < 0:
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


# --- Сводная таблица сравнения ---
def calc_stats(pnl: pd.Series, name: str) -> dict:
    cum = pnl.cumsum()
    total = pnl.sum()
    trades = int((pnl != 0).sum())
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    win_rate = wins / trades * 100 if trades else 0
    avg_win = float(pnl[pnl > 0].mean()) if wins else 0
    avg_loss = float(pnl[pnl < 0].mean()) if losses else 0
    payoff = abs(avg_win / avg_loss) if avg_loss else 0
    expectancy = float(pnl[pnl != 0].mean()) if trades else 0
    running_max = cum.cummax()
    drawdown = cum - running_max
    max_dd = float(drawdown.min())
    pf_gross = float(pnl[pnl > 0].sum())
    pf_loss = abs(float(pnl[pnl < 0].sum()))
    profit_factor = pf_gross / pf_loss if pf_loss else 0
    recovery = total / abs(max_dd) if max_dd else 0
    return {
        "Стратегия": name,
        "Сделок": trades,
        "Win%": f"{win_rate:.1f}",
        "Total P/L": f"{total:,.0f}",
        "Max DD": f"{max_dd:,.0f}",
        "PF": f"{profit_factor:.2f}",
        "Payoff": f"{payoff:.2f}",
        "Expectancy": f"{expectancy:,.0f}",
        "Recovery": f"{recovery:.2f}",
    }


stats = pd.DataFrame([
    calc_stats(merged["pnl_16"], "pj16 Sentiment"),
    calc_stats(merged["pnl_14"], "pj14 Embedding"),
    calc_stats(merged["pnl_combined"], "Комбинация"),
])
stats_html = stats.to_html(index=False, classes="stats-table", border=0)

# === Сравнительные equity-кривые (все три стратегии на одном графике) ===
fig_compare = go.Figure()
fig_compare.add_trace(
    go.Scatter(
        x=merged["date"], y=merged["cum_16"],
        mode="lines",
        line=dict(color="#2e7d32", width=2),
        name="pj16 Sentiment",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    )
)
fig_compare.add_trace(
    go.Scatter(
        x=merged["date"], y=merged["cum_14"],
        mode="lines",
        line=dict(color="#1565c0", width=2),
        name="pj14 Embedding",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    )
)
fig_compare.add_trace(
    go.Scatter(
        x=merged["date"], y=merged["cum_combined"],
        mode="lines",
        line=dict(color="#6a1b9a", width=2.5),
        name="Комбинация (1 контракт)",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    )
)
fig_compare.update_layout(
    height=600, title_text="Сравнение стратегий — RTS (equity, 1 контракт)",
    title_x=0.5,
    showlegend=True,
    legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="center", x=0.5),
    template="plotly_white",
    hovermode="x unified",
)
fig_compare.update_yaxes(tickformat=",")

# === Подробный отчёт по комбинированной стратегии ===
pl = merged["pnl_combined"].astype(float)
cum = pl.cumsum()
dates = merged["date_dt"]

day_colors = ["#d32f2f" if v < 0 else "#2e7d32" for v in pl]

merged["Неделя"] = dates.dt.to_period("W")
weekly = merged.groupby("Неделя", as_index=False)["pnl_combined"].sum()
weekly["dt"] = weekly["Неделя"].apply(lambda p: p.start_time)
week_colors = ["#d32f2f" if v < 0 else "#00838f" for v in weekly["pnl_combined"]]

merged["Месяц"] = dates.dt.to_period("M")
monthly = merged.groupby("Месяц", as_index=False)["pnl_combined"].sum()
monthly["dt"] = monthly["Месяц"].dt.to_timestamp()
month_colors = ["#d32f2f" if v < 0 else "#1565c0" for v in monthly["pnl_combined"]]

running_max = cum.cummax()
drawdown = cum - running_max

for w in (5, 10, 20):
    merged[f"MA{w}"] = pl.rolling(w, min_periods=1).mean()

# ── Метрики ──
total_profit = float(cum.iloc[-1])
total_trades = len(merged)
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

date_range_days = (dates.max() - dates.min()).days or 1
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

# ── Графики комбинации ──
fig = make_subplots(
    rows=4, cols=2,
    subplot_titles=(
        "P/L по сделкам",
        "Накопленная прибыль (equity)",
        "P/L по неделям",
        "P/L по месяцам",
        "Drawdown от максимума",
        "Распределение P/L сделок",
        "Скользящие средние P/L (5/10/20)",
        "",
    ),
    specs=[
        [{"type": "bar"}, {"type": "scatter"}],
        [{"type": "bar"}, {"type": "bar"}],
        [{"type": "scatter"}, {"type": "histogram"}],
        [{"type": "scatter"}, {}],
    ],
    vertical_spacing=0.07,
    horizontal_spacing=0.08,
)

fig.add_trace(
    go.Bar(
        x=merged["date"], y=pl, marker_color=day_colors,
        name="P/L сделки",
        hovertemplate="%{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=1, col=1,
)
fig.add_trace(
    go.Scatter(
        x=merged["date"], y=cum,
        mode="lines", fill="tozeroy",
        line=dict(color="#6a1b9a", width=2),
        fillcolor="rgba(106,27,154,0.15)",
        name="Equity",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f}<extra></extra>",
    ),
    row=1, col=2,
)
fig.add_trace(
    go.Bar(
        x=weekly["dt"], y=weekly["pnl_combined"], marker_color=week_colors,
        name="P/L неделя",
        hovertemplate="Нед. %{x|%Y-%m-%d}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=2, col=1,
)
fig.add_trace(
    go.Bar(
        x=monthly["dt"], y=monthly["pnl_combined"], marker_color=month_colors,
        name="P/L месяц",
        text=[f"{v:,.0f}" for v in monthly["pnl_combined"]],
        textposition="outside",
        hovertemplate="%{x|%Y-%m}<br>P/L: %{y:,.0f}<extra></extra>",
    ),
    row=2, col=2,
)
fig.add_trace(
    go.Scatter(
        x=merged["date"], y=drawdown,
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
            x=merged["date"], y=merged[f"MA{w}"],
            mode="lines", line=dict(color=color, width=1.5),
            name=f"MA{w}",
            hovertemplate=f"MA{w}: " + "%{y:,.0f}<extra></extra>",
        ),
        row=4, col=1,
    )
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=4, col=1)

fig.update_layout(
    height=1800, width=1500,
    title_text=f"Комбинированная стратегия — RTS<br><sub>{stats_text}</sub>",
    title_x=0.5,
    showlegend=True,
    legend=dict(orientation="h", yanchor="top", y=-0.03, xanchor="center", x=0.5),
    template="plotly_white",
    hovermode="x unified",
)
for r, c in [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (4, 1)]:
    fig.update_yaxes(tickformat=",", row=r, col=c)

# ── Таблица статистики ──
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
    title_text="<b>Комбинация — RTS: статистика стратегии</b>",
    title_x=0.5, title_font_size=18,
    height=32 + num_rows * 26 + 80, width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# ── Таблица коэффициентов ──
coefficients = [
    {"name": "Recovery Factor", "formula": "Чистая прибыль / |Max Drawdown|",
     "value": f"{recovery_factor:.2f}",
     "description": "Коэффициент восстановления — во сколько раз прибыль превышает максимальную просадку. RF > 1 — стратегия заработала больше, чем потеряла в худший период."},
    {"name": "Profit Factor", "formula": "Валовая прибыль / Валовый убыток",
     "value": f"{profit_factor:.2f}",
     "description": "Фактор прибыли. PF > 1 — прибыльность, 1.5–2.0 хорошо, > 2.0 отлично."},
    {"name": "Payoff Ratio", "formula": "Средний выигрыш / Средний проигрыш",
     "value": f"{payoff_ratio:.2f}",
     "description": "При высоком payoff стратегия остаётся прибыльной даже при win rate < 50%."},
    {"name": "Sharpe Ratio", "formula": "(Ср. P/L / Std) × √252",
     "value": f"{sharpe:.2f}",
     "description": "Отношение доходности к риску, приведённое к году. > 1 хорошо, > 2 отлично, > 3 исключительно."},
    {"name": "Sortino Ratio", "formula": "(Ср. P/L / Downside Std) × √252",
     "value": f"{sortino:.2f}",
     "description": "Модификация Шарпа, учитывающая только нисходящую волатильность."},
    {"name": "Calmar Ratio", "formula": "Годовая доходность / |Max Drawdown|",
     "value": f"{calmar:.2f}",
     "description": "Отношение годовой прибыли к макс. просадке. > 1 — прибыль превышает худшую просадку, > 3 отлично."},
    {"name": "Expectancy", "formula": "Win% × Ср.выигрыш − Loss% × Ср.проигрыш",
     "value": f"{expectancy:,.0f}",
     "description": "Матожидание на одну сделку. Положительное — стратегия имеет преимущество (edge)."},
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
    title_text="<b>Комбинация — RTS: ключевые коэффициенты</b>",
    title_x=0.5, title_font_size=18,
    height=560, width=1500,
    margin=dict(l=20, r=20, t=60, b=20),
)

# === Сохранение HTML ===
OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
with OUTPUT_HTML.open("w", encoding="utf-8") as f:
    f.write("<!DOCTYPE html>\n<html><head><meta charset='utf-8'>\n")
    f.write("<title>Сравнение стратегий — RTS</title>\n")
    f.write("<style>\n")
    f.write("body { font-family: Arial, sans-serif; margin: 20px; background: #fafafa; }\n")
    f.write(".stats-table { border-collapse: collapse; margin: 20px auto; font-size: 14px; }\n")
    f.write(".stats-table th { background: #37474f; color: white; padding: 8px 16px; }\n")
    f.write(".stats-table td { padding: 6px 16px; border-bottom: 1px solid #ddd; text-align: center; }\n")
    f.write(".stats-table tr:hover { background: #e3f2fd; }\n")
    f.write("</style>\n</head><body>\n")
    f.write("<h2 style='text-align:center;'>Сводная статистика — RTS</h2>\n")
    f.write(stats_html)
    f.write(fig_compare.to_html(include_plotlyjs="cdn", full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_stats.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n<hr style='margin:30px 0; border:1px solid #ccc'>\n")
    f.write(fig_table.to_html(include_plotlyjs=False, full_html=False))
    f.write("\n</body></html>")

print(f"Отчёт сохранён: {OUTPUT_HTML}")
