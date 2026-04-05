"""
Бэктест торговой стратегии на основе эмбеддингов новостей.
Загружает дневные котировки и кэш эмбеддингов, объединяет по датам.
Для каждой даты перебирает окна k=3..30, находит наиболее похожий день
по косинусному сходству чанков и формирует P/L на основе совпадения направлений.
Выбирает лучшее k по скользящей сумме P/L за test_days дней.
Применяет зеркальное отображение P/L (инверсия стратегии).
Строит график кумулятивного P/L с наложенной диаграммой лучших k.
Сохраняет результаты в Excel и explain-данные в pickle.
Делает предсказание на следующую торговую сессию и сохраняет в файл.
Конфигурация через settings.yaml, логирование с ротацией (3 файла).
"""

from pathlib import Path
from datetime import datetime
import pickle
import sqlite3
import logging
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Путь к settings.yaml в той же директории, что и скрипт
SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

_CHUNK_MATRIX_CACHE = {}  # Кэш для матриц чанков
EXPLAIN_STORE = {}  # { k -> [ { trade_date, best_j_date, score, pairs, body_cur, body_prev } ] }

# Чтение настроек
with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
    settings = yaml.safe_load(f)

# ==== Параметры ====
ticker = settings['ticker']
ticker_lc = ticker.lower()
cache_file = Path(settings['cache_file'].replace('{ticker_lc}', ticker_lc))  # Путь к pkl-файлу с кэшем
path_db_day = Path(settings['path_db_day'].replace('{ticker}', ticker))  # Путь к БД дневных котировок
predict_path = Path(settings['predict_path'].replace('{ticker_lc}', ticker_lc))
min_prev_files = settings.get('min_prev_files', 2)
test_days = settings.get('test_days', 23) + 1
START_DATE = settings.get('start_date_test', "2025-10-01")
model_name = settings.get('model_name', 'bge-m3')  # Ollama модель
provider = settings['provider']

# === Логирование ===
log_dir = Path(__file__).parent / 'log'
log_dir.mkdir(parents=True, exist_ok=True)
# Имя файла лога с датой и временем запуска (один файл на запуск!)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = log_dir / f'simulate_trade_{timestamp}.txt'

# Настройка логирования: ТОЛЬКО один файл + консоль
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),  # один файл
        logging.StreamHandler()                           # консоль
    ]
)

# Ручная очистка старых логов (оставляем только 3 самых новых)
def cleanup_old_logs(log_dir: Path, max_files: int = 3):
    """Удаляет старые лог-файлы, оставляя max_files самых новых."""
    log_files = sorted(log_dir.glob("simulate_trade_*.txt"))
    if len(log_files) > max_files:
        for old_file in log_files[:-max_files]:
            try:
                old_file.unlink()
                print(f"Удалён старый лог: {old_file.name}")
            except Exception as e:
                print(f"Не удалось удалить {old_file}: {e}")

# Вызываем очистку ПЕРЕД началом логирования
cleanup_old_logs(log_dir, max_files=3)
logging.info(f"🚀 Запуск скрипта. Лог-файл: {log_file}")

def load_quotes(path_db_quote):
    """Загрузка котировок и расчет NEXT_BODY."""
    with sqlite3.connect(path_db_quote) as conn:
        df = pd.read_sql_query(
            "SELECT TRADEDATE, OPEN, CLOSE FROM Futures",
            conn,
            parse_dates=['TRADEDATE']  # <-- Преобразуем TRADEDATE в datetime
        )
    df = df.set_index('TRADEDATE').sort_index()
    df['NEXT_BODY'] = (df['CLOSE'] - df['OPEN']).shift(-1)
    df = df.dropna(subset=['NEXT_BODY'])
    return df[['NEXT_BODY']]

def load_cache(cache_file_path):
    """Загрузка кэша эмбеддингов."""
    with open(cache_file_path, 'rb') as f:
        df = pickle.load(f)
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    return df.set_index('TRADEDATE').sort_index()

def chunks_to_matrix(chunks):
    key = id(chunks)
    if key not in _CHUNK_MATRIX_CACHE:
        _CHUNK_MATRIX_CACHE[key] = np.vstack(
            [c["embedding"] for c in chunks]
        ).astype(np.float32)
    return _CHUNK_MATRIX_CACHE[key]

def cosine(a: np.ndarray, b: np.ndarray) -> float:
    """Сравнение по косинусному сходству"""
    # эмбеддинги уже L2-нормализованы
    return float(np.dot(a, b))

def chunks_similarity_fast(
    chunks_a: list,
    chunks_b: list,
    top_k: int = 5
) -> float:
    """    Быстрое retriever-grade similarity через матричное умножение    """

    if not chunks_a or not chunks_b:
        return 0.0

    A = chunks_to_matrix(chunks_a)  # (Na, D)
    B = chunks_to_matrix(chunks_b)  # (Nb, D)

    # Все cosine similarity сразу
    S = A @ B.T  # (Na, Nb)

    # top-k по всем значениям
    flat = S.ravel()

    if flat.size <= top_k:
        return float(flat.mean())

    # быстрее чем sort
    top = np.partition(flat, -top_k)[-top_k:]
    return float(top.mean())

def chunks_similarity_with_explain(
    chunks_a: list,
    chunks_b: list,
    top_k: int = 5
):
    """
    Возвращает (score, pairs) где pairs — список топ-k совпадений:
    [{"chunk_a": idx_a, "chunk_b": idx_b, "similarity": float,
      "text_a": "...", "text_b": "..."}...]
    Использует матричную операцию, быстро.
    """
    if not chunks_a or not chunks_b:
        return 0.0, []

    A = chunks_to_matrix(chunks_a)  # (Na, D)
    B = chunks_to_matrix(chunks_b)  # (Nb, D)

    S = A @ B.T  # (Na, Nb)

    flat = S.ravel()
    total = flat.size
    k = min(top_k, total)

    if total == 0:
        return 0.0, []

    # get indices of top-k in flattened array
    if total <= k:
        top_idx = np.arange(total)
    else:
        top_idx = np.argpartition(flat, -k)[-k:]

    # build readable pairs
    Nb = B.shape[0]
    pairs = []
    # sort top_idx by actual similarity descending for readability
    top_idx_sorted = top_idx[np.argsort(flat[top_idx])[::-1]]

    for idx in top_idx_sorted:
        ia = int(idx // Nb)
        ib = int(idx % Nb)
        sim = float(S[ia, ib])
        text_a = chunks_a[ia].get("text", "") if isinstance(chunks_a[ia], dict) else ""
        text_b = chunks_b[ib].get("text", "") if isinstance(chunks_b[ib], dict) else ""
        pairs.append({
            "chunk_a": ia,
            "chunk_b": ib,
            "similarity": sim,
            "text_a": text_a,
            "text_b": text_b
        })

    score = float(np.mean([p["similarity"] for p in pairs])) if pairs else 0.0
    return score, pairs

def compute_max_k(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    k: int,
    col_chunks: str = "CHUNKS",
    col_body: str = "NEXT_BODY",
    top_k_chunks: int = 5
) -> pd.Series:

    result = pd.Series(index=df.index, dtype=float)

    dates = df.index
    start_pos = dates.get_loc(start_date)

    # ensure explain list exists for this k
    EXPLAIN_STORE.setdefault(k, [])

    for i in range(start_pos, len(df)):
        if i < k:
            continue

        chunks_cur = df.iloc[i][col_chunks]
        body_cur = df.iloc[i][col_body]

        similarities = []
        indices = []

        # быстрые симы для выбора best_j
        for j in range(i - k, i):
            chunks_prev = df.iloc[j][col_chunks]

            sim = chunks_similarity_fast(
                chunks_cur,
                chunks_prev,
                top_k=top_k_chunks
            )

            similarities.append(sim)
            indices.append(j)

        # индекс самой похожей строки
        best_idx = int(np.argmax(similarities))
        best_j = indices[best_idx]
        body_prev = df.iloc[best_j][col_body]

        # --- записываем explain только для выбранного best_j ---
        try:
            score, pairs = chunks_similarity_with_explain(
                chunks_cur,
                df.iloc[best_j][col_chunks],
                top_k=top_k_chunks
            )
        except Exception as e:
            logging.error(f"Ошибка при формировании explain для {dates[i]} vs {dates[best_j]}: {e}")
            score, pairs = similarities[best_idx], []

        EXPLAIN_STORE[k].append({
            "trade_date": dates[i],
            "best_j_date": dates[best_j],
            "score": float(similarities[best_idx]),
            "explained_score": float(score),
            "pairs": pairs,
            "body_cur": float(body_cur),
            "body_prev": float(body_prev)
        })
        # --------------------------------------------------------

        if np.sign(body_cur) == np.sign(body_prev):
            result.iloc[i] = abs(body_cur)
        else:
            result.iloc[i] = -abs(body_cur)

    return result

# ==========================================================
# ПРЕДСКАЗАНИЕ НА СЛЕДУЮЩУЮ СЕССИЮ
# ==========================================================
def predict_next_session(
    df_emb: pd.DataFrame,
    df_rez: pd.DataFrame,
    df_bar: pd.DataFrame,
    predict_dir: Path,
    top_k_chunks: int = 5
):
    """
    Делает предсказание на следующую торговую сессию
    на основе последней даты из embeddings_ollama.pkl
    """

    logging.info("🔮 Запуск предсказания следующей сессии")

    # Последний markdown-файл с новостями (дата из имени файла = дата в кэше эмбеддингов)
    last_md_date = df_emb.index.max().date()
    logging.info(f"Последний md-файл с новостями: {last_md_date}.md")

    # Последняя дневная свеча в БД котировок (напрямую из БД, без отсечения NaN)
    with sqlite3.connect(path_db_day) as conn:
        last_bar_date = pd.read_sql_query(
            "SELECT MAX(TRADEDATE) as max_date FROM Futures", conn
        ).iloc[0]["max_date"]
    logging.info(f"Последняя дневная свеча в БД: {last_bar_date}")

    # ------------------------------------------------------
    # 1️⃣ Последняя дата из embeddings
    # ------------------------------------------------------
    last_date = df_emb.index.max()
    chunks_current = df_emb.loc[last_date, "CHUNKS"]

    logging.info(f"Последняя дата в embeddings: {last_date.date()}")

    # ------------------------------------------------------
    # 2️⃣ Определяем лучшее окно k
    #     Берём последнее значение из df_rez
    # ------------------------------------------------------
    best_k = int(df_rez.iloc[-1]["max"])
    logging.info(f"Лучшее окно k из тестирования: {best_k}")

    # ------------------------------------------------------
    # 3️⃣ Формируем окно предыдущих k дней
    # ------------------------------------------------------
    emb_dates = df_emb.index.sort_values()
    last_pos = emb_dates.get_loc(last_date)

    if last_pos < best_k:
        logging.warning("Недостаточно истории для предсказания")
        return

    compare_indices = range(last_pos - best_k, last_pos)

    similarities = []
    indices = []

    for j in compare_indices:
        date_prev = emb_dates[j]
        chunks_prev = df_emb.iloc[j]["CHUNKS"]

        sim = chunks_similarity_fast(
            chunks_current,
            chunks_prev,
            top_k=top_k_chunks
        )

        similarities.append(sim)
        indices.append(j)

    # ------------------------------------------------------
    # 4️⃣ Находим самый похожий день
    # ------------------------------------------------------
    best_idx = int(np.argmax(similarities))
    best_j = indices[best_idx]
    best_date = emb_dates[best_j]
    best_similarity = similarities[best_idx]

    logging.info(f"Самый похожий день: {best_date.date()}")
    logging.info(f"Similarity: {best_similarity:.4f}")

    # ------------------------------------------------------
    # 5️⃣ Определяем направление движения
    #     ИСПОЛЬЗУЕМ NEXT_BODY (как в тестировании)
    # ------------------------------------------------------
    # df_rez не содержит NEXT_BODY,
    # поэтому нужно взять его из df_combined.
    # Но df_combined в эту функцию не передаётся,
    # поэтому правильнее передать df_bar (где есть NEXT_BODY).

    # Получаем NEXT_BODY исторического похожего дня
    if best_date not in df_bar.index:
        logging.warning("Лучший день отсутствует в df_bar (нет NEXT_BODY)")
        return

    next_body_value = df_bar.loc[best_date, "NEXT_BODY"]

    # Определяем направление строго по знаку NEXT_BODY
    if next_body_value >= 0:
        direction = "up"
    elif next_body_value < 0:
        direction = "down"

    logging.info(f"NEXT_BODY похожего дня: {next_body_value:.2f}")
    logging.info(f"Предсказанное направление: {direction}")

    # ------------------------------------------------------
    # 6️⃣ Формируем список дат для сравнения
    # ------------------------------------------------------
    compare_dates = [str(emb_dates[i].date()) for i in compare_indices]

    # ------------------------------------------------------
    # 7️⃣ Сохраняем файл
    # ------------------------------------------------------
    predict_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{last_date.date()}.txt"
    file_path = predict_dir / file_name

    if file_path.exists():
        logging.info(f"⏭️ Файл предсказания уже существует, пропускаем: {file_path}")
        return

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(
            f"Дата предсказания: {last_date.date()}, предыдущих дней для сравнения {best_k}:\n"
        )
        f.write(
            f"Даты для сравнения: {', '.join(compare_dates)}\n"
        )
        f.write(f"Предсказанное направление: {direction}\n")
        f.write(f"NEXT_BODY похожего дня: {next_body_value:.2f}\n")
        f.write(f"Процент сходства: {best_similarity * 100:.2f}%\n")
        f.write("Метаданные ближайшего похожего:\n")
        f.write(f"  date: {best_date.date()}\n")

    logging.info(f"📁 Предсказание сохранено: {file_path}")

def main(path_db_day, cache_file):
    df_bar = load_quotes(path_db_day)  # Загрузка DF с дневными котировками (с 21:00 пред. сессии)
    df_emb = load_cache(cache_file)  # Загрузка DF с векторами новостей

    # Объединение датафреймов по индексу TRADEDATE
    df_combined = df_bar.join(df_emb[['CHUNKS']], how='inner')  # 'inner' — только общие даты

    # Генерация колонок MAX_3 … MAX_30
    start_date = pd.to_datetime(START_DATE)
    for k in range(3, 31):
        col_name = f"MAX_{k}"
        logging.info(f"📊 Расчёт {col_name}")
        df_combined[col_name] = compute_max_k(
            df=df_combined,
            start_date=start_date,
            k=k
        )

    # --- Сохраняем explain-результаты в pickle для дальнейшего анализа ---
    explain_dir = cache_file.parent
    explain_path = explain_dir / f"explain_topk_all.pkl"  # _{timestamp}
    try:
        with open(explain_path, "wb") as ef:
            pickle.dump(EXPLAIN_STORE, ef)
        logging.info(f"🔍 Explain saved: {explain_path}")
    except Exception as e:
        logging.error(f"Не удалось сохранить explain: {e}")

    # debug: показать примеры explain для k=5, если есть
    example_k = 5
    if example_k in EXPLAIN_STORE and EXPLAIN_STORE[example_k]:
        sample = EXPLAIN_STORE[example_k][-1]  # последний рассчитанный день
        logging.info(
            f"Пример explain для k={example_k}: trade_date={sample['trade_date']}, "
            f"best_j={sample['best_j_date']}, score={sample['score']}")
        for p in sample["pairs"][:5]:
            logging.info(
                f"  sim={p['similarity']:.4f} | A[{p['chunk_a']}]='{p['text_a'][:120]}' -> "
                f"B[{p['chunk_b']}]='{p['text_b'][:120]}'")

    # === Замена NaN на 0.0 во всех MAX_ колонках ===
    max_cols = [f"MAX_{k}" for k in range(3, 31)]
    df_combined[max_cols] = df_combined[max_cols].fillna(0.0)

    # === Расчёт PL_ колонок ===
    for k in range(3, 31):
        max_col = f"MAX_{k}"
        pl_col = f"PL_{k}"

        df_combined[pl_col] = (
            df_combined[max_col]
            .shift(1)  # исключаем текущую строку
            .rolling(window=test_days, min_periods=1)
            .sum()
        )

    # Отладочный вывод
    with pd.option_context(
        "display.width", 1000,
        "display.max_columns", 10,
        "display.max_colwidth", 90
    ):
        print("\ndf_bar:")
        print(df_bar)
        print("\ndf_emb:")
        print(df_emb)
        print('\ndf_combined[["NEXT_BODY", "CHUNKS"]]:')
        print(df_combined[["NEXT_BODY", "CHUNKS"]])
        print("\ndf_combined:")
        print(df_combined)

    # === Замена NaN на 0.0 во всех колонках ===
    df_combined = df_combined.fillna(0.0)

    # === ОСТАВИТЬ ТОЛЬКО НУЖНЫЕ КОЛОНКИ ===
    final_cols = [f"MAX_{k}" for k in range(3, 31)] + [f"PL_{k}" for k in range(3, 31)]
    df_combined = df_combined[final_cols].copy()

    # Опционально: сортировка по индексу (по дате)
    df_combined.sort_index(inplace=True)

    # Отладочный вывод
    with pd.option_context(
        "display.width", 1000,
        "display.max_columns", 30,
        "display.max_colwidth", 120,
        "display.min_rows", 30
    ):
        print("\nКомбинированный DataFrame (df_combined) с MAX_ и PL_ колонками:")
        print(df_combined[[f"PL_{k}" for k in range(3, 31)]])

    # ===============================
    # Формирование df_rez
    # ===============================

    pl_cols = [f"PL_{k}" for k in range(3, 31)]
    max_cols = [f"MAX_{k}" for k in range(3, 31)]

    rows = []

    for idx, row in df_combined.iterrows():
        trade_date = idx

        # максимальное значение среди PL_3 ... PL_30
        pl_values = row[pl_cols]
        pl_max = pl_values.max()

        pl_result = 0.0

        # ---
        # if pl_max > 0.0:
        # имя колонки с максимальным PL
        best_pl_col = pl_values.idxmax()  # например "PL_7"
        n = int(best_pl_col.split("_")[1])  # -> 7

        # соответствующая колонка MAX_n
        max_col = f"MAX_{n}"
        pl_result = row[max_col]
        # ---

        rows.append({
            "TRADEDATE": trade_date,
            "P/L": pl_result,
            "max": n
        })

    df_rez = pd.DataFrame(rows).set_index("TRADEDATE")

    # ===============================
    # Вывод df_rez в консоль
    # ===============================
    with pd.option_context(
            "display.width", 1000,
            "display.max_columns", 10,
            "display.max_colwidth", 120
    ):
        print("\nРезультирующий DataFrame (df_rez):")
        print(df_rez)

    # --- ЗЕРКАЛЬНОЕ ОТОБРАЖЕНИЕ (инверсия стратегии) ---
    df_rez["P/L"] *= -1
    # ---------------------------------------------------

    # Сохранение DataFrame в Excel файл (уже с инверсией P/L)
    df_rez.to_excel(Path(__file__).parent / 'df_rez_output.xlsx', index=True)

    # ===============================
    # График cumulative P/L + наложенная столбчатая диаграмма max
    # ===============================

    df_rez["CUM_P/L"] = df_rez["P/L"].cumsum()

    fig, ax1 = plt.subplots(figsize=(12, 7))

    # Основной график: Cumulative P/L (справа)
    ax1.plot(
        df_rez.index, df_rez["CUM_P/L"],
        marker='o',
        markersize=4,
        color='tab:blue',
        label='Cumulative P/L'
    )
    ax1.set_ylabel("Cumulative P/L", color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_xlabel("Date")
    ax1.grid(True, axis='y', alpha=0.3)
    ax1.set_title(
        f"{ticker} Cumulative P/L & Best Window (k) "
        f"{model_name.split(':')[0]} {provider} {timestamp}"
        )

    # Вторая ось Y для столбчатой диаграммы (слева)
    ax2 = ax1.twinx()
    ax2.bar(
        df_rez.index, df_rez["max"],
        alpha=0.5,
        color='tab:green',
        width=0.5,
        label="Best Window (k)"
    )
    ax2.set_ylabel("Best Window (k)", color='tab:green')
    ax2.tick_params(axis='y', labelcolor='tab:green')
    ax2.set_ylim(df_rez["max"].min() - 1, df_rez["max"].max() + 1)

    # Объединение легенды
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    # Оформление оси X
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    fig.tight_layout()

    # Сохранение графика
    plot_dir = Path(__file__).parent / 'plots'
    plot_dir.mkdir(exist_ok=True)
    plot_path = plot_dir / f'{model_name.split(":")[0]}_{provider}.png'
    plt.savefig(plot_path)
    logging.info(f"📊 График сохранён: {plot_path}")

    # ======================================================
    # ВЫЗОВ ПРЕДСКАЗАНИЯ НА СЛЕДУЮЩУЮ СЕССИЮ
    # ======================================================
    # predict_dir = predict_path
    predict_next_session(
        df_emb=df_emb,
        df_rez=df_rez,
        df_bar=df_bar,
        predict_dir=predict_path
    )

    plt.close()  # Освобождаем память

if __name__ == "__main__":
    main(path_db_day, cache_file)