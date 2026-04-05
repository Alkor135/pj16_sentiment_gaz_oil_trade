"""
Скрипт для проверки содержимого pickle-файла с кэшем эмбеддингов.
Загружает DataFrame из .pkl файла и выводит:
- общее количество записей,
- TRADEDATE первых и последних файлов,
- количество чанков в первом документе,
- размерность одного эмбеддинга,
- пример текста и вектора (опционально).
"""

from pathlib import Path
import pickle
import pandas as pd
import numpy as np
import sys

# Путь к settings.yaml, чтобы получить имя ticker и путь к кэшу
SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

try:
    import yaml
    with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
        settings = yaml.safe_load(f)
except Exception as e:
    print(f"❌ Не удалось загрузить settings.yaml: {e}")
    sys.exit(1)

# Формируем путь к кэшу
ticker_lc = settings['ticker'].lower()
cache_file = Path(settings['cache_file'].replace('{ticker_lc}', ticker_lc))

# Проверяем существование файла
if not cache_file.exists():
    print(f"❌ Файл кэша не найден: {cache_file}")
    sys.exit(1)

print(f"✅ Загружаю кэш из: {cache_file}")

try:
    with open(cache_file, 'rb') as f:
        df = pickle.load(f)
    print(f"✅ Успешно загружено {len(df)} записей.")
except Exception as e:
    print(f"❌ Ошибка при загрузке .pkl файла: {e}")
    sys.exit(1)

# Выводим общую информацию
print("\n" + "="*60)
print("ОБЩАЯ ИНФОРМАЦИЯ")
print("="*60)
print(f"Количество отчётов: {len(df)}")
if len(df) > 0:
    print(f"Первые даты: {df['TRADEDATE'].head().tolist()}")
    print(f"Последние даты: {df['TRADEDATE'].tail().tolist()}")

    # Берём первый документ
    first_row = df.iloc[0]
    chunks = first_row['CHUNKS']
    print(f"\nЧанков в первом документе ({first_row['TRADEDATE']}): {len(chunks)}")

    if len(chunks) > 0:
        first_chunk = chunks[0]
        emb = first_chunk['embedding']
        print(f"Размерность эмбеддинга: {len(emb)}")
        print(f"Текст первого чанка (первые 300 символов):\n{first_chunk['text'][:300]}...")
        print(f"Вектор эмбеддинга (первые 10 элементов): {emb[:10]}")
else:
    print("Данные отсутствуют.")

with pd.option_context(
        "display.width", 1000,
        "display.max_columns", 10,
        "display.max_colwidth", 80
):
    print()
    print(df)

# Пример: поиск конкретной даты (можно раскомментировать)
# target_date = "2025-04-01"
# row = df[df['TRADEDATE'] == target_date]
# if len(row) > 0:
#     print(f"\nНайдена запись за {target_date}: {len(row.iloc[0]['CHUNKS'])} чанков")

print("\n✅ Просмотр завершён.")
