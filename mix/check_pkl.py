"""
Просмотр содержимого sentiment_scores.pkl в консоли.
Загружает DataFrame, выводит shape, колонки, диапазон дат и сам df.
"""

import pickle
import sys
from pathlib import Path

import pandas as pd
import yaml

SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
    settings = yaml.safe_load(f) or {}

pkl_path = Path(settings.get("sentiment_output_pkl", "sentiment_scores.pkl"))
if not pkl_path.is_absolute():
    pkl_path = Path(__file__).parent / pkl_path

if not pkl_path.exists():
    print(f"Файл не найден: {pkl_path}")
    sys.exit(1)

with open(pkl_path, "rb") as f:
    df = pickle.load(f)

if not isinstance(df, pd.DataFrame):
    df = pd.DataFrame(df)

print(f"Файл: {pkl_path}")
print(f"Shape: {df.shape}")
print(f"Колонки: {list(df.columns)}")
if "source_date" in df.columns:
    print(f"Период: {df['source_date'].min()} .. {df['source_date'].max()}")

with pd.option_context(
    "display.width", 1000,
    "display.max_columns", 20,
    "display.max_colwidth", 60,
    "display.max_rows", 500,
    "display.float_format", "{:,.2f}".format,
):
    print()
    print(df)
