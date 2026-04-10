"""
Мастер-скрипт для последовательного запуска пайплайна по всем тикерам.
Порядок: sync → (download → convert → embedding → simulate → trade) × N тикеров.
Останавливается при первой ошибке (exit code != 0).
Запускается Планировщиком задач через одно задание.
"""

import subprocess
import sys
import os

BASE = r"C:\Users\Alkor\VSCode\pj16_sentiment_gaz_oil_trade"
PYTHON = os.path.join(BASE, ".venv", "Scripts", "python.exe")

# список скриптов по порядку
SCRIPTS = [
    # r"beget\sync_files.py",

    # r"rts\download_minutes_to_db.py",
    # r"rts\convert_minutes_to_days.py",
    # r"rts\create_markdown_files.py",
    r"rts\sentiment_analysis.py",
    r"rts\sentiment_to_predict.py",
    # r"trade\trade_rts_tri.py",

    # r"mix\download_minutes_to_db.py",
    # r"mix\convert_minutes_to_days.py",
    r"mix\sentiment_analysis.py",
    r"mix\sentiment_to_predict.py",
    # r"trade\trade_mix_tri.py",

    r"rts\sentiment_group_stats.py",
    r"rts\sentiment_backtest.py",
    r"mix\sentiment_group_stats.py",
    r"mix\sentiment_backtest.py",
]

def run_script(script: str) -> int:
    script_path = os.path.join(BASE, script)
    cwd = os.path.dirname(script_path)
    print(f"\n=== Запуск: {script} ===")
    result = subprocess.run([PYTHON, script_path], cwd=cwd)
    return result.returncode

def main():
    for script in SCRIPTS:
        code = run_script(script)
        if code != 0:
            print(f"❌ Ошибка выполнения {script}, код {code}")
            os.system("pause")
            sys.exit(code)
    print("\n✅ Все скрипты выполнены успешно")
    input("\nНажмите Enter для выхода...")  # вместо sys.exit вручную

if __name__ == "__main__":
    main()
