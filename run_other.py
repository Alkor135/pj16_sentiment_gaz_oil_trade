"""
Скрипт для запуска на другом компьютере для формирования файлов отчета и анализа.
Или для запуска вне определенного времене для исключения формирования предсказаний  и открытия позиций.
"""

import subprocess
import sys
import os

BASE = r"C:\Users\Alkor\VSCode\pj16_sentiment_gaz_oil_trade"
PYTHON = os.path.join(BASE, ".venv", "Scripts", "python.exe")

# список скриптов по порядку
SCRIPTS = [
    r"rts\sentiment_analysis.py",
    r"rts\sentiment_group_stats.py",
    r"rts\sentiment_backtest.py",
    r"rts\sentiment_walk_forward.py",
    r"rts\sentiment_walk_forward_analysis.py",
    r"rts\compare_strategies.py",

    r"mix\sentiment_analysis.py",
    r"mix\sentiment_group_stats.py",
    r"mix\sentiment_backtest.py",
    r"mix\sentiment_walk_forward.py",
    r"mix\sentiment_walk_forward_analysis.py",
    r"mix\compare_strategies.py",
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
