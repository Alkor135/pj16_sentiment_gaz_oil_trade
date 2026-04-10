"""
Исполнение сделок по фьючерсу RTS в QUIK через .tri-файлы.
Читает предсказание текущего дня, сравнивает с предыдущим.
Открывает позицию в предсказнную сторону.
Поддерживает ролловер: при смене контракта закрывает старый и открывает новый.
Конфигурация (тикеры, количество) в rts\settings.yaml, логирование с ротацией (3 файла).
"""

from pathlib import Path
from datetime import datetime, date
import re
import logging
import yaml

# --- Конфигурация из settings.yaml ---
ticker_lc = 'rts'
settings_path = Path(__file__).parent.parent / ticker_lc / 'settings.yaml'
trade_settings_path = Path(__file__).parent / 'settings.yaml'

with open(settings_path, encoding='utf-8') as f:
    cfg = yaml.safe_load(f)
with open(trade_settings_path, encoding='utf-8') as f:
    trade_cfg = yaml.safe_load(f)

ticker_close = cfg['ticker_close']
ticker_open = cfg['ticker_open']
quantity_close = str(cfg.get('quantity_close', 1))
quantity_open = str(cfg.get('quantity_open', 1))
trade_account = trade_cfg['trade_account']

# Пути к файлам
predict_path = Path(cfg['predict_path'].format(ticker_lc=ticker_lc))
log_path = Path(__file__).parent / "log"
trade_path = Path(trade_cfg['trade_path'])
trade_filepath = trade_path / "input.tri"

# Создание необходимых директорий
trade_path.mkdir(parents=True, exist_ok=True)
log_path.mkdir(parents=True, exist_ok=True)
state_path = Path(__file__).parent / "state"
state_path.mkdir(parents=True, exist_ok=True)

# Имя файла прогноза на текущую дату
today = date.today()
current_filename = today.strftime("%Y-%m-%d") + ".txt"
current_filepath = predict_path / current_filename

# --- Настройка логгирования ---
# Имя файла лога с датой и временем запуска (один файл на запуск)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = log_path / f'trade_{ticker_lc}_tri_{timestamp}.txt'

# Настройка логгирования: файл + консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Очистка старых логов (оставляем только 3 самых новых)
def cleanup_old_logs(log_dir: Path, prefix: str, max_files: int = 3):
    """Удаляет старые лог-файлы, оставляя max_files самых новых."""
    log_files = sorted(log_dir.glob(f"{prefix}_*.txt"))
    if len(log_files) > max_files:
        for old_file in log_files[:-max_files]:
            try:
                old_file.unlink()
                logger.info(f"Удалён старый лог: {old_file.name}")
            except Exception as e:
                logger.warning(f"Не удалось удалить {old_file}: {e}")

cleanup_old_logs(log_path, prefix=f"trade_{ticker_lc}_tri")

# --- Вспомогательные функции ---
def get_direction(filepath):
    """
    Извлекает предсказание (up/down) из указанного файла.
    Проверяет несколько кодировок для корректного чтения.
    """
    encodings = ['utf-8', 'cp1251']
    for encoding in encodings:
        try:
            with filepath.open('r', encoding=encoding) as f:
                for line in f:
                    if "Предсказанное направление:" in line:
                        direction = line.split(":", 1)[1].strip().lower()
                        if direction in ['up', 'down']:
                            return direction
            return None
        except UnicodeDecodeError:
            continue
    logger.error(f"Не удалось прочитать файл {filepath} с кодировками {encodings}.")
    return None

def get_next_trans_id(trade_filepath):
    """
    Определяет следующий TRANS_ID на основе максимального значения в файле.
    """
    trans_id = 1
    if trade_filepath.exists():
        try:
            with trade_filepath.open('r', encoding='cp1251') as f:
                content = f.read()
                trans_ids = re.findall(r'TRANS_ID=(\d+);', content)
                if trans_ids:
                    trans_id = max(int(tid) for tid in trans_ids if tid.isdigit()) + 1
        except (UnicodeDecodeError, ValueError) as e:
            logger.error(f"Ошибка при чтении TRANS_ID из {trade_filepath}: {e}")
    return trans_id

# --- Основная логика ---
# Защита от повторной записи: один тикер + одна дата = один маркер
done_marker = state_path / f"{ticker_lc}_{today.strftime('%Y-%m-%d')}.done"
if done_marker.exists():
    logger.info(f"Маркер {done_marker.name} уже существует — транзакция за сегодня уже записана. Пропуск.\n")
    exit(0)

# Проверка наличия файла прогноза на сегодня
if not current_filepath.exists() or current_filepath.stat().st_size == 0:
    logger.info(f"Файл {current_filepath} не существует или пуст. Нет торгов.\n")
    exit(0)

# Сбор и сортировка всех .txt файлов по дате
files = []  # Список имен всех файлов предсказаний
for filepath in predict_path.glob("*.txt"):
    try:
        file_date = datetime.strptime(filepath.stem, "%Y-%m-%d").date()
        files.append((file_date, filepath.name))
    except ValueError:
        continue

files.sort(key=lambda x: x[0], reverse=True)  # Сортировка списка имен всех файлов с предсказаниями

# Поиск текущего и предыдущего файла
current_date = today  # Текущая дата
prev_filename = None  # Имя файла с предыдущим предсказанием
for i, (file_date, filename) in enumerate(files):
    if file_date == current_date:
        if i + 1 < len(files):
            prev_filename = files[i + 1][1]
        break

if prev_filename is None:
    logger.info("Предыдущий файл не найден.\n")
    exit(0)

prev_filepath = predict_path / prev_filename
logger.info(f"Предыдущий файл предсказаний: {prev_filepath}")
logger.info(f"Текущий файл предсказаний: {current_filepath}")

# Получение направлений из текущего и предыдущего файлов
current_predict = get_direction(current_filepath)
prev_predict = get_direction(prev_filepath)

if current_predict is None or prev_predict is None:
    logger.warning("Не удалось найти предсказанное направление в одном или обоих файлах.\n")
    exit(0)

# --- Формирование сигнала ---
trans_id = get_next_trans_id(trade_filepath)
expiry_date = today.strftime("%Y%m%d")
trade_direction = None
trade_content = None

def create_trade_block(tr_id, ticker, action, quantity):
    """Формирует блок транзакции в зависимости от направления и инструмента."""
    return (
        f'TRANS_ID={tr_id};'
        f'CLASSCODE=SPBFUT;'
        f'ACTION=Ввод заявки;'
        f'Торговый счет={trade_account};'
        f'К/П={action};'
        f'Тип=Рыночная;'
        f'Класс=SPBFUT;'
        f'Инструмент={ticker};'
        f'Цена=0;'
        f'Количество={quantity};'
        f'Условие исполнения=Поставить в очередь;'
        # f'Комментарий=SPBFUT16qg3//TRI;'
        f'Комментарий={tr_id} {today.strftime("%y%m%d")};'
        f'Переносить заявку=Нет;'
        f'Дата экспирации={expiry_date};'
        f'Код внешнего пользователя=;\n'
    )

# --- Логика выбора направления ---
# Проверка на совпадение инструментов (тикеры одинаковые)
if ticker_close == ticker_open:
    # Условия для переворота позиций
    if current_predict == 'up' and prev_predict == 'down':
        trade_direction = 'BUY'
        trade_content = (
            create_trade_block(trans_id, ticker_close, 'Покупка', quantity_close) +
            create_trade_block(trans_id+1, ticker_open, 'Покупка', quantity_open)
        )
    elif current_predict == 'down' and prev_predict == 'up':
        trade_direction = 'SELL'
        trade_content = (
            create_trade_block(trans_id, ticker_close, 'Продажа', quantity_close) +
            create_trade_block(trans_id+1, ticker_open, 'Продажа', quantity_open)
        )
# --- Условие ролловера (тикеры разные) ---
elif ticker_close != ticker_open:
    # Условия для переворота позиций во время ролловера
    if current_predict == 'up' and prev_predict == 'down':
        trade_direction = 'BUY'
        trade_content = (
                create_trade_block(trans_id, ticker_close, 'Покупка', quantity_close) +
                create_trade_block(trans_id+1, ticker_open, 'Покупка', quantity_open)
        )
    elif current_predict == 'down' and prev_predict == 'up':
        trade_direction = 'SELL'
        trade_content = (
                create_trade_block(trans_id, ticker_close, 'Продажа', quantity_close) +
                create_trade_block(trans_id+1, ticker_open, 'Продажа', quantity_open)
        )
    # Условия для переоткрытия позиций в том же направлении по новому тикеру на ролловере
    elif current_predict == 'up' and prev_predict == 'up':
        trade_direction = 'BUY'
        trade_content = (
                create_trade_block(trans_id, ticker_close, 'Продажа', quantity_close) +
                create_trade_block(trans_id+1, ticker_open, 'Покупка', quantity_open)
        )
    elif current_predict == 'down' and prev_predict == 'down':
        trade_direction = 'SELL'
        trade_content = (
                create_trade_block(trans_id, ticker_close, 'Покупка', quantity_close) +
                create_trade_block(trans_id+1, ticker_open, 'Продажа', quantity_open)
        )

# --- Запись результата ---
if trade_content:
    with trade_filepath.open('a', encoding='cp1251') as f:
        f.write(trade_content)
    done_marker.touch()
    logger.info(f'{prev_predict=}, {current_predict=}')
    logger.info(f"Добавлена транзакция {trade_direction} с TRANS_ID={trans_id} в файл {trade_filepath}.")
    logger.info(f"Добавлена транзакция {trade_direction} с TRANS_ID={trans_id+1} в файл {trade_filepath}.\n")
else:
    logger.info(
        f"На {today} условия для сигналов BUY или SELL не выполнены. "
        f"{prev_predict=}, {current_predict=}\n")
