"""
Скрипт скачивает минутные данные из MOEX ISS API и сохраняет их в базу данных SQLite.
Если в базе данных уже есть данные, он проверяет их полноту и докачивает недостающие данные.
Если данных нет, он загружает все доступные данные, начиная с указанной даты.
Минутные данные за текущую сессию на MOEX ISS API доступны после 19:05 текущего дня,
после окончания основной сессии.
"""

from pathlib import Path
import sqlite3
from datetime import datetime, timedelta, date, time
import requests
import pandas as pd
import logging
import yaml

# Путь к settings.yaml в той же директории, что и скрипт
SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

# Чтение настроек
with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
    settings = yaml.safe_load(f)

# ==== Параметры ====
ticker = settings['ticker']
ticker_lc = ticker.lower()

# Начальная дата для загрузки минутных данных
start_date = datetime.strptime(settings['start_date_download_minutes'], "%Y-%m-%d").date()

# Путь к базе данных с минутными барами фьючерсов
path_db_minute = Path(settings['path_db_minute'].replace('{ticker}', ticker))

# --- Настройка логирования ---
# Папка для логов — та же, что у simulate_trade.py и других скриптов в rts/
log_dir = Path(__file__).parent / 'log'
log_dir.mkdir(parents=True, exist_ok=True)

# Имя файла лога с датой и временем запуска (один файл на запуск)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = log_dir / f'download_minutes_to_db_{timestamp}.txt'

# Настройка логирования: файл + консоль
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

cleanup_old_logs(log_dir, prefix="download_minutes_to_db")


def request_moex(session, url, retries = 5, timeout = 10):
    """Функция запроса данных с повторными попытками"""
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса {url} (попытка {attempt + 1}): {e}")
            if attempt == retries - 1:
                return None

def create_tables(connection: sqlite3.Connection) -> None:
    """ Функция создания таблиц в БД если их нет"""
    try:
        with connection:
            connection.execute('''CREATE TABLE if not exists Futures (
                            TRADEDATE         TEXT PRIMARY KEY UNIQUE NOT NULL,
                            SECID             TEXT NOT NULL,
                            OPEN              REAL NOT NULL,
                            LOW               REAL NOT NULL,
                            HIGH              REAL NOT NULL,
                            CLOSE             REAL NOT NULL,
                            VOLUME            INTEGER NOT NULL,
                            LSTTRADE          DATE NOT NULL)'''
                           )
        logger.info('Таблицы в БД созданы')
    except sqlite3.OperationalError as exception:
        logger.error(f"Ошибка при создании БД: {exception}")

def get_info_future(session, security):
    """Запрашивает у MOEX информацию по инструменту"""
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = request_moex(session, url)

    if not j:
        return pd.Series(["", "2130-01-01"])  # Гарантируем, что всегда 2 значения

    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in j['description']['data']]
    df = pd.DataFrame(data)

    shortname = df.loc[df['name'] == 'SHORTNAME', 'value'].values[0] \
        if 'SHORTNAME' in df['name'].values else ""
    lsttrade = df.loc[df['name'] == 'LSTTRADE', 'value'].values[0] \
        if 'LSTTRADE' in df['name'].values else df.loc[df['name'] == 'LSTDELDATE', 'value'].values[0] \
        if 'LSTDELDATE' in df['name'].values else "2130-01-01"

    return pd.Series([shortname, lsttrade])  # Гарантируем возврат 2 значений

def get_minute_candles(session, ticker: str, start_date: date, from_str: str = None, till_str: str = None) -> pd.DataFrame:
    """Получает все минутные данные по фьючерсу за указанную дату с учетом пагинации"""
    if from_str is None:
        from_str = datetime.combine(start_date, time(0, 0)).isoformat()
    if till_str is None:
        till_str = datetime.combine(start_date, time(23, 59, 59)).isoformat()

    all_data = []
    start = 0
    page_size = 500  # MOEX ISS API возвращает до 500 записей за запрос

    while True:
        url = (
            f'https://iss.moex.com/iss/engines/futures/markets/forts/securities/{ticker}/candles.json?'
            f'interval=1&from={from_str}&till={till_str}'
            f'&start={start}'
        )
        logger.info(f"Запрос минутных данных (start={start}): {url}")

        j = request_moex(session, url)
        if not j or 'candles' not in j or not j['candles'].get('data'):
            logger.error(f"Нет минутных данных для {ticker} на {start_date}")
            break

        data = [{k: r[i] for i, k in enumerate(j['candles']['columns'])} for r in j['candles']['data']]
        if not data:
            break

        all_data.extend(data)
        start += page_size

        if len(data) < page_size:
            break

    if not all_data:
        logger.error(f"Нет данных для {ticker} на {start_date}")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)

    df = df.rename(columns={
        'begin': 'TRADEDATE',
        'open': 'OPEN',
        'close': 'CLOSE',
        'high': 'HIGH',
        'low': 'LOW',
        'volume': 'VOLUME'
    })

    df['SECID'] = ticker

    df = df.dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE', 'VOLUME'])
    logger.info(df.to_string(max_rows=6, max_cols=18))

    return df[['TRADEDATE', 'SECID', 'OPEN', 'LOW', 'HIGH', 'CLOSE', 'VOLUME']].reset_index(drop=True)

def save_to_db(df: pd.DataFrame, connection: sqlite3.Connection) -> None:
    """Сохраняет DataFrame в таблицу Futures"""
    if df.empty:
        logger.error("DataFrame пуст, данные не сохранены")
        return

    try:
        with connection:
            df.to_sql('Futures', connection, if_exists='append', index=False)
        logger.info(f"Сохранено {len(df)} записей в таблицу Futures")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при сохранении данных в БД: {e}")

def get_future_date_results(
        session,
        start_date: date,
        ticker: str,
        connection: sqlite3.Connection,
        cursor: sqlite3.Cursor) -> None:
    """Получает данные по фьючерсам с MOEX ISS API и сохраняет их в базу данных."""
    today_date = datetime.now().date()  # Текущая дата
    while start_date <= today_date:
        date_str = start_date.strftime('%Y-%m-%d')
        # Проверяем количество записей в БД за дату
        cursor.execute("SELECT COUNT(*) FROM Futures WHERE DATE(TRADEDATE) = ?", (date_str,))
        count = cursor.fetchone()[0]

        if count == 0:
            # Нет минутных данных в БД, запрашиваем данные о торгуемых фьючерсах на дату
            # За текущую дату торгуемые тикеры доступны после 19:05, после окончания основной сессии
            request_url = (
                f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?'
                f'date={date_str}&assetcode={ticker}'
            )

            j = request_moex(session, request_url)
            if j is None:
                logger.error(f"Ошибка получения данных для {start_date}. Прерываем процесс, чтобы повторить попытку в следующий запуск.")
                break
            elif 'history' not in j or not j['history'].get('data'):
                # History API не вернул данных — возможно, торги ещё идут.
                # Fallback: берём SECID и LSTTRADE из последней записи в БД.
                cursor.execute("SELECT SECID, LSTTRADE FROM Futures ORDER BY TRADEDATE DESC LIMIT 1")
                last_row = cursor.fetchone()
                if last_row is None:
                    logger.info(f"Нет данных по торгуемым фьючерсам {ticker} за {start_date}, БД пуста — пропускаем")
                    start_date += timedelta(days=1)
                    continue

                last_secid = last_row[0]
                last_lsttrade = datetime.strptime(last_row[1], '%Y-%m-%d').date() if isinstance(last_row[1], str) else last_row[1]

                if last_lsttrade <= start_date:
                    # Контракт из БД истёк — ролловер.
                    # Пробуем взять ticker_close из settings.yaml как новый контракт.
                    ticker_close = settings.get('ticker_close')
                    if ticker_close and ticker_close != last_secid:
                        # Проверяем, что ticker_close — действующий контракт
                        _, tc_lsttrade_str = get_info_future(session, ticker_close)
                        try:
                            tc_lsttrade = datetime.strptime(tc_lsttrade_str, '%Y-%m-%d').date()
                        except (ValueError, TypeError):
                            tc_lsttrade = None

                        if tc_lsttrade and tc_lsttrade > start_date:
                            logger.info(f"Ролловер: {last_secid} истёк {last_lsttrade}, "
                                        f"используем ticker_close={ticker_close} (LSTTRADE={tc_lsttrade})")
                            current_ticker = ticker_close
                            lasttrade = tc_lsttrade

                            minute_df = get_minute_candles(session, current_ticker, start_date)
                            minute_df['LSTTRADE'] = lasttrade
                            if not minute_df.empty:
                                save_to_db(minute_df, connection)

                            start_date += timedelta(days=1)
                            continue

                    # ticker_close отсутствует, совпадает со старым или тоже истёк — ждём history API
                    if not ticker_close:
                        logger.info(f"Контракт {last_secid} истёк {last_lsttrade}, "
                                    f"ticker_close не задан в settings.yaml — ждём history API")
                    elif ticker_close == last_secid:
                        logger.warning(f"ticker_close={ticker_close} совпадает с истёкшим контрактом "
                                       f"{last_secid} — обновите settings.yaml")
                    else:
                        logger.info(f"ticker_close={ticker_close} тоже истёк — ждём history API")
                    start_date += timedelta(days=1)
                    continue

                # Контракт ещё активен, используем его для загрузки минутных данных
                logger.info(f"History API пуст за {start_date}, fallback на {last_secid} (LSTTRADE={last_lsttrade})")
                current_ticker = last_secid
                lasttrade = last_lsttrade

                minute_df = get_minute_candles(session, current_ticker, start_date)
                minute_df['LSTTRADE'] = lasttrade
                if not minute_df.empty:
                    save_to_db(minute_df, connection)

                start_date += timedelta(days=1)
                continue

            data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in j['history']['data']]
            df = pd.DataFrame(data).dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])
            if len(df) == 0:
                start_date += timedelta(days=1)
                continue

            df[['SHORTNAME', 'LSTTRADE']] = df.apply(
                lambda x: get_info_future(session, x['SECID']), axis=1, result_type='expand'
            )
            df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"], errors='coerce').dt.date.fillna('2130-01-01')
            df = df[df['LSTTRADE'] > start_date].dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])
            df = df[df['LSTTRADE'] == df['LSTTRADE'].min()].reset_index(drop=True)
            df = df.drop(columns=[
                'OPENPOSITIONVALUE', 'VALUE', 'SETTLEPRICE', 'SWAPRATE', 'WAPRICE',
                'SETTLEPRICEDAY', 'NUMTRADES', 'SHORTNAME', 'CHANGE', 'QTY'
            ], errors='ignore')

            current_ticker = df.loc[0, 'SECID']
            lasttrade = df.loc[0, 'LSTTRADE']

            # Получаем минутные данные
            minute_df = get_minute_candles(session, current_ticker, start_date)
            minute_df['LSTTRADE'] = lasttrade
            if not minute_df.empty:
                save_to_db(minute_df, connection)

        else:
            # Есть минутные данные за дату, проверяем полноту
            cursor.execute("SELECT MAX(TRADEDATE) FROM Futures WHERE DATE(TRADEDATE) = ?", (date_str,))
            max_time_str = cursor.fetchone()[0]
            max_dt = datetime.strptime(max_time_str, '%Y-%m-%d %H:%M:%S')

            threshold_time = time(23, 49, 0)
            is_today = start_date == today_date

            if not is_today and max_dt.time() >= threshold_time:
                logger.info(f"Минутные данные за {start_date} полные, пропускаем дату {start_date}")
                start_date += timedelta(days=1)
                continue

            # Неполные минутные данные или сегодняшний день (после 19:05), докачиваем
            cursor.execute("SELECT SECID, LSTTRADE FROM Futures WHERE DATE(TRADEDATE) = ? LIMIT 1", (date_str,))
            row = cursor.fetchone()
            current_ticker = row[0]
            lasttrade = datetime.strptime(row[1], '%Y-%m-%d').date() if isinstance(row[1], str) else row[1]

            from_dt = max_dt + timedelta(minutes=1)
            from_str = from_dt.isoformat()

            if is_today:
                till_dt = datetime.now()
            else:
                till_dt = datetime.combine(start_date, time(23, 59, 59))
            till_str = till_dt.isoformat()

            minute_df = get_minute_candles(session, current_ticker, start_date, from_str, till_str)
            minute_df['LSTTRADE'] = lasttrade
            if not minute_df.empty:
                save_to_db(minute_df, connection)

        start_date += timedelta(days=1)

def main(
        ticker: str = ticker,
        path_db: Path = path_db_minute,
        start_date: date = start_date) -> None:
    """
    Основная функция: подключается к базе данных, создает таблицы и загружает данные по фьючерсам.
    """
    try:
        # Создание директории под БД, если не существует
        path_db.parent.mkdir(parents=True, exist_ok=True)

        # Подключение к базе данных
        connection = sqlite3.connect(str(path_db), check_same_thread=True)
        cursor = connection.cursor()

        # Проверяем наличие таблицы Futures
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Futures'")
        exist_table = cursor.fetchone()
        # Если таблица Futures не существует, создаем её
        if exist_table is None:
            create_tables(connection)

        # Проверяем, есть ли записи в таблице Futures
        cursor.execute("SELECT EXISTS (SELECT 1 FROM Futures) as has_rows")
        exists_rows = cursor.fetchone()[0]
        # Если таблица Futures не пустая
        if exists_rows:
            # Находим максимальную дату
            cursor.execute("SELECT MAX(DATE(TRADEDATE)) FROM Futures")
            max_trade_date = cursor.fetchone()[0]
            if max_trade_date:
                # Устанавливаем start_date на максимальную дату для проверки полноты
                start_date = datetime.strptime(max_trade_date, "%Y-%m-%d").date()
                logger.info(f"Начальная дата для загрузки минутных данных: {start_date}")

        with requests.Session() as session:
            get_future_date_results(session, start_date, ticker, connection, cursor)

    except Exception as e:
        logger.error(f"Ошибка в main: {e}")

    finally:
        # VACUUM и закрытие соединения (с проверкой, что объекты были созданы)
        if 'cursor' in locals():
            cursor.execute("VACUUM")
            logger.info("VACUUM выполнен: база данных оптимизирована")
            cursor.close()
        if 'connection' in locals():
            connection.close()
            logger.info(f"Соединение с минутной БД {path_db} по фьючерсам {ticker} закрыто.")


if __name__ == '__main__':
    main(ticker, path_db_minute, start_date)