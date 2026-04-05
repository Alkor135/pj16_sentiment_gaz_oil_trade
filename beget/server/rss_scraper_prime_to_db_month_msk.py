"""
RSS скрапер новостей с сайта 1prime.ru
и записью в БД SQLite 3 по месяцам.
Дата/время берутся из pubDate и сохраняются в формате '%Y-%m-%d %H:%M:%S' (MSK).
Берутся все категории.
"""

import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import sqlite3
import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Настройка логирования
log_handler = TimedRotatingFileHandler(
    '/home/user/rss_scraper/log/rss_scraper_prime_to_db_month_msk.txt',
    when='midnight',
    interval=1,
    backupCount=3
)
log_handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    style='%'
))
logging.getLogger('').setLevel(logging.INFO)
logging.getLogger('').addHandler(log_handler)


async def fetch_rss(session: aiohttp.ClientSession, rss_link: str) -> list[dict]:
    """
    Получает и парсит RSS-ленту 1prime.ru, берёт все категории.
    """
    news_items = []
    try:
        async with session.get(rss_link) as response:
            xml_content = await response.text()
            root = ET.fromstring(xml_content)

            for item in root.findall('.//item'):
                title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
                pub_date_raw = item.find('pubDate').text if item.find('pubDate') is not None else None

                pub_date = None
                if pub_date_raw:
                    try:
                        dt_obj = datetime.strptime(pub_date_raw, "%a, %d %b %Y %H:%M:%S %z")
                        pub_date = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logging.error(f"Ошибка парсинга даты '{pub_date_raw}': {e}")

                if pub_date:
                    news_items.append({
                        "date": pub_date,
                        "title": title
                    })
    except Exception as e:
        logging.error(f"Ошибка при парсинге {rss_link}: {e}")
    return news_items


async def async_parsing_news(rss_link: str) -> pd.DataFrame:
    """
    Асинхронно парсит RSS и возвращает DataFrame.
    """
    timeout = aiohttp.ClientTimeout(total=40)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        news_list = await fetch_rss(session, rss_link)
    df = pd.DataFrame(news_list, columns=["date", "title"])
    return df


def parsing_news(rss_link: str) -> pd.DataFrame:
    """
    Обёртка для асинхронного парсинга.
    """
    return asyncio.run(async_parsing_news(rss_link))


def get_db_path(base_dir: str, date: datetime) -> str:
    """
    Путь к базе данных по текущему месяцу.
    """
    year_month = date.strftime("%Y_%m")
    return os.path.join(base_dir, f"rss_news_prime_{year_month}.db")


def load_existing_news(base_dir: str) -> set:
    """
    Загружает существующие новости (date, title) за текущий месяц в set для проверки дублей.
    """
    db_path = get_db_path(base_dir, datetime.now())
    if not os.path.exists(db_path):
        return set()

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT date, title FROM news")
            return set(cursor.fetchall())
    except Exception as e:
        logging.error(f"Ошибка при загрузке существующих новостей из {db_path}: {e}")
        return set()


def save_to_sqlite(df: pd.DataFrame, base_dir: str) -> None:
    """
    Сохраняет DataFrame в SQLite БД текущего месяца с предварительной проверкой дублей.
    """
    if df.empty:
        logging.error("DataFrame пустой, нечего сохранять в БД.")
        return

    # Предварительная проверка дублей
    existing_news = load_existing_news(base_dir)
    before_filter = len(df)
    df = df[~df.apply(lambda row: (row['date'], row['title']) in existing_news, axis=1)]

    if df.empty:
        logging.info(f"Все {before_filter} новостей уже есть в БД, ничего не добавлено.")
        return

    current_date = datetime.now()
    db_path = get_db_path(base_dir, current_date)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    date TEXT,
                    title TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_date_title ON news(date, title)")
            df[["date", "title"]].to_sql('news', conn, if_exists='append', index=False)
            logging.info(f"Новости сохранены в {db_path}. Сохранено строк: {len(df)}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении в БД {db_path}: {e}")


def remove_duplicates_from_db(base_dir: str) -> None:
    """
    Удаляет дубликаты по дате (без времени) и title.
    """
    current_date = datetime.now()
    db_path = get_db_path(base_dir, current_date)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM news")
            before_count = cursor.fetchone()[0]
            conn.execute("""
                DELETE FROM news
                WHERE rowid NOT IN (
                    SELECT rowid
                    FROM (
                        SELECT
                            rowid,
                            DATE(date) AS news_date,
                            title,
                            ROW_NUMBER() OVER (PARTITION BY DATE(date), title ORDER BY date ASC) AS rn
                        FROM news
                    ) AS subquery
                    WHERE rn = 1
                );
            """)
            cursor = conn.execute("SELECT COUNT(*) FROM news")
            after_count = cursor.fetchone()[0]
            deleted_count = before_count - after_count
            logging.info(f"Дубликаты удалены из {db_path}. Удалено: {deleted_count}")

            conn.isolation_level = None
            conn.execute("VACUUM")
            logging.info(f"VACUUM выполнен для {db_path}\n")
    except Exception as e:
        logging.error(f"Ошибка при удалении дубликатов из {db_path}: {e}")


def main():
    RSS_LINK = "https://1prime.ru/export/rss2/archive/index.xml"
    BASE_DIR = "/home/user/rss_scraper/db_rss_prime"

    logging.info(f"Запуск сбора данных: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    df = parsing_news(RSS_LINK)
    df = df.sort_values(by='date')
    save_to_sqlite(df, BASE_DIR)
    remove_duplicates_from_db(BASE_DIR)


if __name__ == '__main__':
    main()
