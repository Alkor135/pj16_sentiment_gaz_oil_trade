"""
RSS скрапер новостей Investing.com с записью в SQLite по месяцам.

Изменения:
- RSS ссылки берутся из settings.yaml
- Удалён парсинг HTML страницы webmaster-tools/rss
- Добавлены HTTP-заголовки (User-Agent) для обхода 403
- Добавлен retry при загрузке RSS
- Добавлены дополнительные проверки и логирование

Время публикации новостей конвертируется в московское время (MSK).
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
from pytz import timezone
import yaml
from pathlib import Path

# ==========================================================
#                 НАСТРОЙКА ЛОГИРОВАНИЯ
# ==========================================================

log_handler = TimedRotatingFileHandler(
    '/home/user/rss_scraper/log/rss_scraper_investing_to_db_month_msk.log',
    when='midnight',
    interval=1,
    backupCount=3
)

log_handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    style='%'
))

log_handler.converter = lambda *args: datetime.now(
    timezone('Europe/Moscow')
).timetuple()

# Сброс старых обработчиков
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.Formatter.converter = lambda *args: datetime.now(
    timezone('Europe/Moscow')
).timetuple()

logging.getLogger('').setLevel(logging.INFO)
logging.getLogger('').addHandler(log_handler)

# ==========================================================
#                 ЗАГРУЗКА SETTINGS.YAML
# ==========================================================

SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

if not SETTINGS_FILE.exists():
    logging.error("Файл settings.yaml не найден.")
    raise FileNotFoundError("settings.yaml не найден")

with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
    settings = yaml.safe_load(f)

rss_links = settings.get("rss_links", [])

if not rss_links:
    logging.error("В settings.yaml отсутствует список rss_links.")
    raise ValueError("rss_links не найден в settings.yaml")

# ==========================================================
#                 ЗАГОЛОВКИ ДЛЯ HTTP
# ==========================================================

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# ==========================================================
#                 АСИНХРОННАЯ ЗАГРУЗКА RSS
# ==========================================================

async def fetch_rss(session: aiohttp.ClientSession,
                    rss_link: str,
                    retries: int = 3) -> list[dict]:
    """
    Асинхронно получает и парсит одну RSS-ленту.
    Добавлен retry при ошибках сети или 403.
    """

    news_items = []

    for attempt in range(retries):
        try:
            async with session.get(rss_link) as response:

                if response.status != 200:
                    raise Exception(f"HTTP статус {response.status}")

                xml_content = await response.text()

                root = ET.fromstring(xml_content)

                channel = root.find('.//channel')
                channel_name = (
                    channel.find('title').text
                    if channel is not None and channel.find('title') is not None
                    else ""
                )

                logging.info(f'Обработка канала: {channel_name}')

                for item in root.findall('.//item'):

                    title = (
                        item.find('title').text
                        if item.find('title') is not None
                        else "Нет заголовка"
                    )

                    pub_date = (
                        item.find('pubDate').text
                        if item.find('pubDate') is not None
                        else None
                    )

                    link = (
                        item.find('link').text
                        if item.find('link') is not None
                        else ""
                    )

                    news_items.append({
                        "date": pub_date,
                        "section": channel_name,
                        "title": title,
                        "link": link
                    })

                return news_items

        except Exception as e:
            logging.warning(
                f"Попытка {attempt+1}/{retries} не удалась для {rss_link}: {e}"
            )
            await asyncio.sleep(2)

    logging.error(f"Не удалось загрузить {rss_link} после {retries} попыток.")
    return news_items


async def async_parsing_news(rss_links: list[str]) -> pd.DataFrame:
    """
    Асинхронно парсит все RSS-ленты.
    """

    timeout = aiohttp.ClientTimeout(total=40)

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=HTTP_HEADERS
    ) as session:

        tasks = [fetch_rss(session, link) for link in rss_links]
        results = await asyncio.gather(*tasks)

    all_news = [item for sublist in results for item in sublist]

    df = pd.DataFrame(
        all_news,
        columns=["date", "section", "title", "link"]
    )

    if df.empty:
        logging.warning("После парсинга DataFrame пустой.")
        return df

    # Парсим дату как UTC
    df["date"] = pd.to_datetime(
        df["date"],
        utc=True,
        errors="coerce"
    )

    # Конвертируем в московское время
    df["date"] = df["date"].dt.tz_convert(
        timezone('Europe/Moscow')
    )

    return df


def parsing_news(rss_links: list[str]) -> pd.DataFrame:
    """
    Обёртка для вызова async из синхронного кода.
    """
    return asyncio.run(async_parsing_news(rss_links))

# ==========================================================
#                 РАБОТА С SQLITE
# ==========================================================

def get_db_path(base_dir: str, date: datetime) -> str:
    year_month = date.strftime("%Y_%m")
    return os.path.join(base_dir,
                        f"rss_news_investing_{year_month}.db")


def save_to_sqlite(df: pd.DataFrame, base_dir: str) -> None:

    if df.empty:
        logging.error("DataFrame пустой, нечего сохранять.")
        return

    current_date = datetime.now()
    db_path = get_db_path(base_dir, current_date)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Убираем timezone перед записью
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    date TEXT,
                    title TEXT
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_news_date_title
                ON news(date, title)
            """)

            df[["date", "title"]].to_sql(
                'news',
                conn,
                if_exists='append',
                index=False
            )

            logging.info(
                f"Сохранено строк: {len(df)} в {db_path}"
            )

        except Exception as e:
            logging.error(f"Ошибка записи в БД: {e}")


def remove_duplicates_from_db(base_dir: str) -> None:

    current_date = datetime.now()
    db_path = get_db_path(base_dir, current_date)

    try:
        with sqlite3.connect(db_path) as conn:

            before_count = conn.execute(
                "SELECT COUNT(*) FROM news"
            ).fetchone()[0]

            conn.execute("""
                DELETE FROM news
                WHERE rowid NOT IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               DATE(date) AS news_date,
                               title,
                               ROW_NUMBER()
                               OVER (
                                   PARTITION BY DATE(date), title
                                   ORDER BY date ASC
                               ) AS rn
                        FROM news
                    )
                    WHERE rn = 1
                );
            """)

            after_count = conn.execute(
                "SELECT COUNT(*) FROM news"
            ).fetchone()[0]

            logging.info(
                f"Удалено дубликатов: {before_count - after_count}"
            )

            conn.isolation_level = None
            conn.execute("VACUUM")

    except Exception as e:
        logging.error(f"Ошибка удаления дубликатов: {e}")

# ==========================================================
#                         MAIN
# ==========================================================

def main(base_dir: str) -> None:

    logging.info(
        f"Запуск сбора данных: "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    logging.info(f"Количество RSS-лент: {len(rss_links)}")

    df = parsing_news(rss_links)

    if df.empty:
        logging.warning("Нет новых данных.")
        return

    df = df.sort_values(by='date')

    save_to_sqlite(df, base_dir)
    remove_duplicates_from_db(base_dir)


if __name__ == '__main__':

    BASE_DIR = "/home/user/rss_scraper/db_rss_investing"

    main(BASE_DIR)