"""
Единый RSS-скрапер для Interfax, 1Prime и Investing.
Все новости сохраняются в одну SQLite-базу с указанием провайдера и временем загрузки.
База данных создаётся по месяцам (формат: rss_news_YYYY_MM.db).
Логирование в файл и консоль.

ИЗМЕНЕНИЯ:
- RSS-ссылки берутся из settings.yaml
- Удалён HTML-парсинг Investing
- Добавлен контроль параллелизма (Semaphore)
- Добавлены HTTP-заголовки
- Улучшена устойчивость к сетевым ошибкам
"""

import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import sqlite3
import os
import logging
from logging.handlers import RotatingFileHandler
from pytz import timezone
from pathlib import Path
import yaml

# ==========================================================
#                     ЗАГРУЗКА SETTINGS
# ==========================================================

SETTINGS_FILE = Path(__file__).parent / "settings.yaml"

with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
    settings = yaml.safe_load(f)

BASE_DIR = settings['base_dir']
MAX_CONCURRENT = settings.get("max_concurrent_requests", 5)

RSS_INTERFAX = settings['rss'].get('interfax', [])
RSS_PRIME = settings['rss'].get('prime', [])
RSS_INVESTING = settings['rss'].get('investing', [])

# ==========================================================
#                     ЛОГИРОВАНИЕ
# ==========================================================

LOG_DIR = Path(Path(__file__).parent / 'log')
LOG_FILE = Path(LOG_DIR / "rss_scraper_all_providers_to_db_month_msk.log")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.Formatter.converter = lambda *args: datetime.now(timezone('Europe/Moscow')).timetuple()

log_formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

root_logger = logging.getLogger('')
root_logger.handlers.clear()
root_logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=1_048_576,
    backupCount=1,
    encoding='utf-8'
)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# ==========================================================
#                    HTTP ЗАГОЛОВКИ
# ==========================================================

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml;q=0.9,*/*;q=0.8",
}

# ==========================================================
#              ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================================

def get_db_path_by_date(base_dir, date_str):
    """
    Возвращает путь к БД по дате новости (формат YYYY-MM).
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    db_name = f"rss_news_{dt.year}_{dt.month:02d}.db"
    return Path(base_dir) / db_name


async def fetch_with_semaphore(semaphore, session, url, provider):
    """
    Обёртка для ограничения количества одновременных запросов.
    Защищает от перегрузки сервера и бана IP.
    """
    async with semaphore:
        return await fetch_rss(session, url, provider)


async def fetch_rss(session: aiohttp.ClientSession, rss_link: str, provider: str) -> list[dict]:
    """
    Универсальный парсер RSS.
    Логика обработки различается по провайдеру.
    """

    news_items = []

    try:
        async with session.get(rss_link) as response:

            if response.status != 200:
                logging.error(f"{provider}: HTTP {response.status} для {rss_link}")
                return news_items

            xml_content = await response.text()

            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e:
                logging.error(f"{provider}: ошибка XML {rss_link}: {e}")
                return news_items

            for item in root.findall('.//item'):

                title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
                pub_date_raw = item.find('pubDate').text if item.find('pubDate') is not None else None
                pub_date = None

                # --- Разная логика обработки дат ---
                try:
                    if provider == "interfax":
                        category = item.find('category').text if item.find('category') is not None else ""
                        if category.strip() != "Экономика":
                            continue
                        dt_obj = datetime.strptime(pub_date_raw, "%a, %d %b %Y %H:%M:%S %z")
                        pub_date = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

                    elif provider == "prime":
                        dt_obj = datetime.strptime(pub_date_raw, "%a, %d %b %Y %H:%M:%S %z")
                        pub_date = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

                    elif provider == "investing":
                        dt_obj = pd.to_datetime(pub_date_raw, utc=True, errors="coerce")
                        dt_obj = dt_obj.tz_convert(timezone('Europe/Moscow'))
                        pub_date = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

                except Exception as e:
                    logging.error(f"{provider}: ошибка парсинга даты {pub_date_raw}: {e}")

                if pub_date:
                    news_items.append({
                        "date": pub_date,
                        "title": title,
                        "provider": provider
                    })

    except Exception as e:
        logging.error(f"{provider}: ошибка запроса {rss_link}: {e}")

    return news_items


# ==========================================================
#             АСИНХРОННЫЙ СБОР ВСЕХ НОВОСТЕЙ
# ==========================================================

async def gather_all_news():

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    timeout = aiohttp.ClientTimeout(total=40)

    async with aiohttp.ClientSession(timeout=timeout, headers=HTTP_HEADERS) as session:

        tasks = []

        for link in RSS_INTERFAX:
            tasks.append(fetch_with_semaphore(semaphore, session, link, "interfax"))

        for link in RSS_PRIME:
            tasks.append(fetch_with_semaphore(semaphore, session, link, "prime"))

        for link in RSS_INVESTING:
            tasks.append(fetch_with_semaphore(semaphore, session, link, "investing"))

        results = await asyncio.gather(*tasks)

        all_news = []
        for sublist in results:
            all_news.extend(sublist)

        logging.info(f"Всего собрано новостей: {len(all_news)}")

        return all_news


# ==========================================================
#                   РАБОТА С БД
# ==========================================================

def create_db(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                loaded_at TEXT,
                date TEXT,
                title TEXT,
                provider TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_date_title_provider "
            "ON news(date, title, provider)"
        )


def save_to_sqlite(news_list: list[dict], base_dir: str):

    if not news_list:
        logging.info("Нет новостей для сохранения.")
        return

    from collections import defaultdict
    news_by_month = defaultdict(list)

    for item in news_list:
        db_path = get_db_path_by_date(base_dir, item["date"])
        news_by_month[db_path].append(item)

    for db_path, items in news_by_month.items():

        create_db(db_path)

        now_str = datetime.now(timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S")

        with sqlite3.connect(db_path) as conn:
            existing = set(conn.execute("SELECT date, title, provider FROM news"))

            filtered = [
                (now_str, item["date"], item["title"], item["provider"])
                for item in items
                if (item["date"], item["title"], item["provider"]) not in existing
            ]

            if not filtered:
                logging.info(f"{db_path.name}: новых новостей нет.")
                continue

            conn.executemany(
                "INSERT INTO news (loaded_at, date, title, provider) VALUES (?, ?, ?, ?)",
                filtered
            )
            conn.commit()

            logging.info(f"{db_path.name}: сохранено {len(filtered)} новостей.")


# ==========================================================
#                         MAIN
# ==========================================================

def main():
    try:
        logging.info(f"Запуск сбора данных: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        all_news = asyncio.run(gather_all_news())

        if all_news:
            all_news.sort(key=lambda x: x["date"])
            save_to_sqlite(all_news, BASE_DIR)
        else:
            logging.info("Нет новостей для обработки.")

    except Exception as e:
        logging.exception(f"Неожиданная ошибка в main(): {e}")


if __name__ == '__main__':
    main()