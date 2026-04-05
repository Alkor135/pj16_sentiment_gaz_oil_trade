#!/usr/bin/env python3
"""
Скрипт собирает все RSS-ссылки со страницы
https://ru.investing.com/webmaster-tools/rss
и сохраняет их в links.yaml

Запускать с IP, где страница открывается без 403.
"""

import requests
from bs4 import BeautifulSoup
import yaml
from pathlib import Path


URL = "https://ru.investing.com/webmaster-tools/rss"
OUTPUT_FILE = Path("links.yaml")


def collect_rss_links(url: str) -> list[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    rss_links = set()

    # Ищем ВСЕ ссылки, оканчивающиеся на .rss
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.endswith(".rss"):
            if href.startswith("/"):
                href = "https://ru.investing.com" + href
            rss_links.add(href)

    return sorted(rss_links)


def save_to_yaml(rss_links: list[str], output_path: Path):
    data = {"rss_links": rss_links}

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(
            data,
            f,
            allow_unicode=True,
            sort_keys=False
        )

    print(f"\nНайдено RSS-ссылок: {len(rss_links)}")
    print(f"Файл сохранён: {output_path.resolve()}")


def main():
    print("Сбор RSS ссылок...")
    links = collect_rss_links(URL)

    if not links:
        print("RSS ссылки не найдены.")
        return

    save_to_yaml(links, OUTPUT_FILE)


if __name__ == "__main__":
    main()