"""Simple scraper for NDL Times of India collection.

Usage:
    python -m scraper.scrape_toi --start-url http://www.ndl.gov.in/nw_document/toi/timesofindia/thetoi \
        --output output.jsonl --max-pages 200

This script performs a breadth-first crawl limited to the ndl.gov.in domain and writes one JSON object per line.
"""
import argparse
import json
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper.utils import extract_text_and_title


HEADERS = {"User-Agent": "ndli-toi-scraper/1.0 (+https://github.com/)"}


def same_domain(base_netloc: str, url: str) -> bool:
    try:
        return urlparse(url).netloc.endswith(base_netloc)
    except Exception:
        return False


def normalize_link(base: str, link: str) -> str:
    return urljoin(base, link)


def is_probable_article(soup: BeautifulSoup) -> bool:
    # Heuristic: contains <article> or multiple <p> tags
    if soup.find("article"):
        return True
    pcount = len(soup.find_all("p"))
    return pcount >= 5


def crawl(start_url: str, max_pages: int = 200, delay: float = 1.0, output_path: str = "output.jsonl"):
    parsed = urlparse(start_url)
    base_netloc = parsed.netloc

    q = deque([start_url])
    seen = set([start_url])

    with open(output_path, "w", encoding="utf-8") as out_f:
        pbar = tqdm(total=max_pages, desc="pages")
        pages = 0
        while q and pages < max_pages:
            url = q.popleft()
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                html = resp.text
            except Exception as e:
                tqdm.write(f"Failed to fetch {url}: {e}")
                time.sleep(delay)
                continue

            soup = BeautifulSoup(html, "lxml")

            # Save if probable article
            if is_probable_article(soup):
                data = extract_text_and_title(html)
                record = {
                    "url": url,
                    "title": data.get("title"),
                    "text": data.get("text"),
                }
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            # Enqueue links from this page (domain-limited)
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("javascript:"):
                    continue
                full = normalize_link(url, href)
                if full in seen:
                    continue
                if not same_domain(base_netloc, full):
                    continue
                seen.add(full)
                q.append(full)

            pages += 1
            pbar.update(1)
            time.sleep(delay)
        pbar.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-url", required=True)
    parser.add_argument("--output", default="output.jsonl")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()

    crawl(args.start_url, max_pages=args.max_pages, delay=args.delay, output_path=args.output)


if __name__ == "__main__":
    main()
