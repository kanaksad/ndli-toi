"""Simple scraper for NDL Times of India collection.

Usage:
    python -m scraper.scrape_toi --start-url http://www.ndl.gov.in/nw_document/toi/timesofindia/thetoi \
        --output output.jsonl --max-pages 200

This script performs a breadth-first crawl limited to the ndl.gov.in domain and writes one JSON object per line.
"""
import argparse
import json
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper.utils import extract_titles_from_page


HEADERS = {"User-Agent": "ndli-toi-titles-scraper/1.0 (+https://github.com/)"}


def normalize_link(base: str, link: str) -> str:
    return urljoin(base, link)


def list_year_urls(start_url: str) -> list:
    """Return a list of candidate year URLs from the start page."""
    try:
        resp = requests.get(start_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch start url {start_url}: {e}")

    soup = BeautifulSoup(resp.text, "lxml")
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    years = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        full = normalize_link(base, href)
        # Heuristic: year pages in this collection often contain 'IN__thetoi_'
        if "/nw_document/toi/timesofindia/" in full and "IN__thetoi_" in full:
            if full not in seen:
                seen.add(full)
                years.append(full)

    return years


def list_linked_pages(url: str) -> list:
    """Return all same-domain links found on the given page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        tqdm.write(f"Failed to fetch {url}: {e}")
        return []

    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    soup = BeautifulSoup(resp.text, "lxml")
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        full = normalize_link(base, href)
        # Keep only links within same domain/collection path
        if parsed.netloc not in urlparse(full).netloc:
            continue
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
    return links


def list_month_urls(year_url: str) -> list:
    """Return candidate month URLs found on a year page.

    Heuristic: links under the same collection path that include the year (YYYY)
    in their path and are different from the provided year_url.
    """
    try:
        resp = requests.get(year_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch year url {year_url}: {e}")

    parsed = urlparse(year_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    soup = BeautifulSoup(resp.text, "lxml")

    year = None
    # Try to extract 4-digit year from the URL
    import re
    m = re.search(r"(19|20)\d{2}", year_url)
    if m:
        year = m.group(0)

    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        full = normalize_link(base, href)
        # same-domain only
        if parsed.netloc not in urlparse(full).netloc:
            continue
        if full == year_url:
            continue
        # If year extracted, prefer links that include the year in path
        if year and year not in full:
            # also allow links that look like month pages by path depth
            # but skip if they are top-level navigation
            continue
        if full in seen:
            continue
        seen.add(full)
        links.append(full)

    return links


def extract_titles_from_date_url(date_url: str) -> list:
    """Fetch a date page and extract candidate titles."""
    try:
        resp = requests.get(date_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        tqdm.write(f"Failed to fetch {date_url}: {e}")
        return []

    out = extract_titles_from_page(resp.text)
    return out.get("titles", [])


def run_hierarchical_scrape(start_url: str,
                            output_path: str = "output_titles.jsonl",
                            delay: float = 1.0,
                            max_years: int = None,
                            max_months: int = None,
                            max_dates: int = None,
                            max_titles_per_date: int = None):
    """Run hierarchical scraping: list years, then months, then dates, then extract titles.

    Writes JSON lines with {"year_url","month_url","date_url","title"}.
    Parameters allow limiting the breadth/depth for small-scale experiments.
    """
    years = list_year_urls(start_url)
    if max_years:
        years = years[:max_years]

    with open(output_path, "w", encoding="utf-8") as out_f:
        pbar = tqdm(total=len(years), desc="years")
        for y in years:
            # From a year page, list month-like links
            month_links = list_linked_pages(y)
            # Heuristic: months often include the year in path or be under the year page
            if max_months:
                month_links = month_links[:max_months]

            for m in month_links:
                # From month page, there will be date links
                date_links = list_linked_pages(m)
                if max_dates:
                    date_links = date_links[:max_dates]

                for d in date_links:
                    titles = extract_titles_from_date_url(d)
                    if max_titles_per_date:
                        titles = titles[:max_titles_per_date]
                    for t in titles:
                        record = {
                            "year_url": y,
                            "month_url": m,
                            "date_url": d,
                            "title": t,
                        }
                        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    time.sleep(delay)

            pbar.update(1)
            time.sleep(delay)
        pbar.close()


def main():
    parser = argparse.ArgumentParser(description="Hierarchical NDLI TOI title scraper")
    parser.add_argument("--start-url", required=True)
    parser.add_argument("--list-years", action="store_true", help="List candidate year URLs from the start page and exit")
    parser.add_argument("--list-months", action="store_true", help="List candidate month URLs from a year page and exit")
    parser.add_argument("--output", default="output_titles.jsonl")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--max-years", type=int)
    parser.add_argument("--max-months", type=int)
    parser.add_argument("--max-dates", type=int)
    parser.add_argument("--max-titles-per-date", type=int)
    args = parser.parse_args()

    if args.list_years:
        years = list_year_urls(args.start_url)
        for y in years:
            print(y)
        return
    if args.list_months:
        months = list_month_urls(args.start_url)
        for m in months:
            print(m)
        return

    run_hierarchical_scrape(
        args.start_url,
        output_path=args.output,
        delay=args.delay,
        max_years=args.max_years,
        max_months=args.max_months,
        max_dates=args.max_dates,
        max_titles_per_date=args.max_titles_per_date,
    )


if __name__ == "__main__":
    main()
