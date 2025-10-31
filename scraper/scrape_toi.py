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


def list_date_urls(month_url: str) -> list:
    """Return list of (day_label, url) tuples found on a month page.

    Heuristics used:
    - Anchor text that is a day number (1..31) is mapped to that day.
    - Anchor text that contains a day-month pattern (e.g., '1 Jan', '01-01-2017') is used.
    - If no useful anchor text, attempt to extract a date-like token from the URL path.
    The function returns an ordered list of (label, full_url).
    """
    try:
        resp = requests.get(month_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch month url {month_url}: {e}")

    parsed = urlparse(month_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    soup = BeautifulSoup(resp.text, "lxml")

    candidates = []
    seen = set()

    import re

    # Prefer the month-specific container in the stitching pane when available.
    # The page uses an id like `col_toi_timesofindia_<month_last_segment>` where
    # <month_last_segment> is the last path segment of the month_url. Restricting
    # to this container avoids picking up unrelated navigation links.
    last_segment = parsed.path.rstrip("/").split("/")[-1]
    container_id = f"col_toi_timesofindia_{last_segment}"
    main = soup.find(id=container_id)
    if main is None:
        # fallback to article/main or whole soup
        main = soup.find("article") or soup.find(id="main") or soup.find("main") or soup

    for a in main.find_all("a", href=True):
        text = a.get_text(separator=" ", strip=True)
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        full = normalize_link(base, href)
        if parsed.netloc not in urlparse(full).netloc:
            continue

        label = None
        # simple day number
        if re.fullmatch(r"\d{1,2}", text):
            label = text
        else:
            # look for patterns like '01 Jan', '1-Jan-2017', '2017-01-01', '1 Jan 2017'
            if re.search(r"\d{1,2}\s*[A-Za-z]{3,9}", text) or re.search(r"\d{4}-\d{2}-\d{2}", text) or re.search(r"\d{1,2}-[A-Za-z]{3,9}-\d{4}", text):
                label = text

        # if label still None, try to extract a date-like segment from URL
        if label is None:
            m = re.search(r"(19|20)\d{2}[-_/]?\d{1,2}[-_/]?\d{1,2}", full)
            if m:
                label = m.group(0)
            else:
                # try trailing numeric segment
                m2 = re.search(r"/(\d{1,2})$", full)
                if m2:
                    label = m2.group(1)

        if label is None:
            # fallback: use the anchor text (may be noisy)
            label = text if text else full

        if full in seen:
            continue
        seen.add(full)
        candidates.append((label, full))

    return candidates


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


def list_headline_urls(date_url: str) -> list:
    """Return list of (title, url) tuples for headlines on a date page.

    Heuristic: headline/article URLs appear under `/nw_document/toi/timesofindia/<numeric_id>`
    or as full URLs ending with a numeric ID. We filter anchors whose href path
    ends with a number and whose text looks like a headline.
    """
    try:
        resp = requests.get(date_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch date url {date_url}: {e}")

    parsed = urlparse(date_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    soup = BeautifulSoup(resp.text, "lxml")

    import re
    pattern = re.compile(r"/nw_document/toi/timesofindia/\d+$")

    results = []
    seen = set()

    # Search within the month/day stitching container first if available
    # fall back to whole document
    main = soup.find("article") or soup.find(id="main") or soup.find("main") or soup

    for a in main.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        full = normalize_link(base, href)
        # Only keep anchors whose path ends with a numeric id under the collection
        if not pattern.search(urlparse(full).path):
            continue
        text = a.get_text(separator=" ", strip=True)
        if not text or len(text) < 4:
            # skip very short nav text
            continue
        if full in seen:
            continue
        seen.add(full)
        results.append((text, full))

    return results


def extract_external_link(article_url: str) -> str | None:
    """Given an article page on NDLI, try to extract the external/original news URL.

    Heuristics (in order):
    - meta property="og:url" or meta name="twitter:url" if it points off-site
    - link[rel=canonical]
    - iframe[src] pointing to an external host
    - anchor hrefs that point to non-ndl domains (prefer ones containing known news domains)
    Returns the first candidate external URL or None if not found.
    """
    try:
        resp = requests.get(article_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    from urllib.parse import urlparse

    base = f"{urlparse(article_url).scheme}://{urlparse(article_url).netloc}"

    import re
    news_re = re.compile(r"timesofindia|indiatimes|articleshow|\.cms", re.IGNORECASE)
    # extract NDLI numeric id from article_url (e.g., /.../56881247)
    ndli_id = None
    m_id = re.search(r"/(\d+)$", urlparse(article_url).path)
    if m_id:
        ndli_id = m_id.group(1)

    def is_external(u: str) -> bool:
        try:
            p = urlparse(u)
            return p.netloc and "ndl.gov.in" not in p.netloc
        except Exception:
            return False

    def extract_js_urls(js: str) -> list:
        """Find URLs inside JS window.open or location.href patterns."""
        urls = []
        if not js:
            return urls
        # window.open('URL' ...), window.open("URL" ...)
        for m in re.finditer(r"window\.open\(['\"]([^'\"]+)['\"]", js):
            urls.append(m.group(1))
        # location.href = 'URL' or location.replace('URL')
        for m in re.finditer(r"location(?:\.href|\.replace)?\s*=\s*['\"]([^'\"]+)['\"]", js):
            urls.append(m.group(1))
        return urls

    # FIRST: if the article page embeds a viewer iframe (common on NDLI),
    # fetch the iframe content and look there for the Open Content button.
    # This avoids missing buttons that are injected into the viewer.
    for iframe in soup.find_all("iframe", src=True):
        src = (iframe.get("src") or "").strip()
        if not src:
            continue
        # prefer viewer.php or module-viewer endpoints
        if "viewer.php" in src or "module-viewer" in src:
            viewer_url = normalize_link(base, src)
            try:
                vresp = requests.get(viewer_url, headers=HEADERS, timeout=15)
                vresp.raise_for_status()
            except Exception:
                # if viewer fetch fails, continue to other heuristics
                vresp = None
            if vresp:
                v_soup = BeautifulSoup(vresp.text, "lxml")
                # 1) anchors with href
                for a in v_soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if not href:
                        continue
                    full = normalize_link(viewer_url, href)
                    # prefer external links
                    if not is_external(full):
                        continue
                    # prefer NDLI id in mapped URL, else news domain
                    if ndli_id and ndli_id in full:
                        return full
                    if news_re.search(full):
                        return full
                # 2) anchors with data-href or data-url attributes
                for a in v_soup.find_all(True):
                    for attr in ("data-href", "data-url", "data-link"):
                        val = a.get(attr)
                        if val:
                            full = normalize_link(viewer_url, val.strip())
                            if is_external(full):
                                if ndli_id and ndli_id in full:
                                    return full
                                if news_re.search(full):
                                    return full
                # 3) buttons or elements with onclick javascript that opens a URL
                for el in v_soup.find_all(True, onclick=True):
                    js = el.get("onclick")
                    for u in extract_js_urls(js):
                        full = normalize_link(viewer_url, u)
                        if is_external(full):
                            if ndli_id and ndli_id in full:
                                return full
                            if news_re.search(full):
                                return full
                # 4) meta tags inside viewer page
                for prop in ("og:url", "twitter:url"):
                    tag = v_soup.find("meta", property=prop) or v_soup.find("meta", attrs={"name": prop})
                    if tag and tag.get("content") and is_external(tag.get("content")):
                        return tag.get("content")
                linkc = v_soup.find("link", rel="canonical")
                if linkc and linkc.get("href") and is_external(linkc.get("href")):
                    return linkc.get("href")
                # 5) nested iframe inside viewer
                nested = v_soup.find("iframe", src=True)
                if nested:
                    nsrc = nested.get("src").strip()
                    if nsrc:
                        nfull = normalize_link(viewer_url, nsrc)
                        if is_external(nfull):
                            return nfull
                # otherwise continue to other heuristics

    for a in soup.find_all("a", href=True):
        classes = a.get("class", []) or []
        text = a.get_text(separator=" ", strip=True) or ""
        # match button classes and/or exact call-to-action text
        if ("btn" in classes and "btn-success" in classes) or ("open content" in text.lower()):
            href = a["href"].strip()
            if not href:
                continue
            full = normalize_link(base, href)
            if not is_external(full):
                continue
            # prefer links that include the NDLI id (mapping) or match news patterns
            if ndli_id and ndli_id in full:
                return full
            if news_re.search(full):
                return full

    # 1) meta og:url / twitter:url
    for prop in ("og:url", "twitter:url"):
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content") and is_external(tag.get("content")):
            return tag.get("content")

    # 2) canonical
    linkc = soup.find("link", rel="canonical")
    if linkc and linkc.get("href") and is_external(linkc.get("href")):
        return linkc.get("href")

    # 3) iframe[src]
    for iframe in soup.find_all("iframe", src=True):
        src = iframe.get("src").strip()
        if is_external(src):
            return src

    # 4) anchor hrefs -> collect external anchors but only accept news-like
    # domains or links that include the NDLI id. Do NOT return arbitrary
    # external links to unrelated sites.
    preferred_domains = ["timesofindia", "indiatimes", "articleshow", ".cms", "thehindu", "indianexpress", "hindustantimes"]
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("javascript:"):
            continue
        if not href.lower().startswith(("http://", "https://")):
            continue
        if is_external(href):
            candidates.append(href)

    # Prefer candidates that contain NDLI id
    if ndli_id:
        for c in candidates:
            if ndli_id in c:
                return c

    # Then prefer news-like domains
    for c in candidates:
        if news_re.search(c):
            return c
    # FINAL FALLBACK: try constructing a Times of India articleshow URL using the NDLI id.
    # Many TOI article pages live at /articleshow/<id>.cms and will redirect to the full slug URL.
    if ndli_id:
        try:
            candidate = f"https://timesofindia.indiatimes.com/articleshow/{ndli_id}.cms"
            # use GET with allow_redirects to discover final URL without downloading large body
            r = requests.get(candidate, headers=HEADERS, timeout=15, allow_redirects=True, stream=True)
            # close the stream without reading body
            try:
                r.close()
            except Exception:
                pass
            if r.status_code and r.status_code < 400 and is_external(r.url):
                return r.url
        except Exception:
            pass

    # otherwise do not return arbitrary external links
    return None


def run_hierarchical_scrape(start_url: str,
                            output_path: str = "output_titles.jsonl",
                            delay: float = 1.0,
                            max_years: int = None,
                            max_months: int = None,
                            max_dates: int = None,
                            max_titles_per_date: int = None,
                            resolve_externals: bool = False):
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
                    # If resolving externals, prefer to fetch headline entries (title,url)
                    # so we have the article URL to pass into the resolver. Otherwise use
                    # the lighter-weight title extractor.
                    if resolve_externals:
                        items = list_headline_urls(d)
                    else:
                        items = extract_titles_from_date_url(d)

                    if max_titles_per_date:
                        items = items[:max_titles_per_date]

                    for item in items:
                        if resolve_externals and isinstance(item, (list, tuple)) and len(item) >= 2:
                            title, article_url = item[0], item[1]
                            record = {
                                "year_url": y,
                                "month_url": m,
                                "date_url": d,
                                "title": title,
                                "article_url": article_url,
                            }
                            try:
                                record["external_url"] = extract_external_link(article_url)
                            except Exception:
                                record["external_url"] = None
                        else:
                            # item is a plain title string
                            title = item if isinstance(item, str) else str(item)
                            record = {
                                "year_url": y,
                                "month_url": m,
                                "date_url": d,
                                "title": title,
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
    parser.add_argument("--list-dates", action="store_true", help="List candidate date URLs from a month page and exit")
    parser.add_argument("--list-headlines", action="store_true", help="List headline title & url pairs from a date page and exit")
    parser.add_argument("--resolve-externals", action="store_true", help="When listing headlines, also resolve and print the external/original news URL if available")
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
    if args.list_dates:
        dates = list_date_urls(args.start_url)
        # print tab-separated: label \t url
        for label, url in dates:
            print(f"{label}\t{url}")
        return
    if args.list_headlines:
        hits = list_headline_urls(args.start_url)
        if args.resolve_externals:
            for title, url in hits:
                ext = extract_external_link(url)
                print(f"{title}\t{url}\t{ext if ext else ''}")
        else:
            for title, url in hits:
                print(f"{title}\t{url}")
        return

    run_hierarchical_scrape(
        args.start_url,
        output_path=args.output,
        delay=args.delay,
        max_years=args.max_years,
        max_months=args.max_months,
        max_dates=args.max_dates,
        max_titles_per_date=args.max_titles_per_date,
        resolve_externals=args.resolve_externals,
    )


if __name__ == "__main__":
    main()
