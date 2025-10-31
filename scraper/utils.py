from bs4 import BeautifulSoup
from typing import Optional, Dict


def extract_text_and_title(html: str) -> Dict[str, Optional[str]]:
    """Extracts a best-effort title and text from HTML string.

    Returns:
      {"title": str or None, "text": str or None, "html": original html}
    """
    soup = BeautifulSoup(html, "lxml")

    # Title
    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Prefer <article>
    article = soup.find("article")
    if article:
        paragraphs = article.find_all("p")
    else:
        # fallback: main or content containers
        main = soup.find(id="main") or soup.find("main")
        if main:
            paragraphs = main.find_all("p")
        else:
            # very generic fallback - all <p>
            paragraphs = soup.find_all("p")

    texts = []
    for p in paragraphs:
        text = p.get_text(separator=" ", strip=True)
        if text:
            texts.append(text)

    text = "\n\n".join(texts) if texts else None

    return {"title": title, "text": text, "html": html}


def extract_titles_from_page(html: str) -> Dict[str, list]:
    """Extracts candidate article titles from a page that lists titles (dates page).

    Returns a dict with a list of titles under key 'titles'. Titles are
    best-effort text contents of anchors or list items filtered by length.
    """
    soup = BeautifulSoup(html, "lxml")

    candidates = []

    # Prefer anchors inside main content/article
    main = soup.find("article") or soup.find(id="main") or soup.find("main")
    search_root = main if main is not None else soup

    # Gather anchor texts
    for a in search_root.find_all("a", href=True):
        text = a.get_text(separator=" ", strip=True)
        if not text:
            continue
        # filter out short nav labels
        if len(text) < 4:
            continue
        # ignore month/day nav links that are just years or months (e.g., '2024')
        if text.isdigit() and len(text) == 4:
            continue
        candidates.append(text)

    # Also consider list items
    for li in search_root.find_all("li"):
        text = li.get_text(separator=" ", strip=True)
        if not text:
            continue
        if len(text) < 4:
            continue
        candidates.append(text)

    # Deduplicate while preserving order
    seen = set()
    titles = []
    for t in candidates:
        if t in seen:
            continue
        seen.add(t)
        titles.append(t)

    return {"titles": titles}
