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
