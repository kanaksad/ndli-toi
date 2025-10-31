NDLI Times of India scraper

What this is

- Minimal Python scraper to crawl the NDLI Times of India collection starting at a given URL and write one JSON object per line with the article title and text.

Quick start

1. Create a virtualenv and install requirements (macOS / zsh):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scraper/requirements.txt
```

2. Run the scraper:

```bash
python -m scraper.scrape_toi --start-url "http://www.ndl.gov.in/nw_document/toi/timesofindia/thetoi" --output ndli_toi.jsonl --max-pages 200
```

Notes and limitations

- This is a best-effort scraper. It uses generic heuristics to find article pages (presence of <article> or >=5 paragraphs).
- Be considerate: set `--delay` to at least 1.0s for polite crawling.
- The code does not check robots.txt. If you plan large-scale scraping, check and respect robots rules and the site's terms of service.

Next steps (suggested)

- Improve article extraction using readability or newspaper3k.
- Add concurrency with rate limiting.
- Persist HTML and extracted metadata in a database.
