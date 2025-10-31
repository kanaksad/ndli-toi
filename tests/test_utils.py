from scraper.utils import extract_text_and_title


def test_extract_text_and_title_basic():
    html = """
    <html>
      <head><title>Sample Article</title></head>
      <body>
        <article>
          <p>First paragraph.</p>
          <p>Second paragraph with <strong>bold</strong> text.</p>
        </article>
      </body>
    </html>
    """

    res = extract_text_and_title(html)
    assert res["title"] == "Sample Article"
    assert "First paragraph." in res["text"]
    assert "Second paragraph" in res["text"]


def test_extract_titles_from_page():
    html = """
    <html>
      <body>
        <main>
          <ul>
            <li><a href="/doc/1">Title One</a></li>
            <li><a href="/doc/2">Title Two</a></li>
          </ul>
        </main>
      </body>
    </html>
    """
    from scraper.utils import extract_titles_from_page

    res = extract_titles_from_page(html)
    assert isinstance(res, dict)
    assert "Title One" in res["titles"]
    assert "Title Two" in res["titles"]
