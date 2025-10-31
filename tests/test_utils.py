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
