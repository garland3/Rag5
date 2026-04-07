from app.core.chunking import chunk_text


def test_chunk_text_splits_long_text():
    text = "Hello world. " * 200  # ~2600 chars
    chunks = chunk_text(text)
    assert len(chunks) > 1
    for chunk in chunks:
        assert len(chunk) <= 1100  # chunk_size + some tolerance


def test_chunk_text_short_text_single_chunk():
    text = "Short text."
    chunks = chunk_text(text)
    assert len(chunks) == 1
    assert chunks[0] == "Short text."


def test_chunk_text_empty_string():
    chunks = chunk_text("")
    assert chunks == []
