"""Lossless 4096-character chunks, including long paragraphs and Unicode."""

import pytest

from wagent.outbound.chunking import chunk_text


@pytest.mark.parametrize(
    "text",
    ["", "short", "a" * 4096, "a" * 10000, "🙂café\n\n" * 1500, "a" * 3000 + "\n\n" + "b" * 7000],
)
def test_chunks_preserve_all_text(text: str) -> None:
    chunks = chunk_text(text)
    assert "".join(chunks) == text
    assert all(0 < len(chunk) <= 4096 for chunk in chunks)
    if text == "a" * 10000:
        assert [len(chunk) for chunk in chunks] == [4096, 4096, 1808]


def test_paragraph_boundary_preferred() -> None:
    assert chunk_text("abc\n\ndefghi", limit=8) == ["abc\n\n", "defghi"]
    with pytest.raises(ValueError):
        chunk_text("anything", limit=0)
