"""Lossless text splitting, preferring paragraph boundaries."""

TEXT_LIMIT = 4096


def chunk_text(text: str, limit: int = TEXT_LIMIT) -> list[str]:
    if limit < 1:
        raise ValueError("limit must be positive")
    chunks: list[str] = []
    while len(text) > limit:
        boundary = text.rfind("\n\n", 0, limit)
        end = boundary + 2 if boundary > 0 and boundary + 2 <= limit else limit
        chunks.append(text[:end])
        text = text[end:]
    if text:
        chunks.append(text)
    return chunks
