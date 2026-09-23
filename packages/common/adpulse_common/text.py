"""Shared query normalization used by indexer tests and serving tokenization docs."""

import re
import unicodedata

STOPWORDS = {
    "a", "an", "the", "for", "and", "or", "of", "to", "in", "on", "with",
}

TOKEN_RE = re.compile(r"[a-z0-9]+")


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    return text.lower().strip()


def tokenize(text: str) -> list[str]:
    tokens = TOKEN_RE.findall(normalize(text))
    return [t for t in tokens if t not in STOPWORDS and len(t) > 1]
