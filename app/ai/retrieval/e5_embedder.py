from __future__ import annotations

from collections.abc import Sequence

from app.ai.local.embedding import DIM, embed_texts


class E5Embedder:
    """E5 embedding model."""

    dim = DIM

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed a list of texts."""
        if not texts:
            return []

        return embed_texts(list(texts)).tolist()
