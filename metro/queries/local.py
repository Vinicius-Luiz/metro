"""Query Repository local baseado em arquivos no filesystem."""

from __future__ import annotations

import logging
from pathlib import Path

from metro.queries.base import QueryRepository
from metro.settings import settings

logger = logging.getLogger(__name__)


class QueryNotFoundError(FileNotFoundError):
    """Arquivo de query não encontrado no repositório local."""


class LocalQueryRepository(QueryRepository):
    """Resolve queries a partir de `.metro/queries/` (ou base_dir informado)."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        if base_dir is None:
            base_dir = settings.query_repository_base_dir
        self._base_dir = Path(base_dir)

    @property
    def base_dir(self) -> Path:
        """Diretório raiz onde as queries são resolvidas."""
        return self._base_dir

    def resolve(self, query_path: str) -> str:
        if not query_path or not query_path.strip():
            raise ValueError("query_path deve ser um caminho não vazio")

        candidate = self._base_dir / query_path.strip()
        if not candidate.is_file():
            raise QueryNotFoundError(
                f"Query não encontrada: {candidate}"
            )

        content = candidate.read_text(encoding="utf-8")
        logger.debug(
            "Query resolvida | query_path=%s | file=%s | size_bytes=%s",
            query_path,
            candidate.resolve(),
            candidate.stat().st_size,
        )
        return content
