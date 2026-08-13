"""Interface base de Query Repositories."""

from __future__ import annotations

from abc import ABC, abstractmethod


class QueryRepository(ABC):
    """Resolve `query_path` para o conteúdo da consulta."""

    @abstractmethod
    def resolve(self, query_path: str) -> str:
        """Retorna o conteúdo da query referenciada por `query_path`."""
