"""Interface base de Source Endpoints."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator

import polars as pl

from metro.core.endpoint import Endpoint
from metro.core.table import Table
from metro.queries.base import QueryRepository


class SourceEndpoint(Endpoint):
    """Contrato para fontes de dados do METRO.

    Responsável por conexão, autenticação, execução da consulta,
    paginação quando necessária, resolução de `query_path` ou query padrão,
    obtenção dos dados e conversão para Polars.

    Todo Source deve implementar `build_default_query()`.
    `query_path` é opcional — quando ausente, a query padrão é utilizada.
    """

    def __init__(
        self,
        runtime: str,
        query_path: str | None = None,
        chunk_size: int | None = None,
        table: Table | None = None,
        query_repository: QueryRepository | None = None,
    ) -> None:
        super().__init__(runtime)
        self._query_path = query_path
        self._chunk_size = chunk_size
        self._table = table
        self._query_repository = query_repository

    @property
    def query_path(self) -> str | None:
        """Referência externa da consulta no Query Repository."""
        return self._query_path

    @property
    def chunk_size(self) -> int | None:
        """Tamanho de chunk/paginação de leitura, quando parametrizado."""
        return self._chunk_size

    @property
    def table(self) -> Table | None:
        """Dataset lógico associado à extração, quando informado."""
        return self._table

    @property
    def query_repository(self) -> QueryRepository | None:
        """Repositório usado para resolver `query_path`, quando informado."""
        return self._query_repository

    def resolve_query(self, query_path: str | None = None) -> str:
        """Resolve a consulta a ser executada.

        Ordem:
        1. Se `query_path` (argumento ou do Endpoint) estiver definido → Query Repository
        2. Caso contrário → `build_default_query()` do Source
        """
        path = query_path if query_path is not None else self.query_path
        if path is not None:
            if self._query_repository is None:
                raise ValueError(
                    "query_repository é obrigatório quando query_path está informado"
                )
            return self._query_repository.resolve(path)

        return self.build_default_query()

    @abstractmethod
    def build_default_query(self) -> str:
        """Monta a consulta padrão do Source quando `query_path` não é informado.

        Todo Source deve implementar este método. A forma da consulta depende
        da tecnologia (SQL, aggregation pipeline NoSQL, etc.).
        """

    def require_table(self) -> Table:
        """Garante que `table` está definida para montagem da query padrão."""
        if self._table is None:
            raise ValueError(
                "table é obrigatória para montar a query padrão quando "
                "query_path não está informado"
            )
        return self._table

    @abstractmethod
    def read(self) -> pl.DataFrame:
        """Extrai os dados e retorna um Polars DataFrame."""

    def read_batches(self) -> Iterator[pl.DataFrame]:
        """Extrai os dados em batches quando `chunk_size` estiver definido.

        A implementação padrão delega para `read()`.
        Sources específicas podem sobrescrever para paginação nativa.
        """
        yield self.read()
