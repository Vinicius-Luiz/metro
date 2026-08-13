"""Source Endpoint PostgreSQL."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import polars as pl
import psycopg
from psycopg.rows import dict_row

from metro.core.table import Table
from metro.queries.base import QueryRepository
from metro.secrets.base import SecretProvider
from metro.sources.base import SourceEndpoint

logger = logging.getLogger(__name__)


class PostgreSQLSource(SourceEndpoint):
    """Extrai dados de PostgreSQL e entrega Polars DataFrame."""

    def __init__(
        self,
        runtime: str,
        secret_provider: SecretProvider,
        query_repository: QueryRepository | None = None,
        query_path: str | None = None,
        chunk_size: int | None = None,
        table: Table | None = None,
    ) -> None:
        super().__init__(
            runtime=runtime,
            query_path=query_path,
            chunk_size=chunk_size,
            table=table,
            query_repository=query_repository,
        )
        self._secret_provider = secret_provider
        self._connection: psycopg.Connection[Any] | None = None

    def connect(self) -> None:
        secret = self._secret_provider.get_secret(self.runtime)
        if not isinstance(secret, str):
            raise TypeError(
                f"PostgreSQL espera connection string para runtime '{self.runtime}', "
                f"obtido: {type(secret).__name__}"
            )

        logger.info("Conectando ao PostgreSQL (runtime=%s)", self.runtime)
        logger.debug(
            "Parâmetros Source PostgreSQL | runtime=%s | query_path=%s | "
            "chunk_size=%s | table=%s",
            self.runtime,
            self.query_path,
            self.chunk_size,
            None if self.table is None else self.table.qualified_name,
        )
        self._connection = psycopg.connect(conninfo=secret)
        logger.debug(
            "Conexão PostgreSQL estabelecida | server=%s | autocommit=%s",
            self._connection.info.host,
            self._connection.autocommit,
        )
        if self.table is not None and self.table.schema_name:
            self._log_table_metadata(self.table)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Conexão PostgreSQL encerrada (runtime=%s)", self.runtime)

    def build_default_query(self) -> str:
        """Monta SELECT com todas as colunas da table via information_schema."""
        table = self.require_table()
        if not table.schema_name:
            raise ValueError(
                "schema_name é obrigatório para montar a query padrão no PostgreSQL"
            )

        connection = self._require_connection()
        columns = self._list_column_metadata(connection, table)
        if not columns:
            raise ValueError(
                f"Nenhuma coluna encontrada para {table.qualified_name}"
            )

        column_names = [column["name"] for column in columns]
        quoted_columns = ", ".join(_quote_ident(name) for name in column_names)
        schema = _quote_ident(table.schema_name)
        name = _quote_ident(table.name)
        query = f"SELECT {quoted_columns} FROM {schema}.{name}"
        logger.info(
            "Query padrão montada para %s (%s colunas)",
            table.qualified_name,
            len(column_names),
        )
        logger.debug("Colunas da query padrão: %s", columns)
        return query

    def read(self) -> pl.DataFrame:
        connection = self._require_connection()
        query = self.resolve_query()
        logger.info("Executando query no PostgreSQL (runtime=%s)", self.runtime)
        logger.debug("Query path: %s", self.query_path)
        logger.debug("SQL: %s", query)

        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            description = cursor.description

        dataframe = pl.DataFrame(rows) if rows else pl.DataFrame()
        if description:
            result_columns = [col.name for col in description]
            logger.debug(
                "Resultado da query | rows=%s | columns=%s | column_names=%s",
                dataframe.height,
                dataframe.width,
                result_columns,
            )
        else:
            logger.debug("Resultado da query | rows=%s | columns=%s", dataframe.height, dataframe.width)
        return dataframe

    def read_batches(self) -> Iterator[pl.DataFrame]:
        if self.chunk_size is None:
            yield self.read()
            return

        connection = self._require_connection()
        query = self.resolve_query()
        logger.info(
            "Lendo PostgreSQL em batches (runtime=%s, chunk_size=%s)",
            self.runtime,
            self.chunk_size,
        )
        logger.debug("SQL (batches): %s", query)

        with connection.cursor(
            name="metro_postgresql_cursor",
            row_factory=dict_row,
        ) as cursor:
            cursor.itersize = self.chunk_size
            cursor.execute(query)

            batch_index = 0
            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                batch = pl.DataFrame(rows)
                batch_index += 1
                logger.debug(
                    "Batch %s lido: rows=%s | columns=%s",
                    batch_index,
                    batch.height,
                    list(batch.columns),
                )
                yield batch

    def _log_table_metadata(self, table: Table) -> None:
        connection = self._require_connection()
        columns = self._list_column_metadata(connection, table)
        logger.debug(
            "Metadados da tabela | qualified_name=%s | schema=%s | name=%s | "
            "column_count=%s",
            table.qualified_name,
            table.schema_name,
            table.name,
            len(columns),
        )
        logger.debug("Metadados das colunas: %s", columns)

    def _list_column_metadata(
        self,
        connection: psycopg.Connection[Any],
        table: Table,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, (table.schema_name, table.name))
            return [
                {
                    "name": row[0],
                    "data_type": row[1],
                    "udt_name": row[2],
                    "nullable": row[3] == "YES",
                    "max_length": row[4],
                    "numeric_precision": row[5],
                    "numeric_scale": row[6],
                    "ordinal_position": row[7],
                }
                for row in cursor.fetchall()
            ]

    def _require_connection(self) -> psycopg.Connection[Any]:
        if self._connection is None:
            raise RuntimeError(
                "PostgreSQLSource não está conectado. Chame connect() antes de read()."
            )
        return self._connection


def _quote_ident(identifier: str) -> str:
    """Escapa identificador SQL com aspas duplas."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
