"""Source Endpoint PostgreSQL."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import polars as pl
import psycopg

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
        self._lower_bound: tuple[str, Any] | None = None

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

    def apply_lower_bound(self, reference_column: str, min_value: Any) -> None:
        if not reference_column or not reference_column.strip():
            raise ValueError("reference_column deve ser um identificador não vazio")
        self._lower_bound = (reference_column.strip(), min_value)
        logger.info(
            "Lower bound aplicado (runtime=%s, column=%s, min_value=%s)",
            self.runtime,
            self._lower_bound[0],
            min_value,
        )

    def read(self) -> pl.DataFrame:
        connection = self._require_connection()
        query, params = self._prepare_query()
        logger.info("Executando query no PostgreSQL (runtime=%s)", self.runtime)
        logger.debug("Query path: %s", self.query_path)
        logger.debug("SQL: %s | params=%s", query, params)

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            schema = _polars_schema_from_description(cursor.description)

        dataframe = _dataframe_from_rows(rows, schema)
        logger.debug(
            "Resultado da query | rows=%s | columns=%s | column_names=%s",
            dataframe.height,
            dataframe.width,
            list(dataframe.columns),
        )
        return dataframe

    def read_batches(self) -> Iterator[pl.DataFrame]:
        if self.chunk_size is None:
            yield self.read()
            return

        connection = self._require_connection()
        query, params = self._prepare_query()
        logger.info(
            "Lendo PostgreSQL em batches (runtime=%s, chunk_size=%s)",
            self.runtime,
            self.chunk_size,
        )
        logger.debug("SQL (batches): %s | params=%s", query, params)

        with connection.cursor(name="metro_postgresql_cursor") as cursor:
            cursor.itersize = self.chunk_size
            cursor.execute(query, params)

            batch_index = 0
            schema: dict[str, pl.DataType] | None = None
            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if schema is None:
                    schema = _polars_schema_from_description(cursor.description)
                if not rows:
                    break
                batch = _dataframe_from_rows(rows, schema)
                batch_index += 1
                logger.debug(
                    "Batch %s lido: rows=%s | columns=%s",
                    batch_index,
                    batch.height,
                    list(batch.columns),
                )
                yield batch

    def _prepare_query(self) -> tuple[str, tuple[Any, ...] | None]:
        """Resolve a query e aplica lower bound incremental, se houver."""
        query = self.resolve_query()
        if self._lower_bound is None:
            return query, None

        column_name, min_value = self._lower_bound
        wrapped = (
            f"SELECT * FROM ({query}) AS _metro_sub "
            f"WHERE {_quote_ident(column_name)} >= %s"
        )
        return wrapped, (min_value,)

    def _log_table_metadata(self, table: Table) -> None:
        """Registra metadados da tabela via `information_schema` em nível debug."""
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
        """Lista colunas da tabela a partir de `information_schema.columns`."""
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
        """Garante que a conexão PostgreSQL está aberta."""
        if self._connection is None:
            raise RuntimeError(
                "PostgreSQLSource não está conectado. Chame connect() antes de read()."
            )
        return self._connection


def _quote_ident(identifier: str) -> str:
    """Escapa identificador SQL com aspas duplas."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _dataframe_from_rows(
    rows: list[tuple[Any, ...]],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Converte linhas do cursor em Polars DataFrame com o schema informado."""
    if not rows:
        return pl.DataFrame(schema=schema) if schema else pl.DataFrame()
    if not schema:
        return pl.DataFrame(rows, orient="row", infer_schema_length=None)
    return pl.DataFrame(rows, schema=schema, orient="row")


def _polars_schema_from_description(
    description: Any,
) -> dict[str, pl.DataType]:
    """Monta schema Polars a partir de `cursor.description` do Psycopg."""
    if not description:
        return {}
    return {
        column.name: _pg_type_to_polars(column.type_code)
        for column in description
    }


# OIDs estáveis do PostgreSQL (pg_type). Tipos não mapeados viram String.
_PG_OID_TO_POLARS: dict[int, pl.DataType] = {
    16: pl.Boolean,
    20: pl.Int64,
    21: pl.Int16,
    23: pl.Int32,
    25: pl.String,
    700: pl.Float32,
    701: pl.Float64,
    1042: pl.String,
    1043: pl.String,
    1082: pl.Date,
    1083: pl.Time,
    1114: pl.Datetime("us"),
    1184: pl.Datetime("us", time_zone="UTC"),
    1700: pl.Float64,
    2950: pl.String,
    3802: pl.String,
}


def _pg_type_to_polars(type_code: int) -> pl.DataType:
    """Mapeia OID PostgreSQL para tipo Polars (fallback: String)."""
    return _PG_OID_TO_POLARS.get(int(type_code), pl.String)
