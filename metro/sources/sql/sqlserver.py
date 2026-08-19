"""Source Endpoint SQL Server."""

from __future__ import annotations

import datetime
import decimal
import logging
from collections.abc import Iterator
from typing import Any

import mssql_python
import polars as pl

from metro.core.table import Table
from metro.queries.base import QueryRepository
from metro.secrets.base import SecretProvider
from metro.sources.base import SourceEndpoint

logger = logging.getLogger(__name__)

# mssql-python só aceita UID/PWD e Encrypt/TrustServerCertificate como yes/no.
_ADO_TO_MSSQL_PYTHON_KEYS = {
    "user id": "UID",
    "userid": "UID",
    "user": "UID",
    "password": "PWD",
}
_BOOLISH_KEYS = {"encrypt", "trustservercertificate", "trust_server_certificate"}
_BOOLISH_VALUES = {
    "true": "yes",
    "false": "no",
    "1": "yes",
    "0": "no",
}


def _normalize_connection_string(connection_string: str) -> str:
    """Normaliza keywords ADO.NET para o formato aceito pelo mssql-python."""
    parts: list[str] = []
    for fragment in connection_string.split(";"):
        fragment = fragment.strip()
        if not fragment:
            continue
        if "=" not in fragment:
            parts.append(fragment)
            continue
        key, value = fragment.split("=", 1)
        key_stripped = key.strip()
        value_stripped = value.strip()
        canonical = _ADO_TO_MSSQL_PYTHON_KEYS.get(
            key_stripped.lower(), key_stripped
        )
        if key_stripped.lower() in _BOOLISH_KEYS:
            value_stripped = _BOOLISH_VALUES.get(
                value_stripped.lower(), value_stripped
            )
        parts.append(f"{canonical}={value_stripped}")
    return ";".join(parts) + ";"


class SQLServerSource(SourceEndpoint):
    """Extrai dados de SQL Server e entrega Polars DataFrame."""

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
        self._connection: mssql_python.Connection | None = None
        self._lower_bound: tuple[str, Any, bool] | None = None

    def connect(self) -> None:
        secret = self._secret_provider.get_secret(self.runtime)
        if not isinstance(secret, str):
            raise TypeError(
                f"SQL Server espera connection string para runtime '{self.runtime}', "
                f"obtido: {type(secret).__name__}"
            )

        logger.info("Conectando ao SQL Server (runtime=%s)", self.runtime)
        logger.debug(
            "Parâmetros Source SQL Server | runtime=%s | query_path=%s | "
            "chunk_size=%s | table=%s",
            self.runtime,
            self.query_path,
            self.chunk_size,
            None if self.table is None else self.table.qualified_name,
        )
        self._connection = mssql_python.connect(
            _normalize_connection_string(secret)
        )
        logger.debug(
            "Conexão SQL Server estabelecida | autocommit=%s",
            self._connection.autocommit,
        )
        if self.table is not None and self.table.schema_name:
            self._log_table_metadata(self.table)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Conexão SQL Server encerrada (runtime=%s)", self.runtime)

    def build_default_query(self) -> str:
        """Monta SELECT com todas as colunas da table via INFORMATION_SCHEMA."""
        table = self.require_table()
        if not table.schema_name:
            raise ValueError(
                "schema_name é obrigatório para montar a query padrão no SQL Server"
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

    def apply_lower_bound(
        self,
        reference_column: str,
        min_value: Any,
        *,
        inclusive: bool = True,
    ) -> None:
        if not reference_column or not reference_column.strip():
            raise ValueError("reference_column deve ser um identificador não vazio")
        self._lower_bound = (reference_column.strip(), min_value, inclusive)
        logger.info(
            "Lower bound aplicado (runtime=%s, column=%s, min_value=%s, inclusive=%s)",
            self.runtime,
            self._lower_bound[0],
            min_value,
            inclusive,
        )

    def read(self) -> pl.DataFrame:
        connection = self._require_connection()
        query, params = self._prepare_query()
        logger.info("Executando query no SQL Server (runtime=%s)", self.runtime)
        logger.debug("Query path: %s", self.query_path)
        logger.debug("SQL: %s | params=%s", query, params)

        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            schema = _polars_schema_from_description(cursor.description)
        finally:
            cursor.close()

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
            "Lendo SQL Server em batches (runtime=%s, chunk_size=%s)",
            self.runtime,
            self.chunk_size,
        )
        logger.debug("SQL (batches): %s | params=%s", query, params)

        cursor = connection.cursor()
        try:
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
        finally:
            cursor.close()

    def _require_connection(self) -> mssql_python.Connection:
        """Garante que a conexão SQL Server está aberta."""
        if self._connection is None:
            raise RuntimeError(
                "SQLServerSource não está conectado. Chame connect() antes de read()."
            )
        return self._connection

    def _list_column_metadata(
        self,
        connection: mssql_python.Connection,
        table: Table,
    ) -> list[dict[str, Any]]:
        """Lista colunas da tabela a partir de INFORMATION_SCHEMA.COLUMNS."""
        sql = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
            ORDER BY ordinal_position
        """
        cursor = connection.cursor()
        try:
            cursor.execute(sql, (table.schema_name, table.name))
            return [
                {
                    "name": row[0],
                    "data_type": row[1],
                    "nullable": row[2] == "YES",
                    "max_length": row[3],
                    "numeric_precision": row[4],
                    "numeric_scale": row[5],
                    "ordinal_position": row[6],
                }
                for row in cursor.fetchall()
            ]
        finally:
            cursor.close()

    def _log_table_metadata(self, table: Table) -> None:
        """Registra metadados da tabela via INFORMATION_SCHEMA em nível debug."""
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

    def _prepare_query(self) -> tuple[str, tuple[Any, ...] | None]:
        """Resolve a query e aplica lower bound incremental, se houver."""
        query = self.resolve_query()
        if self._lower_bound is None:
            return query, None

        column_name, min_value, inclusive = self._lower_bound
        operator = ">=" if inclusive else ">"
        # Append/MaxValue usa exclusive (>) para não reprocessar o watermark.
        # Replace/Partition usa inclusive (>=) para cobrir o início da janela.
        wrapped = (
            f"SELECT * FROM ({query}) AS _metro_sub "
            f"WHERE {_quote_ident(column_name)} {operator} ?"
        )
        return wrapped, (_coerce_bound_value(min_value),)


def _coerce_bound_value(value: Any) -> Any:
    """Converte strings ISO de data/hora para datetime (evita DATEFORMAT dmy)."""
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
        if fmt == "%Y-%m-%d":
            return parsed.date()
        return parsed
    return value


def _quote_ident(identifier: str) -> str:
    """Escapa identificador SQL Server com colchetes."""
    escaped = identifier.replace("]", "]]")
    return f"[{escaped}]"


def _dataframe_from_rows(
    rows: list[Any],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Converte linhas do cursor em Polars DataFrame com o schema informado."""
    tuple_rows = [tuple(row) for row in rows]
    if not tuple_rows:
        return pl.DataFrame(schema=schema) if schema else pl.DataFrame()
    if not schema:
        return pl.DataFrame(tuple_rows, orient="row", infer_schema_length=None)
    return pl.DataFrame(tuple_rows, schema=schema, orient="row")


def _polars_schema_from_description(
    description: Any,
) -> dict[str, pl.DataType]:
    """Monta schema Polars a partir de cursor.description do mssql-python."""
    if not description:
        return {}
    return {
        column[0]: _mssql_python_type_to_polars(column[1])
        for column in description
    }


# mssql-python expõe a classe Python do tipo SQL, não um OID numérico.
_MSSQL_PYTHON_TYPE_TO_POLARS: dict[type, pl.DataType] = {
    bool: pl.Boolean,
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bytes: pl.String,
    datetime.date: pl.Date,
    datetime.time: pl.Time,
    datetime.datetime: pl.Datetime("us"),
    decimal.Decimal: pl.Float64,
}


def _mssql_python_type_to_polars(type_class: type) -> pl.DataType:
    """Mapeia classe Python do mssql-python para tipo Polars (fallback: String)."""
    return _MSSQL_PYTHON_TYPE_TO_POLARS.get(type_class, pl.String)
