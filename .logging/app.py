"""API minimalista para execuções do METRO usando psycopg direto."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Literal

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

DATABASE_URL = os.getenv("METRO_LOGGING_DATABASE")
if not DATABASE_URL:
    raise RuntimeError("METRO_LOGGING_DATABASE não configurado no .env")

ExecutionStatus = Literal["running", "success", "error", "cancelled"]

app = FastAPI(
    title="METRO Logging API",
    description="API minimalista para registro de execuções do METRO",
    version="0.4.0",
)


class ExecutionCreate(BaseModel):
    started_at: datetime | None = None
    status: ExecutionStatus = "running"
    schema_name: str | None = Field(default=None, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    target_schema_name: str = Field(..., min_length=1, max_length=255)
    target_name: str = Field(..., min_length=1, max_length=255)
    mode: str = Field(..., min_length=1, max_length=255)
    source_type: str = Field(..., min_length=1, max_length=255)
    source_runtime: str = Field(..., min_length=1, max_length=255)
    target_type: str = Field(..., min_length=1, max_length=255)
    target_runtime: str = Field(..., min_length=1, max_length=255)
    strategy_type: str | None = Field(default=None, max_length=255)
    strategy_reference_column: str | None = Field(default=None, max_length=255)
    strategy_lookback_periods: int | None = None
    partition_type: str | None = Field(default=None, max_length=255)
    partition_reference_column: str | None = Field(default=None, max_length=255)


class ExecutionUpdate(BaseModel):
    finished_at: datetime | None = None
    status: ExecutionStatus | None = None
    rows_processed: int | None = None
    duration_seconds: float | None = None


class ExecutionResponse(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    schema_name: str | None
    name: str
    target_schema_name: str
    target_name: str
    mode: str
    source_type: str
    source_runtime: str
    target_type: str
    target_runtime: str
    strategy_type: str | None
    strategy_reference_column: str | None
    strategy_lookback_periods: int | None
    partition_type: str | None
    partition_reference_column: str | None
    rows_processed: int | None
    duration_seconds: float | None


RETURNING_COLUMNS = """
    id,
    started_at,
    finished_at,
    status,
    schema_name,
    name,
    target_schema_name,
    target_name,
    mode,
    source_type,
    source_runtime,
    target_type,
    target_runtime,
    strategy_type,
    strategy_reference_column,
    strategy_lookback_periods,
    partition_type,
    partition_reference_column,
    rows_processed,
    duration_seconds
"""


@contextmanager
def get_db_connection() -> Iterator[psycopg.Connection]:
    """Context manager para conexão com PostgreSQL."""
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def row_to_dict(row: tuple[Any, ...], cursor: psycopg.Cursor) -> dict[str, Any]:
    """Converte row tuple para dict usando descrição do cursor."""
    data = {desc.name: value for desc, value in zip(cursor.description, row)}
    if data.get("duration_seconds") is not None:
        data["duration_seconds"] = float(data["duration_seconds"])
    if data.get("rows_processed") is not None:
        data["rows_processed"] = int(data["rows_processed"])
    if data.get("strategy_lookback_periods") is not None:
        data["strategy_lookback_periods"] = int(data["strategy_lookback_periods"])
    return data


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "metro-logging-api"}


@app.post(
    "/executions",
    response_model=ExecutionResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_execution(execution: ExecutionCreate) -> ExecutionResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO logging.executions
                (
                    started_at,
                    status,
                    schema_name,
                    name,
                    target_schema_name,
                    target_name,
                    mode,
                    source_type,
                    source_runtime,
                    target_type,
                    target_runtime,
                    strategy_type,
                    strategy_reference_column,
                    strategy_lookback_periods,
                    partition_type,
                    partition_reference_column
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING {RETURNING_COLUMNS}
                """,
                (
                    execution.started_at or datetime.now(),
                    execution.status,
                    execution.schema_name,
                    execution.name,
                    execution.target_schema_name,
                    execution.target_name,
                    execution.mode,
                    execution.source_type,
                    execution.source_runtime,
                    execution.target_type,
                    execution.target_runtime,
                    execution.strategy_type,
                    execution.strategy_reference_column,
                    execution.strategy_lookback_periods,
                    execution.partition_type,
                    execution.partition_reference_column,
                ),
            )
            conn.commit()
            row = cur.fetchone()
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Falha ao criar execução",
                )
            return ExecutionResponse(**row_to_dict(row, cur))


@app.patch("/executions/{execution_id}", response_model=ExecutionResponse)
def update_execution(
    execution_id: int,
    update: ExecutionUpdate,
) -> ExecutionResponse:
    fields: list[str] = []
    params: list[Any] = []

    if update.finished_at is not None:
        fields.append("finished_at = %s")
        params.append(update.finished_at)
    if update.status is not None:
        fields.append("status = %s")
        params.append(update.status)
    if update.rows_processed is not None:
        fields.append("rows_processed = %s")
        params.append(update.rows_processed)
    if update.duration_seconds is not None:
        fields.append("duration_seconds = %s")
        params.append(update.duration_seconds)

    if not fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nenhum campo para atualizar",
        )

    params.append(execution_id)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE logging.executions
                SET {", ".join(fields)}
                WHERE id = %s
                RETURNING {RETURNING_COLUMNS}
                """,
                params,
            )
            conn.commit()
            row = cur.fetchone()
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Execução não encontrada (id={execution_id})",
                )
            return ExecutionResponse(**row_to_dict(row, cur))


@app.get("/executions/{execution_id}", response_model=ExecutionResponse)
def get_execution(execution_id: int) -> ExecutionResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {RETURNING_COLUMNS}
                FROM logging.executions
                WHERE id = %s
                """,
                (execution_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Execução não encontrada (id={execution_id})",
                )
            return ExecutionResponse(**row_to_dict(row, cur))


@app.get("/executions", response_model=list[ExecutionResponse])
def list_executions(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    status_filter: ExecutionStatus | None = Query(default=None, alias="status"),
    schema_name: str | None = Query(default=None),
    name: str | None = Query(default=None),
    target_schema_name: str | None = Query(default=None),
    target_name: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    source_type: str | None = Query(default=None),
    target_type: str | None = Query(default=None),
    strategy_type: str | None = Query(default=None),
) -> list[ExecutionResponse]:
    conditions: list[str] = []
    params: list[Any] = []

    if status_filter is not None:
        conditions.append("status = %s")
        params.append(status_filter)
    if schema_name is not None:
        conditions.append("schema_name = %s")
        params.append(schema_name)
    if name is not None:
        conditions.append("name = %s")
        params.append(name)
    if target_schema_name is not None:
        conditions.append("target_schema_name = %s")
        params.append(target_schema_name)
    if target_name is not None:
        conditions.append("target_name = %s")
        params.append(target_name)
    if mode is not None:
        conditions.append("mode = %s")
        params.append(mode)
    if source_type is not None:
        conditions.append("source_type = %s")
        params.append(source_type)
    if target_type is not None:
        conditions.append("target_type = %s")
        params.append(target_type)
    if strategy_type is not None:
        conditions.append("strategy_type = %s")
        params.append(strategy_type)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    params.extend([limit, skip])

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {RETURNING_COLUMNS}
                FROM logging.executions
                {where_clause}
                ORDER BY started_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            )
            rows = cur.fetchall()
            return [ExecutionResponse(**row_to_dict(row, cur)) for row in rows]
