"""API CRUD minimalista para watermarks do METRO usando psycopg direto."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

DATABASE_URL = os.getenv("METRO_WATERMARK_POSTGRES_DATABASE")
if not DATABASE_URL:
    raise RuntimeError("METRO_WATERMARK_POSTGRES_DATABASE não configurado no .env")

app = FastAPI(
    title="METRO Watermark API",
    description="API CRUD minimalista para watermarks",
    version="0.1.0",
)


class WatermarkCreate(BaseModel):
    task_identifier: str = Field(..., min_length=1, max_length=255)
    reference_column: str = Field(..., min_length=1, max_length=255)
    watermark_value: str
    watermark_type: str = Field(..., min_length=1, max_length=50)
    last_record_count: int = 0


class WatermarkUpdate(BaseModel):
    watermark_value: str
    last_record_count: int | None = None


class WatermarkResponse(BaseModel):
    id: int
    task_identifier: str
    reference_column: str
    watermark_value: str
    watermark_type: str
    last_execution: datetime
    last_record_count: int
    created_at: datetime
    updated_at: datetime


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
    return {desc.name: value for desc, value in zip(cursor.description, row)}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "metro-watermark-api"}


@app.post(
    "/watermarks",
    response_model=WatermarkResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_watermark(watermark: WatermarkCreate) -> WatermarkResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO watermark.watermarks
                    (
                        task_identifier,
                        reference_column,
                        watermark_value,
                        watermark_type,
                        last_record_count
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING
                        id,
                        task_identifier,
                        reference_column,
                        watermark_value,
                        watermark_type,
                        last_execution,
                        last_record_count,
                        created_at,
                        updated_at
                    """,
                    (
                        watermark.task_identifier,
                        watermark.reference_column,
                        watermark.watermark_value,
                        watermark.watermark_type,
                        watermark.last_record_count,
                    ),
                )
                conn.commit()
                row = cur.fetchone()
                if row is None:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Falha ao criar watermark",
                    )
                return WatermarkResponse(**row_to_dict(row, cur))
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "Watermark já existe para "
                        f"task_identifier={watermark.task_identifier}, "
                        f"reference_column={watermark.reference_column}"
                    ),
                ) from None


@app.get(
    "/watermarks/{task_identifier}/{reference_column}",
    response_model=WatermarkResponse,
)
def get_watermark(task_identifier: str, reference_column: str) -> WatermarkResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    task_identifier,
                    reference_column,
                    watermark_value,
                    watermark_type,
                    last_execution,
                    last_record_count,
                    created_at,
                    updated_at
                FROM watermark.watermarks
                WHERE task_identifier = %s AND reference_column = %s
                """,
                (task_identifier, reference_column),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        "Watermark não encontrado para "
                        f"task_identifier={task_identifier}, "
                        f"reference_column={reference_column}"
                    ),
                )
            return WatermarkResponse(**row_to_dict(row, cur))


@app.put(
    "/watermarks/{task_identifier}/{reference_column}",
    response_model=WatermarkResponse,
)
def update_watermark(
    task_identifier: str,
    reference_column: str,
    update: WatermarkUpdate,
) -> WatermarkResponse:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if update.last_record_count is not None:
                cur.execute(
                    """
                    UPDATE watermark.watermarks
                    SET
                        watermark_value = %s,
                        last_record_count = %s,
                        last_execution = NOW()
                    WHERE task_identifier = %s AND reference_column = %s
                    RETURNING
                        id,
                        task_identifier,
                        reference_column,
                        watermark_value,
                        watermark_type,
                        last_execution,
                        last_record_count,
                        created_at,
                        updated_at
                    """,
                    (
                        update.watermark_value,
                        update.last_record_count,
                        task_identifier,
                        reference_column,
                    ),
                )
            else:
                cur.execute(
                    """
                    UPDATE watermark.watermarks
                    SET
                        watermark_value = %s,
                        last_execution = NOW()
                    WHERE task_identifier = %s AND reference_column = %s
                    RETURNING
                        id,
                        task_identifier,
                        reference_column,
                        watermark_value,
                        watermark_type,
                        last_execution,
                        last_record_count,
                        created_at,
                        updated_at
                    """,
                    (update.watermark_value, task_identifier, reference_column),
                )

            conn.commit()
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        "Watermark não encontrado para "
                        f"task_identifier={task_identifier}, "
                        f"reference_column={reference_column}"
                    ),
                )
            return WatermarkResponse(**row_to_dict(row, cur))


@app.delete(
    "/watermarks/{task_identifier}/{reference_column}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_watermark(task_identifier: str, reference_column: str) -> Response:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM watermark.watermarks
                WHERE task_identifier = %s AND reference_column = %s
                """,
                (task_identifier, reference_column),
            )
            if cur.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        "Watermark não encontrado para "
                        f"task_identifier={task_identifier}, "
                        f"reference_column={reference_column}"
                    ),
                )
            conn.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.get("/watermarks", response_model=list[WatermarkResponse])
def list_watermarks(skip: int = 0, limit: int = 100) -> list[WatermarkResponse]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    task_identifier,
                    reference_column,
                    watermark_value,
                    watermark_type,
                    last_execution,
                    last_record_count,
                    created_at,
                    updated_at
                FROM watermark.watermarks
                ORDER BY last_execution DESC
                LIMIT %s OFFSET %s
                """,
                (limit, skip),
            )
            rows = cur.fetchall()
            return [WatermarkResponse(**row_to_dict(row, cur)) for row in rows]
