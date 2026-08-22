"""Cliente HTTP para comunicação com a API de logging do METRO."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import requests

from metro.settings import settings

logger = logging.getLogger(__name__)


class LoggingAPIError(RuntimeError):
    """Erro de comunicação com a API de logging."""


class LoggingClient:
    """Cliente HTTP para registrar o ciclo de vida de execuções.

    A API é a única abstração necessária — o METRO apenas consome HTTP.
    """

    def __init__(self, api_base_url: str) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        logger.debug(
            "LoggingClient configurado com api_base_url=%s",
            self._api_base_url,
        )

    def create_execution(
        self,
        *,
        schema_name: str | None,
        name: str,
        target_schema_name: str,
        target_name: str,
        mode: str,
        source_type: str,
        source_runtime: str,
        target_type: str,
        target_runtime: str,
        strategy_type: str | None = None,
        strategy_reference_column: str | None = None,
        strategy_lookback_periods: int | None = None,
        partition_type: str | None = None,
        partition_reference_column: str | None = None,
        started_at: datetime | None = None,
    ) -> int:
        """Cria execução com status running. Retorna execution_id."""
        url = f"{self._api_base_url}/executions"
        payload: dict[str, Any] = {
            "status": "running",
            "schema_name": schema_name,
            "name": name,
            "target_schema_name": target_schema_name,
            "target_name": target_name,
            "mode": mode,
            "source_type": source_type,
            "source_runtime": source_runtime,
            "target_type": target_type,
            "target_runtime": target_runtime,
            "strategy_type": strategy_type,
            "strategy_reference_column": strategy_reference_column,
            "strategy_lookback_periods": strategy_lookback_periods,
            "partition_type": partition_type,
            "partition_reference_column": partition_reference_column,
        }
        if started_at is not None:
            payload["started_at"] = started_at.isoformat()

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=settings.logging_api_timeout,
            )
            response.raise_for_status()
            data = response.json()
            execution_id = int(data["id"])
            logger.debug(
                "Execução criada (id=%s, table=%s.%s)",
                execution_id,
                schema_name,
                name,
            )
            return execution_id
        except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
            raise LoggingAPIError(
                f"Falha ao criar execução em {url}: {exc}"
            ) from exc

    def update_execution(
        self,
        execution_id: int,
        *,
        finished_at: datetime | None = None,
        status: str | None = None,
        rows_processed: int | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        """Atualiza execução (parcial)."""
        url = f"{self._api_base_url}/executions/{execution_id}"
        payload: dict[str, Any] = {}
        if finished_at is not None:
            payload["finished_at"] = finished_at.isoformat()
        if status is not None:
            payload["status"] = status
        if rows_processed is not None:
            payload["rows_processed"] = rows_processed
        if duration_seconds is not None:
            payload["duration_seconds"] = duration_seconds

        if not payload:
            return

        try:
            response = requests.patch(
                url,
                json=payload,
                timeout=settings.logging_api_timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise LoggingAPIError(
                f"Falha ao atualizar execução {execution_id} em {url}: {exc}"
            ) from exc

    def finish_execution(
        self,
        execution_id: int,
        status: str,
        rows_processed: int,
        duration_seconds: float,
    ) -> None:
        """Finaliza execução com status success/error/cancelled."""
        self.update_execution(
            execution_id,
            finished_at=datetime.now(),
            status=status,
            rows_processed=rows_processed,
            duration_seconds=duration_seconds,
        )
        logger.debug(
            "Execução finalizada (id=%s, status=%s, rows=%s, duration=%.2fs)",
            execution_id,
            status,
            rows_processed,
            duration_seconds,
        )
