"""Logger de ciclo de vida de execução do METRO (1 registro por run)."""

from __future__ import annotations

import logging
from datetime import datetime

from metro.logging.client import LoggingAPIError, LoggingClient

logger = logging.getLogger(__name__)


class ExecutionLogger:
    """Gerencia o ciclo de vida de 1 execução do METRO.

    Registra início, atualiza progresso e finaliza com status/métricas.
    Falhas de envio não interrompem a task.
    """

    def __init__(self, client: LoggingClient) -> None:
        self._client = client
        self._execution_id: int | None = None
        self._started_at: datetime | None = None

    @property
    def execution_id(self) -> int | None:
        """ID da execução atual, se já iniciada."""
        return self._execution_id

    def start(
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
    ) -> None:
        """Inicia execução (POST /executions)."""
        self._started_at = datetime.now()
        try:
            self._execution_id = self._client.create_execution(
                schema_name=schema_name,
                name=name,
                target_schema_name=target_schema_name,
                target_name=target_name,
                mode=mode,
                source_type=source_type,
                source_runtime=source_runtime,
                target_type=target_type,
                target_runtime=target_runtime,
                strategy_type=strategy_type,
                strategy_reference_column=strategy_reference_column,
                strategy_lookback_periods=strategy_lookback_periods,
                partition_type=partition_type,
                partition_reference_column=partition_reference_column,
                started_at=self._started_at,
            )
            logger.info(
                "Execução registrada (id=%s, table=%s.%s -> %s/%s)",
                self._execution_id,
                schema_name,
                name,
                target_schema_name,
                target_name,
            )
        except LoggingAPIError as exc:
            logger.warning("Falha ao registrar início da execução: %s", exc)
            self._execution_id = None

    def update_rows(self, rows_processed: int) -> None:
        """Atualiza contador de rows (opcional, durante execução)."""
        if self._execution_id is None:
            return
        try:
            self._client.update_execution(
                self._execution_id,
                rows_processed=rows_processed,
            )
        except LoggingAPIError as exc:
            logger.warning("Falha ao atualizar rows da execução: %s", exc)

    def finish_success(self, rows_processed: int) -> None:
        """Finaliza execução com sucesso."""
        self._finish(status="success", rows_processed=rows_processed)

    def finish_error(self, rows_processed: int = 0) -> None:
        """Finaliza execução com erro."""
        self._finish(status="error", rows_processed=rows_processed)

    def _finish(
        self,
        *,
        status: str,
        rows_processed: int,
    ) -> None:
        if self._execution_id is None or self._started_at is None:
            return
        duration = (datetime.now() - self._started_at).total_seconds()
        try:
            self._client.finish_execution(
                self._execution_id,
                status=status,
                rows_processed=rows_processed,
                duration_seconds=round(duration, 2),
            )
            logger.info(
                "Execução finalizada (id=%s, status=%s, rows=%s, duration=%.2fs)",
                self._execution_id,
                status,
                rows_processed,
                duration,
            )
        except LoggingAPIError as exc:
            logger.warning("Falha ao finalizar execução: %s", exc)
