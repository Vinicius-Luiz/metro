"""Estratégia Incremental Append / MaxValue."""

from __future__ import annotations

import logging
from typing import Any

from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.replication.writer import write_batched, write_partitioned
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint
from metro.watermark.client import WatermarkClient

logger = logging.getLogger(__name__)


class AppendMaxValueStrategy(ReplicationStrategy):
    """Append incremental baseado no valor máximo da reference_column (watermark)."""

    def __init__(
        self,
        reference_column: str,
        watermark_client: WatermarkClient,
        aggregation: str = "MAX",
        partition_type: str | None = None,
        partition_column: str | None = None,
    ) -> None:
        super().__init__(
            mode="incremental",
            strategy_type="append",
            method="max_value",
            reference_column=reference_column,
            aggregation=aggregation,
            partition_type=partition_type,
        )
        self._watermark_client = watermark_client
        self._partition_column = partition_column or reference_column

    def execute(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        if self.reference_column is None:
            raise RuntimeError("reference_column é obrigatório para Append/MaxValue")

        task_identifier = f"{table.target_schema_name}.{table.target_name}"
        dataset_path = table.target_dataset_path

        watermark_data = self._watermark_client.get_watermark(
            task_identifier,
            self.reference_column,
        )

        if watermark_data is None:
            logger.info(
                "Primeira execução Append/MaxValue (task_identifier=%s, "
                "reference_column=%s). Processando todo o dataset "
                "(equivalente a Full Load).",
                task_identifier,
                self.reference_column,
            )
            previous_watermark = None
            is_first_run = True
        else:
            previous_watermark = watermark_data["watermark_value"]
            is_first_run = False
            logger.info(
                "Executando Append/MaxValue (task_identifier=%s, "
                "reference_column=%s, watermark_anterior=%s)",
                task_identifier,
                self.reference_column,
                previous_watermark,
            )
            source.apply_lower_bound(
                self.reference_column,
                previous_watermark,
                inclusive=False,
            )

        staging_path = target.begin_staging(dataset_path)
        use_partition = self.partition_type is not None

        try:
            if use_partition:
                new_watermark, watermark_type, record_count = (
                    self._extract_and_write_partitioned(
                        source=source,
                        target=target,
                        staging_path=staging_path,
                    )
                )
            else:
                new_watermark, watermark_type, record_count = self._extract_and_write(
                    source=source,
                    target=target,
                    staging_path=staging_path,
                )

            if record_count == 0:
                logger.info(
                    "Nenhum dado novo encontrado (task_identifier=%s, "
                    "reference_column=%s, watermark=%s)",
                    task_identifier,
                    self.reference_column,
                    previous_watermark,
                )
                target.discard_staging(dataset_path)
                return

            logger.info(
                "Novo watermark calculado (reference_column=%s, watermark=%s, "
                "tipo=%s, rows=%s, partitioned=%s)",
                self.reference_column,
                new_watermark,
                watermark_type,
                record_count,
                use_partition,
            )

            if is_first_run:
                target.commit_staging(dataset_path, partitions=None)
                self._watermark_client.create_watermark(
                    task_identifier=task_identifier,
                    reference_column=self.reference_column,
                    watermark_value=new_watermark,
                    watermark_type=watermark_type,
                    record_count=record_count,
                )
            else:
                target.commit_append_staging(dataset_path)
                self._watermark_client.update_watermark(
                    task_identifier=task_identifier,
                    reference_column=self.reference_column,
                    watermark_value=new_watermark,
                    watermark_type=watermark_type,
                    record_count=record_count,
                )

            logger.info(
                "Append/MaxValue concluído (task_identifier=%s, rows=%s, "
                "watermark=%s)",
                task_identifier,
                record_count,
                new_watermark,
            )
        except Exception:
            target.discard_staging(dataset_path)
            raise

    def _extract_and_write(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        staging_path: str,
    ) -> tuple[Any, str, int]:
        """Extrai dados, materializa Parquet e retorna (max, tipo, rows)."""
        assert self.reference_column is not None

        total_rows, new_watermark = write_batched(
            source=source,
            target=target,
            staging_path=staging_path,
            track_max=self.reference_column,
        )

        watermark_type = _infer_watermark_type_from_value(new_watermark)

        logger.info(
            "Extração Append concluída (rows=%s, watermark=%s, type=%s)",
            total_rows,
            new_watermark,
            watermark_type,
        )
        return new_watermark, watermark_type, total_rows

    def _extract_and_write_partitioned(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        staging_path: str,
    ) -> tuple[Any, str, int]:
        """Extrai dados, materializa Parquet particionado Hive e retorna (max, tipo, rows)."""
        assert self.reference_column is not None
        assert self.partition_type is not None
        assert self._partition_column is not None

        logger.info(
            "Append particionado (partition_column=%s, granularity=%s)",
            self._partition_column,
            self.partition_type,
        )

        total_rows, new_watermark = write_partitioned(
            source=source,
            target=target,
            staging_path=staging_path,
            reference_column=self._partition_column,
            granularity=self.partition_type,
            allowed_partitions=None,
            track_max=self.reference_column,
        )

        watermark_type = _infer_watermark_type_from_value(new_watermark)

        logger.info(
            "Extração Append particionado concluída (rows=%s, watermark=%s)",
            total_rows,
            new_watermark,
        )
        return new_watermark, watermark_type, total_rows


def _infer_watermark_type_from_value(value: Any) -> str:
    """Infere o tipo do watermark a partir do valor rastreado."""
    if value is None:
        return "string"
    if hasattr(value, "year"):
        return "timestamp"
    if isinstance(value, int):
        return "int"
    return "string"
