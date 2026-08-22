"""Estratégia Full Load."""

from __future__ import annotations

import logging

from metro.core.metadata import MetadataContext
from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.replication.writer import write_batched, write_part, write_partitioned
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)


class FullLoadStrategy(ReplicationStrategy):
    """Ingestão completa do dataset: Source → Polars → Parquet → Target."""

    def __init__(
        self,
        reference_column: str | None = None,
        granularity: str | None = None,
        metadata_context: MetadataContext | None = None,
    ) -> None:
        super().__init__(
            mode="full_load",
            reference_column=reference_column,
            partition_type=granularity,
        )
        self._reference_column = reference_column
        self._granularity = granularity
        self._metadata_context = metadata_context

    def execute(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        dataset_path = table.target_dataset_path
        staging_path = target.begin_staging(dataset_path)
        try:
            if self._reference_column is not None and self._granularity is not None:
                self._execute_partitioned(
                    source,
                    target,
                    table,
                    staging_path,
                )
            elif source.chunk_size is None and target.chunk_size is None:
                self._execute_single(source, target, table, staging_path)
            else:
                self._execute_batched(source, target, table, staging_path)
            target.commit_staging(dataset_path, partitions=None)
        except Exception:
            target.discard_staging(dataset_path)
            raise

    def _execute_partitioned(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
        staging_path: str,
    ) -> None:
        """Full Load com escrita Hive por `reference_column` e granularidade."""
        assert self._reference_column is not None
        assert self._granularity is not None
        logger.info(
            "Iniciando Full Load particionado (table=%s, path=%s, "
            "column=%s, granularity=%s)",
            table.qualified_name,
            table.target_dataset_path,
            self._reference_column,
            self._granularity,
        )
        total_rows, _ = write_partitioned(
            source=source,
            target=target,
            staging_path=staging_path,
            reference_column=self._reference_column,
            granularity=self._granularity,
            allowed_partitions=None,
            metadata_context=self._metadata_context,
        )
        self._rows_processed = total_rows
        logger.info(
            "Full Load particionado concluído (table=%s, rows=%s)",
            table.qualified_name,
            total_rows,
        )

    def _execute_single(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
        staging_path: str,
    ) -> None:
        """Full Load em uma única leitura e um único arquivo Parquet."""
        logger.info(
            "Iniciando Full Load (table=%s, path=%s)",
            table.qualified_name,
            table.target_dataset_path,
        )

        dataframe = source.read()
        logger.debug(
            "DataFrame carregado: rows=%s, columns=%s, column_names=%s, dtypes=%s",
            dataframe.height,
            dataframe.width,
            list(dataframe.columns),
            {name: str(dtype) for name, dtype in dataframe.schema.items()},
        )

        write_part(
            target,
            staging_path,
            1,
            dataframe,
            metadata_context=self._metadata_context,
        )
        self._rows_processed = dataframe.height
        logger.info(
            "Full Load concluído (table=%s, rows=%s)",
            table.qualified_name,
            dataframe.height,
        )

    def _execute_batched(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
        staging_path: str,
    ) -> None:
        """Full Load com leitura/escrita em batches (`chunk_size`)."""
        if not target.supports_batch_write():
            raise RuntimeError(
                f"{type(target).__name__} não suporta escrita em batches. "
                "Remova source.chunk_size e target.chunk_size ou use um Target "
                "que implemente supports_batch_write()."
            )

        logger.info(
            "Iniciando Full Load em batches (table=%s, path=%s, "
            "source.chunk_size=%s, target.chunk_size=%s)",
            table.qualified_name,
            table.target_dataset_path,
            source.chunk_size,
            target.chunk_size,
        )

        total_rows, _ = write_batched(
            source=source,
            target=target,
            staging_path=staging_path,
            track_max=None,
            metadata_context=self._metadata_context,
        )
        self._rows_processed = total_rows

        logger.info(
            "Full Load concluído (table=%s, rows=%s)",
            table.qualified_name,
            total_rows,
        )
