"""Estratégia Full Load."""

from __future__ import annotations

import logging

import polars as pl
import psutil

from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.replication.writer import collect_garbage, write_part, write_partitioned
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)


class FullLoadStrategy(ReplicationStrategy):
    """Ingestão completa do dataset: Source → Polars → Parquet → Target."""

    def __init__(
        self,
        reference_column: str | None = None,
        granularity: str | None = None,
    ) -> None:
        super().__init__(
            mode="full_load",
            reference_column=reference_column,
            partition_type=granularity,
        )
        self._reference_column = reference_column
        self._granularity = granularity

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
        write_partitioned(
            source=source,
            target=target,
            staging_path=staging_path,
            reference_column=self._reference_column,
            granularity=self._granularity,
            allowed_partitions=None,
        )
        logger.info(
            "Full Load particionado concluído (table=%s)",
            table.qualified_name,
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

        write_part(target, staging_path, 1, dataframe)
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

        write_size = target.chunk_size
        enable_gc = write_size is not None
        process = psutil.Process() if enable_gc else None
        logger.info(
            "Iniciando Full Load em batches (table=%s, path=%s, "
            "source.chunk_size=%s, target.chunk_size=%s)",
            table.qualified_name,
            table.target_dataset_path,
            source.chunk_size,
            write_size,
        )

        accumulated: list[pl.DataFrame] = []
        accumulated_rows = 0
        file_index = 0
        total_rows = 0
        read_batches = 0

        def flush(force: bool = False) -> None:
            nonlocal accumulated, accumulated_rows, file_index, total_rows
            if not accumulated:
                return

            dataframe = (
                accumulated[0]
                if len(accumulated) == 1
                else pl.concat(accumulated, how="vertical")
            )
            accumulated = []
            accumulated_rows = 0

            if write_size is None:
                file_index += 1
                write_part(target, staging_path, file_index, dataframe)
                total_rows += dataframe.height
                return

            while dataframe.height >= write_size:
                chunk = dataframe.slice(0, write_size)
                dataframe = dataframe.slice(write_size)
                file_index += 1
                write_part(target, staging_path, file_index, chunk)
                total_rows += chunk.height
                del chunk
                collect_garbage(process)

            if dataframe.height == 0:
                return
            if force:
                file_index += 1
                write_part(target, staging_path, file_index, dataframe)
                total_rows += dataframe.height
                del dataframe
                collect_garbage(process)
                return

            accumulated = [dataframe]
            accumulated_rows = dataframe.height

        for batch in source.read_batches():
            read_batches += 1
            if batch.height == 0:
                continue
            logger.debug(
                "Batch de leitura %s: rows=%s, columns=%s",
                read_batches,
                batch.height,
                list(batch.columns),
            )
            accumulated.append(batch)
            accumulated_rows += batch.height
            if write_size is None or accumulated_rows >= write_size:
                flush()

        flush(force=True)

        logger.info(
            "Full Load concluído (table=%s, rows=%s, files=%s, read_batches=%s)",
            table.qualified_name,
            total_rows,
            file_index,
            read_batches,
        )
