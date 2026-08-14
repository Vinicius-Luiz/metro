"""Estratégia Full Load."""

from __future__ import annotations

import gc
import logging

import polars as pl
import psutil

from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)

_BYTES_PER_MIB = 1024 * 1024


class FullLoadStrategy(ReplicationStrategy):
    """Ingestão completa do dataset: Source → Polars → Parquet → Target."""

    def __init__(self) -> None:
        super().__init__(mode="full_load")

    def execute(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        if source.chunk_size is None and target.chunk_size is None:
            self._execute_single(source, target, table)
            return

        self._execute_batched(source, target, table)

    def _execute_single(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        dataset_path = table.target_dataset_path
        logger.info(
            "Iniciando Full Load (table=%s, path=%s)",
            table.qualified_name,
            dataset_path,
        )

        dataframe = source.read()
        logger.debug(
            "DataFrame carregado: rows=%s, columns=%s, column_names=%s, dtypes=%s",
            dataframe.height,
            dataframe.width,
            list(dataframe.columns),
            {name: str(dtype) for name, dtype in dataframe.schema.items()},
        )

        target.delete_partition(dataset_path)
        _write_part(target, dataset_path, 1, dataframe)
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
    ) -> None:
        if not target.supports_batch_write():
            raise RuntimeError(
                f"{type(target).__name__} não suporta escrita em batches. "
                "Remova source.chunk_size e target.chunk_size ou use um Target "
                "que implemente supports_batch_write()."
            )

        dataset_path = table.target_dataset_path
        write_size = target.chunk_size
        enable_gc = write_size is not None
        process = psutil.Process() if enable_gc else None
        logger.info(
            "Iniciando Full Load em batches (table=%s, path=%s, "
            "source.chunk_size=%s, target.chunk_size=%s)",
            table.qualified_name,
            dataset_path,
            source.chunk_size,
            write_size,
        )

        target.delete_partition(dataset_path)

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
                _write_part(target, dataset_path, file_index, dataframe)
                total_rows += dataframe.height
                return

            while dataframe.height >= write_size:
                chunk = dataframe.slice(0, write_size)
                dataframe = dataframe.slice(write_size)
                file_index += 1
                _write_part(target, dataset_path, file_index, chunk)
                total_rows += chunk.height
                del chunk
                _collect_garbage(process)

            if dataframe.height == 0:
                return
            if force:
                file_index += 1
                _write_part(target, dataset_path, file_index, dataframe)
                total_rows += dataframe.height
                del dataframe
                _collect_garbage(process)
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


def _write_part(
    target: TargetEndpoint,
    dataset_path: str,
    file_index: int,
    dataframe: pl.DataFrame,
) -> None:
    part_path = f"{dataset_path}/part_{file_index:04d}.parquet"
    logger.info(
        "Materializando parte %s (path=%s, rows=%s)",
        file_index,
        part_path,
        dataframe.height,
    )
    target.write(dataframe, part_path)


def _rss_mib(process: psutil.Process) -> float:
    return process.memory_info().rss / _BYTES_PER_MIB


def _collect_garbage(process: psutil.Process | None) -> None:
    """Coleta lixo após a parte já estar persistida no Target."""
    if process is None:
        return

    memory_before_mb = _rss_mib(process)
    counts_before = gc.get_count()
    collected = gc.collect()
    memory_after_mb = _rss_mib(process)
    freed_mb = memory_before_mb - memory_after_mb
    freed_percent = (
        (freed_mb / memory_before_mb) * 100 if memory_before_mb > 0 else 0.0
    )
    logger.info(
        "Garbage collection executado | memory_before_mb=%.1f | "
        "memory_after_mb=%.1f | freed_mb=%.1f | freed_percent=%.1f | collected=%s",
        memory_before_mb,
        memory_after_mb,
        freed_mb,
        freed_percent,
        collected,
    )
    logger.debug(
        "GC detalhes | counts_before=%s | counts_after=%s | stats=%s",
        counts_before,
        gc.get_count(),
        gc.get_stats(),
    )
