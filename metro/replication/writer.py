"""Helpers compartilhados de escrita Parquet e GC entre strategies."""

from __future__ import annotations

import gc
import logging
from typing import Any

import polars as pl
import psutil

from metro.replication.partitioning import split_by_partition
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)

_BYTES_PER_MIB = 1024 * 1024


def write_part(
    target: TargetEndpoint,
    path_prefix: str,
    file_index: int,
    dataframe: pl.DataFrame,
) -> None:
    """Materializa um arquivo `part_NNNN.parquet` sob o path informado."""
    part_path = f"{path_prefix}/part_{file_index:04d}.parquet"
    logger.info(
        "Materializando parte %s (path=%s, rows=%s)",
        file_index,
        part_path,
        dataframe.height,
    )
    target.write(dataframe, part_path)


def write_batched(
    source: SourceEndpoint,
    target: TargetEndpoint,
    staging_path: str,
    track_max: str | None = None,
) -> tuple[int, Any]:
    """Materializa batches em arquivos Parquet planos sob staging_path.

    Processa batches do Source, acumula DataFrames até atingir write_size,
    executa GC automático e opcionalmente rastreia valor máximo de coluna.

    Args:
        source: Source Endpoint para leitura de batches.
        target: Target Endpoint para escrita.
        staging_path: Path base de staging (_tmp).
        track_max: Coluna para rastrear valor máximo (usado por Append watermark).

    Returns:
        (total_rows, max_value) onde max_value é None se track_max não informado.
    """
    write_size = target.chunk_size
    process = psutil.Process() if write_size is not None else None

    accumulated: list[pl.DataFrame] = []
    accumulated_rows = 0
    file_index = 0
    total_rows = 0
    read_batches = 0
    tracked_max: Any = None

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

        if track_max is not None:
            if track_max not in batch.columns:
                raise ValueError(
                    f"Coluna de watermark '{track_max}' não encontrada "
                    f"no DataFrame. Colunas: {list(batch.columns)}"
                )
            batch_max = batch[track_max].max()
            if batch_max is not None and (
                tracked_max is None or batch_max > tracked_max
            ):
                tracked_max = batch_max

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
        "Materialização em batches concluída (rows=%s, files=%s, "
        "read_batches=%s, tracked_max=%s)",
        total_rows,
        file_index,
        read_batches,
        tracked_max if track_max else "N/A",
    )
    return total_rows, tracked_max


def write_partitioned(
    source: SourceEndpoint,
    target: TargetEndpoint,
    staging_path: str,
    reference_column: str,
    granularity: str,
    allowed_partitions: frozenset[str] | None = None,
    track_max: str | None = None,
) -> tuple[int, Any]:
    """Materializa batches particionados em Hive sob staging_path.

    Quando `allowed_partitions` é None, grava todas as partições encontradas.
    Quando informado, grava apenas as partições da janela (Replace).
    
    Args:
        track_max: Se informado, rastreia o valor máximo desta coluna durante
                   a escrita (usado por Append para atualizar watermark).
    
    Returns:
        (total_rows, max_value) onde max_value é None se track_max não informado.
    """
    write_size = target.chunk_size
    process = psutil.Process() if write_size is not None else None

    accumulated: dict[str, list[pl.DataFrame]] = {}
    accumulated_rows: dict[str, int] = {}
    file_index: dict[str, int] = {}
    total_rows = 0
    read_batches = 0
    tracked_max: Any = None

    def flush_partition(partition_path: str, force: bool = False) -> None:
        nonlocal total_rows
        frames = accumulated.get(partition_path)
        if not frames:
            return

        dataframe = (
            frames[0] if len(frames) == 1 else pl.concat(frames, how="vertical")
        )
        accumulated[partition_path] = []
        accumulated_rows[partition_path] = 0

        path_prefix = f"{staging_path}/{partition_path}"

        if write_size is None:
            file_index[partition_path] = file_index.get(partition_path, 0) + 1
            write_part(
                target,
                path_prefix,
                file_index[partition_path],
                dataframe,
            )
            total_rows += dataframe.height
            return

        while dataframe.height >= write_size:
            chunk = dataframe.slice(0, write_size)
            dataframe = dataframe.slice(write_size)
            file_index[partition_path] = file_index.get(partition_path, 0) + 1
            write_part(
                target,
                path_prefix,
                file_index[partition_path],
                chunk,
            )
            total_rows += chunk.height
            del chunk
            collect_garbage(process)

        if dataframe.height == 0:
            return
        if force:
            file_index[partition_path] = file_index.get(partition_path, 0) + 1
            write_part(
                target,
                path_prefix,
                file_index[partition_path],
                dataframe,
            )
            total_rows += dataframe.height
            del dataframe
            collect_garbage(process)
            return

        accumulated[partition_path] = [dataframe]
        accumulated_rows[partition_path] = dataframe.height

    for batch in source.read_batches():
        read_batches += 1
        if batch.height == 0:
            continue
        if reference_column not in batch.columns:
            raise ValueError(
                f"Coluna de referência '{reference_column}' não encontrada "
                f"no DataFrame. Colunas: {list(batch.columns)}"
            )

        if track_max is not None:
            if track_max not in batch.columns:
                raise ValueError(
                    f"Coluna de watermark '{track_max}' não encontrada "
                    f"no DataFrame. Colunas: {list(batch.columns)}"
                )
            batch_max = batch[track_max].max()
            if batch_max is not None and (
                tracked_max is None or batch_max > tracked_max
            ):
                tracked_max = batch_max

        for partition_path, partition_df in split_by_partition(
            batch,
            reference_column,
            granularity,
        ):
            if (
                allowed_partitions is not None
                and partition_path not in allowed_partitions
            ):
                logger.debug(
                    "Partição fora da janela ignorada: %s",
                    partition_path,
                )
                continue
            if partition_df.height == 0:
                continue
            accumulated.setdefault(partition_path, []).append(partition_df)
            accumulated_rows[partition_path] = (
                accumulated_rows.get(partition_path, 0) + partition_df.height
            )
            if write_size is None or accumulated_rows[partition_path] >= write_size:
                flush_partition(partition_path)

    for partition_path in list(accumulated.keys()):
        flush_partition(partition_path, force=True)

    logger.info(
        "Materialização particionada concluída (rows=%s, files=%s, "
        "read_batches=%s, partitions_written=%s, tracked_max=%s)",
        total_rows,
        sum(file_index.values()),
        read_batches,
        len(file_index),
        tracked_max if track_max else "N/A",
    )
    return total_rows, tracked_max


def rss_mib(process: psutil.Process) -> float:
    """Retorna o RSS do processo em MiB."""
    return process.memory_info().rss / _BYTES_PER_MIB


def collect_garbage(process: psutil.Process | None) -> None:
    """Coleta lixo após a parte já estar persistida no Target."""
    if process is None:
        return

    memory_before_mb = rss_mib(process)
    counts_before = gc.get_count()
    collected = gc.collect()
    memory_after_mb = rss_mib(process)
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
