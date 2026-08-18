"""Estratégia Incremental Replace / Partition."""

from __future__ import annotations

import logging
from datetime import date

from metro.core.metadata import MetadataContext
from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.replication.partitioning import window_cutoff, window_partitions
from metro.replication.writer import write_partitioned
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)


class ReplacePartitionStrategy(ReplicationStrategy):
    """Reconstrói partições Hive do dataset dentro de uma janela de lookback."""

    def __init__(
        self,
        reference_column: str,
        granularity: str,
        lookback_periods: int,
        metadata_context: MetadataContext | None = None,
    ) -> None:
        super().__init__(
            mode="incremental",
            strategy_type="replace",
            method="partition",
            reference_column=reference_column,
            partition_type=granularity,
        )
        self._granularity = granularity
        self._lookback_periods = lookback_periods
        self._metadata_context = metadata_context

    def execute(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        if self.reference_column is None:
            raise RuntimeError("reference_column é obrigatório para Replace/Partition")

        today = date.today()
        cutoff = window_cutoff(today, self._granularity, self._lookback_periods)
        partitions = window_partitions(
            today,
            self._granularity,
            self._lookback_periods,
        )
        partition_set = frozenset(partitions)
        dataset_path = table.target_dataset_path

        logger.info(
            "Iniciando Replace/Partition (table=%s, path=%s, column=%s, "
            "granularity=%s, lookback_periods=%s, cutoff=%s, partitions=%s)",
            table.qualified_name,
            dataset_path,
            self.reference_column,
            self._granularity,
            self._lookback_periods,
            cutoff,
            partitions,
        )

        source.apply_lower_bound(self.reference_column, cutoff)
        staging_path = target.begin_staging(dataset_path)

        try:
            write_partitioned(
                source=source,
                target=target,
                staging_path=staging_path,
                reference_column=self.reference_column,
                granularity=self._granularity,
                allowed_partitions=partition_set,
                metadata_context=self._metadata_context,
            )
            target.commit_staging(dataset_path, partitions=partitions)
        except Exception:
            target.discard_staging(dataset_path)
            raise

        logger.info(
            "Replace/Partition concluído (table=%s, partitions=%s)",
            table.qualified_name,
            len(partitions),
        )
