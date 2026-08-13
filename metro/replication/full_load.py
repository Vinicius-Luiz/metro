"""Estratégia Full Load."""

from __future__ import annotations

import logging

from metro.core.table import Table
from metro.replication.base import ReplicationStrategy
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)


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
        target_path = f"{table.qualified_name}.parquet"
        logger.info(
            "Iniciando Full Load (table=%s, path=%s)",
            table.qualified_name,
            target_path,
        )

        dataframe = source.read()
        logger.debug(
            "DataFrame carregado: rows=%s, columns=%s, column_names=%s, dtypes=%s",
            dataframe.height,
            dataframe.width,
            list(dataframe.columns),
            {name: str(dtype) for name, dtype in dataframe.schema.items()},
        )

        target.write(dataframe, target_path)
        logger.info(
            "Full Load concluído (table=%s, rows=%s)",
            table.qualified_name,
            dataframe.height,
        )
