"""Interface base de Replication Strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod

from metro.core.table import Table
from metro.sources.base import SourceEndpoint
from metro.targets.base import TargetEndpoint


class ReplicationStrategy(ABC):
    """Contrato para estratégias de replicação do METRO.

    A Strategy coordena Source e Target via suas interfaces.
    Não implementa lógica específica de tecnologia de Endpoint.
    """

    def __init__(
        self,
        *,
        mode: str,
        strategy_type: str | None = None,
        method: str | None = None,
        reference_column: str | None = None,
        aggregation: str | None = None,
        partition_type: str | None = None,
    ) -> None:
        self._mode = mode
        self._strategy_type = strategy_type
        self._method = method
        self._reference_column = reference_column
        self._aggregation = aggregation
        self._partition_type = partition_type
        self._rows_processed = 0

    @property
    def rows_processed(self) -> int:
        """Total de linhas processadas na última execução."""
        return self._rows_processed

    @property
    def mode(self) -> str:
        """Modo de replicação (`full_load` ou `incremental`)."""
        return self._mode

    @property
    def strategy_type(self) -> str | None:
        """Tipo da estratégia incremental (`replace`, `append`), quando aplicável."""
        return self._strategy_type

    @property
    def method(self) -> str | None:
        """Método implícito da estratégia (`partition`, `max_value`), quando aplicável."""
        return self._method

    @property
    def reference_column(self) -> str | None:
        """Coluna de referência temporal/valor para partição ou watermark."""
        return self._reference_column

    @property
    def aggregation(self) -> str | None:
        """Agregação do watermark Append (ex.: MAX), quando aplicável."""
        return self._aggregation

    @property
    def partition_type(self) -> str | None:
        """Granularidade Hive (`year`, `month`, `day`), quando aplicável."""
        return self._partition_type

    @abstractmethod
    def execute(
        self,
        source: SourceEndpoint,
        target: TargetEndpoint,
        table: Table,
    ) -> None:
        """Executa a replicação coordenando Source, Target e Table."""
