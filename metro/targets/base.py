"""Interface base de Target Endpoints."""

from __future__ import annotations

from abc import abstractmethod

import polars as pl

from metro.core.endpoint import Endpoint

TEMP_DIRNAME = "_tmp"


class TargetEndpoint(Endpoint):
    """Contrato para destinos de materialização do METRO.

    Responsável por persistir datasets (Parquet) no storage de destino.
    """

    def __init__(self, runtime: str, chunk_size: int | None = None) -> None:
        super().__init__(runtime)
        self._chunk_size = chunk_size

    @property
    def chunk_size(self) -> int | None:
        """Tamanho de chunk/escrita, quando parametrizado."""
        return self._chunk_size

    def supports_batch_write(self) -> bool:
        """Indica se o Target materializa batches em múltiplos arquivos.

        A implementação padrão retorna False. Targets que escrevem um
        arquivo Parquet por path (ex.: filesystem) devem sobrescrever.
        """
        return False

    @abstractmethod
    def write(self, dataframe: pl.DataFrame, path: str) -> None:
        """Materializa o DataFrame no destino informado."""

    def delete_partition(self, path: str) -> None:
        """Remove uma partição existente no destino.

        Implementações de Replace/Partition devem sobrescrever este método.
        A implementação padrão indica que a operação não é suportada.
        """
        raise NotImplementedError(
            f"{type(self).__name__} não implementa delete_partition"
        )

    def begin_staging(self, dataset_path: str) -> str:
        """Prepara a pasta de staging (`_tmp`) e retorna o path relativo de escrita."""
        raise NotImplementedError(
            f"{type(self).__name__} não implementa begin_staging"
        )

    def commit_staging(
        self,
        dataset_path: str,
        partitions: list[str] | None = None,
    ) -> None:
        """Promove o conteúdo de `_tmp` para o destino final.

        Quando `partitions` é None, substitui o dataset inteiro.
        Quando informado, substitui apenas as subpastas de partição listadas.
        """
        raise NotImplementedError(
            f"{type(self).__name__} não implementa commit_staging"
        )

    def discard_staging(self, dataset_path: str) -> None:
        """Descarta a pasta de staging (`_tmp`) sem alterar o destino final."""
        raise NotImplementedError(
            f"{type(self).__name__} não implementa discard_staging"
        )
