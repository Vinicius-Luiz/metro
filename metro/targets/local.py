"""Target Endpoint Local (filesystem)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any

import polars as pl

from metro.secrets.base import SecretProvider
from metro.targets.base import TargetEndpoint

logger = logging.getLogger(__name__)

DEFAULT_BASE_PATH = "./local"


class LocalTarget(TargetEndpoint):
    """Materializa Parquet em diretório local."""

    def __init__(
        self,
        runtime: str,
        secret_provider: SecretProvider,
        chunk_size: int | None = None,
    ) -> None:
        super().__init__(runtime=runtime, chunk_size=chunk_size)
        self._secret_provider = secret_provider
        self._base_path: Path | None = None

    @property
    def base_path(self) -> Path:
        if self._base_path is None:
            raise RuntimeError(
                "LocalTarget não está conectado. Chame connect() antes de write()."
            )
        return self._base_path

    def connect(self) -> None:
        secret = self._secret_provider.get_secret(self.runtime)
        self._base_path = self._resolve_base_path(secret)
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "LocalTarget pronto (runtime=%s, base_path=%s)",
            self.runtime,
            self._base_path,
        )
        logger.debug(
            "Parâmetros Target Local | runtime=%s | chunk_size=%s | "
            "base_path=%s | secret_type=%s",
            self.runtime,
            self.chunk_size,
            self._base_path.resolve(),
            type(secret).__name__,
        )

    def disconnect(self) -> None:
        self._base_path = None
        logger.debug("LocalTarget desconectado (runtime=%s)", self.runtime)

    def write(self, dataframe: pl.DataFrame, path: str) -> None:
        destination = self.base_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Gravando Parquet em %s", destination)
        logger.debug(
            "Parquet write | path=%s | rows=%s | columns=%s | column_names=%s | "
            "dtypes=%s",
            destination,
            dataframe.height,
            dataframe.width,
            list(dataframe.columns),
            {name: str(dtype) for name, dtype in dataframe.schema.items()},
        )
        dataframe.write_parquet(destination)
        logger.debug(
            "Parquet gravado | path=%s | size_bytes=%s",
            destination,
            destination.stat().st_size if destination.exists() else None,
        )

    def delete_partition(self, path: str) -> None:
        target = self.base_path / path
        if target.is_file():
            target.unlink()
            logger.info("Arquivo removido: %s", target)
            return
        if target.is_dir():
            shutil.rmtree(target)
            logger.info("Diretório removido: %s", target)
            return
        logger.warning("Partição inexistente para remoção: %s", target)

    def _resolve_base_path(self, secret: str | dict[str, Any]) -> Path:
        if isinstance(secret, str):
            return Path(secret)

        if isinstance(secret, dict):
            base_path = secret.get("base_path", DEFAULT_BASE_PATH)
            return Path(str(base_path))

        raise TypeError(
            f"LocalTarget espera str ou dict para runtime '{self.runtime}', "
            f"obtido: {type(secret).__name__}"
        )
