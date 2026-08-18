"""Target Endpoint Local (filesystem)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any

import polars as pl

from metro.secrets.base import SecretProvider
from metro.settings import settings
from metro.targets.base import TEMP_DIRNAME, TargetEndpoint

logger = logging.getLogger(__name__)


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
        """Diretório raiz do Target após `connect()`."""
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

    def supports_batch_write(self) -> bool:
        return True

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
        logger.debug("Partição inexistente para remoção: %s", target)

    def begin_staging(self, dataset_path: str) -> str:
        staging_rel = f"{dataset_path}/{TEMP_DIRNAME}"
        staging_abs = self.base_path / staging_rel
        if staging_abs.exists():
            shutil.rmtree(staging_abs)
            logger.info("Staging residual removido: %s", staging_abs)
        staging_abs.mkdir(parents=True, exist_ok=True)
        logger.info("Staging iniciado: %s", staging_abs)
        return staging_rel

    def commit_staging(
        self,
        dataset_path: str,
        partitions: list[str] | None = None,
    ) -> None:
        dataset_abs = self.base_path / dataset_path
        staging_abs = dataset_abs / TEMP_DIRNAME
        if not staging_abs.exists():
            raise RuntimeError(
                f"Staging inexistente para commit: {staging_abs}"
            )

        dataset_abs.mkdir(parents=True, exist_ok=True)

        if partitions is None:
            self._commit_full_dataset(dataset_abs, staging_abs)
        else:
            self._commit_partitions(dataset_abs, staging_abs, partitions)

        if staging_abs.exists():
            shutil.rmtree(staging_abs)
        logger.info("Staging commitado (dataset=%s)", dataset_path)

    def discard_staging(self, dataset_path: str) -> None:
        staging_abs = self.base_path / dataset_path / TEMP_DIRNAME
        if staging_abs.exists():
            shutil.rmtree(staging_abs)
            logger.info("Staging descartado: %s", staging_abs)
            return
        logger.debug("Staging inexistente para descarte: %s", staging_abs)

    def commit_append_staging(self, dataset_path: str) -> None:
        dataset_abs = self.base_path / dataset_path
        staging_abs = dataset_abs / TEMP_DIRNAME
        if not staging_abs.exists():
            raise RuntimeError(
                f"Staging inexistente para append commit: {staging_abs}"
            )

        dataset_abs.mkdir(parents=True, exist_ok=True)
        promoted = 0

        for child in sorted(staging_abs.iterdir()):
            if child.is_file():
                next_index = _next_part_index(dataset_abs)
                destination = dataset_abs / f"part_{next_index:04d}.parquet"
                shutil.move(str(child), str(destination))
                logger.info("Arquivo append promovido: %s", destination)
                promoted += 1
                continue

            if not child.is_dir():
                continue

            # Partição Hive (ex.: year=2026/)
            partition_dest = dataset_abs / child.name
            partition_dest.mkdir(parents=True, exist_ok=True)
            next_index = _next_part_index(partition_dest)
            for part_file in sorted(child.glob("part_*.parquet")):
                destination = partition_dest / f"part_{next_index:04d}.parquet"
                shutil.move(str(part_file), str(destination))
                logger.info("Arquivo append promovido: %s", destination)
                next_index += 1
                promoted += 1

        if staging_abs.exists():
            shutil.rmtree(staging_abs)
        logger.info(
            "Staging append commitado (dataset=%s, files=%s)",
            dataset_path,
            promoted,
        )

    def _commit_full_dataset(self, dataset_abs: Path, staging_abs: Path) -> None:
        """Substitui o dataset inteiro pelo conteúdo do staging."""
        for child in list(dataset_abs.iterdir()):
            if child.name == TEMP_DIRNAME:
                continue
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)

        for child in list(staging_abs.iterdir()):
            destination = dataset_abs / child.name
            shutil.move(str(child), str(destination))

    def _commit_partitions(
        self,
        dataset_abs: Path,
        staging_abs: Path,
        partitions: list[str],
    ) -> None:
        """Promove apenas as partições listadas do staging para o destino final."""
        for partition in partitions:
            final_part = dataset_abs / partition
            staged_part = staging_abs / partition

            if final_part.exists():
                if final_part.is_file():
                    final_part.unlink()
                else:
                    shutil.rmtree(final_part)

            if staged_part.exists():
                final_part.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(staged_part), str(final_part))
                logger.info("Partição promovida: %s", final_part)
            else:
                logger.info(
                    "Partição removida (sem dados no staging): %s",
                    final_part,
                )

    def _resolve_base_path(self, secret: str | dict[str, Any]) -> Path:
        """Resolve `base_path` a partir de string ou dict do Secret Provider."""
        if isinstance(secret, str):
            return Path(secret)

        base_path = secret.get("base_path", settings.local_storage_base_path)
        return Path(str(base_path))


def _next_part_index(dataset_abs: Path) -> int:
    """Retorna o próximo índice `part_NNNN` disponível no dataset."""
    max_index = 0
    for child in dataset_abs.glob("part_*.parquet"):
        stem = child.stem  # part_0001
        if not stem.startswith("part_"):
            continue
        suffix = stem[5:]
        if suffix.isdigit():
            max_index = max(max_index, int(suffix))
    return max_index + 1
