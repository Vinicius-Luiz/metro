"""Colunas de metadados opcionais adicionadas na materialização Parquet."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import polars as pl
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

DEFAULT_DATA_INGESTAO_COLUMN = "$data_ingestao"
DEFAULT_BANCO_ORIGEM_COLUMN = "$banco_origem"


class MetadataColumnConfig(BaseModel):
    """Configuração de uma coluna de metadado individual."""

    enabled: bool = True
    column_name: str | None = None


class MetadataColumnsConfig(BaseModel):
    """Configuração das colunas de metadados disponíveis."""

    data_ingestao: MetadataColumnConfig = Field(
        default_factory=MetadataColumnConfig,
    )
    banco_origem: MetadataColumnConfig = Field(
        default_factory=MetadataColumnConfig,
    )


class MetadataConfig(BaseModel):
    """Configuração de metadados da tarefa de replicação."""

    enabled: bool = True
    columns: MetadataColumnsConfig = Field(default_factory=MetadataColumnsConfig)

    def is_active(self) -> bool:
        """Indica se ao menos uma coluna de metadado será aplicada."""
        if not self.enabled:
            return False
        return (
            self.columns.data_ingestao.enabled
            or self.columns.banco_origem.enabled
        )


class MetadataValidationError(ValueError):
    """Erro de validação na aplicação de metadados."""


@dataclass(frozen=True)
class MetadataContext:
    """Contexto de execução para enriquecimento de metadados."""

    config: MetadataConfig
    source_table_qualified_name: str
    execution_timestamp: datetime


def _resolve_column_name(
    column_config: MetadataColumnConfig,
    default_name: str,
) -> str:
    """Resolve o nome efetivo da coluna de metadado."""
    if column_config.column_name is None:
        return default_name
    normalized = column_config.column_name.strip()
    if not normalized:
        raise MetadataValidationError(
            f"column_name de metadado não pode ser vazio (padrão: {default_name})"
        )
    return normalized


def _validate_no_column_conflicts(
    dataframe: pl.DataFrame,
    column_names: list[str],
) -> None:
    """Garante que colunas de metadado não conflitam com colunas existentes."""
    existing = set(dataframe.columns)
    conflicts = [name for name in column_names if name in existing]
    if conflicts:
        raise MetadataValidationError(
            "Colunas de metadado conflitam com colunas existentes no dataset: "
            f"{conflicts}. Colunas atuais: {list(dataframe.columns)}"
        )


def add_metadata_columns(
    dataframe: pl.DataFrame,
    metadata_context: MetadataContext | None,
) -> pl.DataFrame:
    """Adiciona colunas de metadados configuradas ao DataFrame.

    Retorna o DataFrame inalterado quando metadados estão desabilitados ou
    quando o contexto não foi informado.
    """
    if metadata_context is None:
        return dataframe

    config = metadata_context.config
    if not config.is_active():
        return dataframe

    columns_to_add: list[tuple[str, pl.Expr]] = []

    if config.columns.data_ingestao.enabled:
        column_name = _resolve_column_name(
            config.columns.data_ingestao,
            DEFAULT_DATA_INGESTAO_COLUMN,
        )
        timestamp_value = metadata_context.execution_timestamp.replace(
            microsecond=0,
        ).isoformat(timespec="seconds")
        columns_to_add.append(
            (column_name, pl.lit(timestamp_value).alias(column_name)),
        )

    if config.columns.banco_origem.enabled:
        column_name = _resolve_column_name(
            config.columns.banco_origem,
            DEFAULT_BANCO_ORIGEM_COLUMN,
        )
        source_name = metadata_context.source_table_qualified_name
        columns_to_add.append(
            (column_name, pl.lit(source_name).alias(column_name)),
        )

    if not columns_to_add:
        return dataframe

    _validate_no_column_conflicts(
        dataframe,
        [name for name, _ in columns_to_add],
    )

    enriched = dataframe.with_columns([expr for _, expr in columns_to_add])
    logger.debug(
        "Metadados aplicados (columns=%s, rows=%s)",
        [name for name, _ in columns_to_add],
        enriched.height,
    )
    return enriched
