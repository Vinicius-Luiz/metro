"""Contrato de tarefa de replicação do METRO."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from metro.core.metadata import MetadataConfig
from metro.core.table import Table

KNOWN_SOURCE_TYPES = frozenset({"postgresql", "sqlserver", "oracle", "mongodb"})
KNOWN_TARGET_TYPES = frozenset({"s3", "local"})
REPLICATION_MODES = frozenset({"full_load", "incremental"})
PARTITION_TYPES = frozenset({"year", "month", "day"})


class TaskValidationError(ValueError):
    """Erro de validação do contrato de tarefa de replicação."""


class SourceConfig(BaseModel):
    """Configuração do Source Endpoint no contrato YAML."""

    type: str = Field(..., min_length=1)
    runtime: str = Field(..., min_length=1)
    query_path: str | None = None
    chunk_size: int | None = Field(default=None, gt=0)

    @field_validator("type")
    @classmethod
    def type_must_be_known(cls, value: str) -> str:
        """Normaliza e valida `source.type` contra os tipos conhecidos."""
        normalized = value.strip().lower()
        if normalized not in KNOWN_SOURCE_TYPES:
            raise TaskValidationError(
                f"source.type inválido: '{value}'. "
                f"Tipos conhecidos: {sorted(KNOWN_SOURCE_TYPES)}"
            )
        return normalized


class TargetConfig(BaseModel):
    """Configuração do Target Endpoint no contrato YAML."""

    type: str = Field(..., min_length=1)
    runtime: str = Field(..., min_length=1)
    chunk_size: int | None = Field(default=None, gt=0)

    @field_validator("type")
    @classmethod
    def type_must_be_known(cls, value: str) -> str:
        """Normaliza e valida `target.type` contra os tipos conhecidos."""
        normalized = value.strip().lower()
        if normalized not in KNOWN_TARGET_TYPES:
            raise TaskValidationError(
                f"target.type inválido: '{value}'. "
                f"Tipos conhecidos: {sorted(KNOWN_TARGET_TYPES)}"
            )
        return normalized


class PartitionConfig(BaseModel):
    """Configuração de partição Hive (opcional em full_load/append; obrigatório em replace)."""

    type: str = Field(..., min_length=1)
    reference_column: str | None = Field(default=None, min_length=1)

    @field_validator("type")
    @classmethod
    def type_must_be_known(cls, value: str) -> str:
        """Normaliza e valida `partition.type` (`year`/`month`/`day`)."""
        normalized = value.strip().lower()
        if normalized not in PARTITION_TYPES:
            raise TaskValidationError(
                f"partition.type inválido: '{value}'. "
                f"Tipos conhecidos: {sorted(PARTITION_TYPES)}"
            )
        return normalized


class StrategyConfig(BaseModel):
    """Configuração da estratégia incremental.

    O método é implícito pelo type: replace → partition; append → max_value.
    Particionamento Hive fica em `replication.partition`, não aqui.
    """

    type: Literal["append", "replace"]
    reference_column: str = Field(..., min_length=1)
    aggregation: str | None = None
    lookback_periods: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_strategy_shape(self) -> StrategyConfig:
        """Garante campos obrigatórios de `replace` (lookback)."""
        if self.type == "replace" and self.lookback_periods is None:
            raise TaskValidationError(
                "strategy.lookback_periods é obrigatório quando type='replace'"
            )
        if self.type == "append" and self.lookback_periods is not None:
            raise TaskValidationError(
                "strategy.lookback_periods não deve ser informado para append"
            )
        return self


class ReplicationConfig(BaseModel):
    """Configuração do modo e estratégia de replicação."""

    mode: Literal["full_load", "incremental"]
    strategy: StrategyConfig | None = None
    partition: PartitionConfig | None = None

    @model_validator(mode="after")
    def validate_mode_strategy(self) -> ReplicationConfig:
        """Valida coerência entre `mode`, `strategy` e `partition`."""
        if self.mode == "incremental" and self.strategy is None:
            raise TaskValidationError(
                "replication.strategy é obrigatório quando mode='incremental'"
            )
        if self.mode == "full_load" and self.strategy is not None:
            raise TaskValidationError(
                "replication.strategy não deve ser informado quando mode='full_load'"
            )
        if (
            self.mode == "incremental"
            and self.strategy is not None
            and self.strategy.type == "replace"
            and self.partition is None
        ):
            raise TaskValidationError(
                "replication.partition é obrigatório quando strategy.type='replace'"
            )
        if self.partition is not None and (
            self.partition.reference_column is None
            or not self.partition.reference_column.strip()
        ):
            raise TaskValidationError(
                "replication.partition.reference_column é obrigatório "
                "quando partition está informado"
            )
        return self


class Task(BaseModel):
    """Tarefa de replicação descrita pelo contrato YAML.

    Agrega Table, Source, Target e Replication sem armazenar secrets,
    conteúdo de queries ou configuração de infraestrutura do ambiente.
    """

    table: Table
    source: SourceConfig
    target: TargetConfig
    replication: ReplicationConfig
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)

    def validate(self) -> Task:
        """Valida o contrato e retorna a própria instância.

        A validação estrutural já ocorre na construção do modelo.
        Este método existe como ponto explícito de verificação do domínio.
        """
        return self

    @classmethod
    def from_yaml(cls, path: str | Path) -> Task:
        """Carrega e valida uma tarefa a partir de um arquivo YAML."""
        yaml_path = Path(path)
        if not yaml_path.is_file():
            raise TaskValidationError(f"Arquivo YAML não encontrado: {yaml_path}")

        try:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise TaskValidationError(
                f"YAML inválido em {yaml_path}: {exc}"
            ) from exc

        if not isinstance(raw, dict):
            raise TaskValidationError(
                f"Contrato YAML deve ser um mapeamento, obtido: {type(raw).__name__}"
            )

        try:
            return cls.model_validate(raw)
        except Exception as exc:
            raise TaskValidationError(
                f"Contrato inválido em {yaml_path}: {exc}"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        """Serializa a tarefa para dicionário compatível com o contrato YAML."""
        return self.model_dump(exclude_none=True)
