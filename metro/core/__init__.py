"""Contratos e entidades do domínio core do METRO."""

from metro.core.endpoint import Endpoint
from metro.core.metadata import MetadataConfig, MetadataContext
from metro.core.table import Table
from metro.core.task import (
    ReplicationConfig,
    SourceConfig,
    TargetConfig,
    Task,
    TaskValidationError,
)

__all__ = [
    "Endpoint",
    "MetadataConfig",
    "MetadataContext",
    "ReplicationConfig",
    "SourceConfig",
    "Table",
    "TargetConfig",
    "Task",
    "TaskValidationError",
]
