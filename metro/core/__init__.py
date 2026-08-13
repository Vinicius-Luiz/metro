"""Contratos e entidades do domínio core do METRO."""

from metro.core.column import Column
from metro.core.endpoint import Endpoint
from metro.core.table import Table
from metro.core.task import (
    ReplicationConfig,
    SourceConfig,
    TargetConfig,
    Task,
    TaskValidationError,
)

__all__ = [
    "Column",
    "Endpoint",
    "ReplicationConfig",
    "SourceConfig",
    "Table",
    "TargetConfig",
    "Task",
    "TaskValidationError",
]
