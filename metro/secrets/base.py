"""Interface base de Secret Providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SecretProvider(ABC):
    """Resolve `runtime` para secret/configuração externa.

    Sources SQL tipicamente recebem connection string.
    Targets podem receber dict de configuração (ex.: base_path).
    """

    @abstractmethod
    def get_secret(self, runtime: str) -> str | dict[str, Any]:
        """Retorna o secret/configuração associado ao runtime."""
