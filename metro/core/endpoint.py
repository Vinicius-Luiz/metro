"""Interface base compartilhada entre Source e Target Endpoints."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Endpoint(ABC):
    """Contrato comum para Endpoints do METRO.

    O `runtime` é o identificador do secret/configuração externa utilizado
    para conectar ao Endpoint. Não contém credenciais.
    """

    def __init__(self, runtime: str) -> None:
        if not runtime or not runtime.strip():
            raise ValueError("runtime deve ser um identificador não vazio")
        self._runtime = runtime.strip()

    @property
    def runtime(self) -> str:
        """Identificador do secret/configuração externa do Endpoint."""
        return self._runtime

    @abstractmethod
    def connect(self) -> None:
        """Estabelece a conexão com o Endpoint."""

    @abstractmethod
    def disconnect(self) -> None:
        """Encerra a conexão com o Endpoint."""

    def __enter__(self) -> Endpoint:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()
