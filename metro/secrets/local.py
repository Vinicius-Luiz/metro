"""Secret Provider local baseado em variáveis de ambiente (.env)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from metro.secrets.base import SecretProvider


class SecretNotFoundError(LookupError):
    """Secret/configuração não encontrado para o runtime informado."""


class LocalSecretProvider(SecretProvider):
    """Simula AWS Secrets Manager via `.env` local.

    Convenção:
    - connection string: `METRO_{RUNTIME}`
    - config dict: `METRO_{RUNTIME}_{FIELD}`

    Exemplos:
    - runtime=`customer_database` → `METRO_CUSTOMER_DATABASE`
    - runtime=`development_storage` → `METRO_DEVELOPMENT_STORAGE_BASE_PATH`
    """

    def __init__(self, env_file: str | Path | None = None) -> None:
        dotenv_path = Path(env_file) if env_file else Path.cwd() / ".env"
        load_dotenv(dotenv_path=dotenv_path, override=False)

    def get_secret(self, runtime: str) -> str | dict[str, Any]:
        if not runtime or not runtime.strip():
            raise ValueError("runtime deve ser um identificador não vazio")

        prefix = f"METRO_{_to_env_key(runtime)}"
        direct_value = os.getenv(prefix)
        if direct_value is not None and direct_value != "":
            return direct_value

        config: dict[str, Any] = {}
        nested_prefix = f"{prefix}_"
        for key, value in os.environ.items():
            if key.startswith(nested_prefix) and value != "":
                field_name = key[len(nested_prefix) :].lower()
                config[field_name] = value

        if config:
            return config

        raise SecretNotFoundError(
            f"Secret não encontrado para runtime '{runtime}'. "
            f"Esperado {prefix} ou {prefix}_*"
        )


def _to_env_key(runtime: str) -> str:
    """Converte runtime para chave UPPER_SNAKE_CASE."""
    return runtime.strip().replace("-", "_").upper()
