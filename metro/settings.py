"""Configurações de infraestrutura do METRO.

Valores podem ser sobrescritos por variáveis de ambiente com prefixo `METRO_`.
Secrets de task (connection strings, runtimes) permanecem no Secret Provider.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MetroSettings(BaseSettings):
    """Configurações globais do motor (não são secrets de task)."""

    model_config = SettingsConfigDict(
        env_prefix="METRO_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=False,
    )

    # Secret Provider
    # Provider de secrets (local por enquanto)
    secret_provider: Literal["local"] = "local"

    # Logging
    # Nível de logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # Diretório dos arquivos de log
    log_dir: Path = Path("logs")
    # Arquivo de log; se omitido: logs/<modo>/<task>_<timestamp>.log
    log_file: Path | None = None

    # Watermark API
    # URL base da API de watermark
    watermark_api_url: str = "http://localhost:8000"
    # Timeout HTTP em segundos
    watermark_api_timeout: int = Field(default=10, gt=0)

    # Query Repository
    # Diretório das queries (query_path)
    query_repository_base_dir: Path = Path(".metro/queries")

    # Local Storage
    # Fallback do Target Local quando o secret não informa base_path
    local_storage_base_path: Path = Path("./local")


settings = MetroSettings()
