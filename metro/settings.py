"""Configurações de infraestrutura do METRO.

Valores padrão vivem neste módulo e devem ser alterados editando este arquivo.
Não são sobrescritáveis por variáveis de ambiente.

O arquivo `.env` não é lido aqui: ele guarda somente credenciais do Secret
Provider (connection strings e secrets de runtime).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class MetroSettings(BaseModel):
    """Configurações globais do motor (não são secrets de task).

    Edite este arquivo diretamente para alterar configurações.
    Não use variáveis de ambiente.
    """

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

    # Logging Database API (opcional)
    # Habilitar envio de logs para API PostgreSQL
    logging_enabled: bool = True
    # URL base da API de logging (porta 8001; watermark usa 8000)
    logging_api_url: str | None = "http://localhost:8001"
    # Timeout HTTP em segundos
    logging_api_timeout: int = Field(default=10, gt=0)

    # Watermark API
    # Habilitar watermark para estratégias incrementais append
    watermark_enabled: bool = True
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
