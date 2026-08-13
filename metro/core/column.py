"""Metadados de coluna do dataset lógico."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Column(BaseModel):
    """Representa metadados de uma coluna do dataset.

    Não modela o valor de cada registro — apenas identidade e metadados.
    """

    name: str = Field(..., min_length=1, description="Nome da coluna")
    data_type: str = Field(..., min_length=1, description="Tipo lógico da coluna")
    nullable: bool = Field(default=True, description="Indica se a coluna aceita nulos")
