"""Dataset lógico replicado pelo METRO."""

from __future__ import annotations

from pydantic import BaseModel, Field

from metro.core.column import Column


class Table(BaseModel):
    """Representa o dataset lógico a ser replicado.

    Em fontes SQL, `schema_name` e `name` identificam a tabela.
    Em fontes NoSQL, `name` pode representar uma collection e `schema_name` permanece opcional.
    """

    name: str = Field(..., min_length=1, description="Nome da tabela ou collection")
    schema_name: str | None = Field(
        default=None,
        description="Schema SQL, quando aplicável",
    )
    columns: list[Column] = Field(
        default_factory=list,
        description="Metadados das colunas do dataset",
    )

    @property
    def qualified_name(self) -> str:
        """Retorna o nome qualificado `schema.name` ou apenas `name`."""
        if self.schema_name:
            return f"{self.schema_name}.{self.name}"
        return self.name
