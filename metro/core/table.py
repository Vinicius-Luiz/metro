"""Dataset lógico replicado pelo METRO."""

from __future__ import annotations

from pydantic import BaseModel, Field

from metro.core.column import Column


class Table(BaseModel):
    """Representa o dataset lógico a ser replicado.

    Em fontes SQL, `schema_name` e `name` identificam a tabela na origem.
    Em fontes NoSQL, `name` pode representar uma collection e `schema_name` permanece opcional.
    `target_schema_name` e `target_name` definem a pasta do dataset no Target.
    """

    name: str = Field(..., min_length=1, description="Nome da tabela ou collection")
    schema_name: str | None = Field(
        default=None,
        description="Schema SQL, quando aplicável",
    )
    target_schema_name: str = Field(
        ...,
        min_length=1,
        description="Schema/pasta do dataset no Target",
    )
    target_name: str = Field(
        ...,
        min_length=1,
        description="Nome da tabela no Target",
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

    @property
    def target_dataset_path(self) -> str:
        """Caminho do dataset no Target: `{target_schema_name}/{target_name}`."""
        return f"{self.target_schema_name}/{self.target_name}"
