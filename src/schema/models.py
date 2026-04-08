from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


NormalizedType = Literal["string", "integer", "decimal", "timestamp", "json", "xml", "text", "categorical"]


@dataclass(frozen=True)
class ForeignKey:
    """Describe a FK link from current column to parent column."""

    referenced_table: str
    referenced_column: str


@dataclass(frozen=True)
class ColumnDefinition:
    """Store column metadata in internal schema DSL."""

    name: str
    raw_type: str
    normalized_type: NormalizedType
    is_primary_key: bool = False
    foreign_key: ForeignKey | None = None
    allowed_values: tuple[str, ...] | None = None


@dataclass(frozen=True)
class TableDefinition:
    """Store table metadata in internal schema DSL."""

    name: str
    columns: list[ColumnDefinition] = field(default_factory=list)

    def primary_key_column(self) -> str:
        for column in self.columns:
            if column.is_primary_key:
                return column.name
        raise ValueError(f"table '{self.name}' has no primary key column")

    def foreign_key_columns(self) -> list[ColumnDefinition]:
        return [column for column in self.columns if column.foreign_key is not None]


@dataclass(frozen=True)
class SchemaDefinition:
    """Store normalized schema with a table lookup helper."""

    tables: list[TableDefinition]

    def table_map(self) -> dict[str, TableDefinition]:
        return {table.name: table for table in self.tables}
