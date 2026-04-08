from __future__ import annotations

from src.schema.knowledge_graph import load_compiled_relation_patterns
from src.schema.models import ColumnDefinition, ForeignKey, SchemaDefinition, TableDefinition


def infer_foreign_keys(schema: SchemaDefinition) -> SchemaDefinition:
    """Infer FK links using relation patterns plus ID-name heuristics."""

    relation_patterns = _load_relation_patterns()
    table_map = schema.table_map()
    updated_tables: list[TableDefinition] = []
    for table in schema.tables:
        updated_columns: list[ColumnDefinition] = []
        for column in table.columns:
            if column.foreign_key is not None:
                updated_columns.append(column)
                continue

            inferred_fk = _infer_fk_for_column(
                table_name=table.name,
                column_name=column.name,
                table_map=table_map,
                relation_patterns=relation_patterns,
            )
            updated_columns.append(
                ColumnDefinition(
                    name=column.name,
                    raw_type=column.raw_type,
                    normalized_type=column.normalized_type,
                    is_primary_key=column.is_primary_key,
                    foreign_key=inferred_fk,
                    allowed_values=column.allowed_values,
                )
            )
        updated_tables.append(TableDefinition(name=table.name, columns=updated_columns))
    return SchemaDefinition(tables=updated_tables)


def _infer_fk_for_column(
    table_name: str,
    column_name: str,
    table_map: dict[str, TableDefinition],
    relation_patterns: list[dict[str, str]],
) -> ForeignKey | None:
    normalized_table_name = table_name.lower()
    normalized_column_name = column_name.lower()
    if normalized_column_name == _primary_key_name_for_table_name(normalized_table_name):
        return None

    for relation_pattern in relation_patterns:
        child_table = relation_pattern["child_table"].lower()
        fk_column = relation_pattern["child_column"].lower()
        parent_table = relation_pattern["parent_table"].lower()
        parent_column = relation_pattern["parent_column"]
        if normalized_table_name == child_table and normalized_column_name == fk_column:
            if parent_table in table_map:
                return ForeignKey(referenced_table=parent_table, referenced_column=parent_column)

    if not normalized_column_name.endswith("_id"):
        return None

    candidate_parent_table_names = _candidate_parent_tables_from_id_column(
        normalized_column_name=normalized_column_name
    )
    for candidate_parent_table_name in candidate_parent_table_names:
        parent_table = table_map.get(candidate_parent_table_name)
        if parent_table is None:
            continue
        parent_pk_column = parent_table.primary_key_column()
        if parent_pk_column.lower() == normalized_column_name:
            if parent_table.name.lower() == normalized_table_name:
                continue
            return ForeignKey(referenced_table=parent_table.name, referenced_column=parent_pk_column)
    return None


def _candidate_parent_tables_from_id_column(normalized_column_name: str) -> list[str]:
    id_prefix = normalized_column_name[: -len("_id")]
    candidates = [id_prefix, f"{id_prefix}s"]
    if id_prefix.endswith("y"):
        candidates.append(f"{id_prefix[:-1]}ies")
    return candidates


def _primary_key_name_for_table_name(normalized_table_name: str) -> str:
    if normalized_table_name.endswith("ies"):
        singular_name = f"{normalized_table_name[:-3]}y"
    elif normalized_table_name.endswith("s"):
        singular_name = normalized_table_name[:-1]
    else:
        singular_name = normalized_table_name
    return f"{singular_name}_id"


def _load_relation_patterns() -> list[dict[str, str]]:
    return load_compiled_relation_patterns()
