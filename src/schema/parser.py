from __future__ import annotations

from pathlib import Path
import json
import re

from src.schema.models import ColumnDefinition, ForeignKey, SchemaDefinition, TableDefinition


CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);",
    flags=re.IGNORECASE | re.DOTALL,
)
FOREIGN_KEY_PATTERN = re.compile(
    r"FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)",
    flags=re.IGNORECASE,
)


def parse_schema(schema_path: Path) -> SchemaDefinition:
    suffix = schema_path.suffix.lower()
    if suffix == ".sql":
        return _parse_sql_schema(schema_path=schema_path)
    if suffix == ".json":
        return _parse_json_schema(schema_path=schema_path)
    raise ValueError(f"unsupported schema format: {schema_path.suffix}")


def _parse_sql_schema(schema_path: Path) -> SchemaDefinition:
    schema_text = schema_path.read_text(encoding="utf-8")
    tables: list[TableDefinition] = []

    for table_name, table_body in CREATE_TABLE_PATTERN.findall(schema_text):
        raw_lines = [line.strip().rstrip(",") for line in table_body.splitlines() if line.strip()]
        parsed_columns: list[ColumnDefinition] = []
        pending_foreign_keys: dict[str, ForeignKey] = {}

        for raw_line in raw_lines:
            foreign_key_match = FOREIGN_KEY_PATTERN.match(raw_line)
            if foreign_key_match:
                fk_column, parent_table, parent_column = foreign_key_match.groups()
                pending_foreign_keys[fk_column] = ForeignKey(
                    referenced_table=parent_table,
                    referenced_column=parent_column,
                )
                continue

            line_parts = raw_line.split()
            column_name = line_parts[0]
            raw_type = line_parts[1] if len(line_parts) > 1 else "TEXT"
            is_primary_key = "PRIMARY KEY" in raw_line.upper()
            parsed_columns.append(
                ColumnDefinition(
                    name=column_name,
                    raw_type=raw_type,
                    normalized_type=_normalize_column_type(raw_type=raw_type, column_name=column_name),
                    is_primary_key=is_primary_key,
                )
            )

        columns_with_fk: list[ColumnDefinition] = []
        for parsed_column in parsed_columns:
            foreign_key = pending_foreign_keys.get(parsed_column.name)
            columns_with_fk.append(
                ColumnDefinition(
                    name=parsed_column.name,
                    raw_type=parsed_column.raw_type,
                    normalized_type=parsed_column.normalized_type,
                    is_primary_key=parsed_column.is_primary_key,
                    foreign_key=foreign_key,
                )
            )

        tables.append(TableDefinition(name=table_name, columns=columns_with_fk))

    if not tables:
        raise ValueError("no CREATE TABLE blocks parsed from SQL schema")
    return SchemaDefinition(tables=tables)


def _parse_json_schema(schema_path: Path) -> SchemaDefinition:
    schema_payload = json.loads(schema_path.read_text(encoding="utf-8"))
    parsed_tables: list[TableDefinition] = []

    for table_payload in schema_payload.get("tables", []):
        table_name = str(table_payload["name"])
        columns_payload = table_payload.get("columns", [])
        columns: list[ColumnDefinition] = []

        for column_payload in columns_payload:
            column_name = str(column_payload["name"])
            raw_type = str(column_payload.get("type", "string"))
            columns.append(
                ColumnDefinition(
                    name=column_name,
                    raw_type=raw_type,
                    normalized_type=_normalize_column_type(raw_type=raw_type, column_name=column_name),
                    is_primary_key=column_name.endswith("_id"),
                )
            )

        parsed_tables.append(TableDefinition(name=table_name, columns=columns))

    if not parsed_tables:
        raise ValueError("json schema has no table definitions")
    return SchemaDefinition(tables=parsed_tables)


def _normalize_column_type(raw_type: str, column_name: str) -> str:
    normalized = raw_type.lower()
    if "json" in normalized:
        return "json"
    if "xml" in normalized:
        return "xml"
    if "time" in normalized or "date" in normalized:
        return "timestamp"
    if "int" in normalized:
        return "integer"
    if "num" in normalized or "decimal" in normalized or "float" in normalized:
        return "decimal"
    if "type" in column_name.lower() or "country" in column_name.lower() or "currency" in column_name.lower():
        return "categorical"
    if column_name.lower().endswith("_text") or "note_text" in column_name.lower():
        return "text"
    return "string"
