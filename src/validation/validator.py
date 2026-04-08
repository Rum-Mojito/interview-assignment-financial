from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json

from src.schema.models import SchemaDefinition


@dataclass(frozen=True)
class ValidationResult:
    """Capture report metrics and row-level validation violations."""

    quality_report: dict[str, object]
    violations: list[dict[str, object]]


def validate_dataset(
    schema: SchemaDefinition,
    records_by_table: dict[str, list[dict[str, object]]],
) -> ValidationResult:
    table_map = schema.table_map()
    violations: list[dict[str, object]] = []

    fk_checks = _evaluate_foreign_key_integrity(
        schema=schema,
        table_map=table_map,
        records_by_table=records_by_table,
        violations=violations,
    )
    null_stats = _calculate_null_statistics(records_by_table=records_by_table)
    categorical_distribution = _calculate_categorical_distribution(records_by_table=records_by_table)

    quality_report: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fk_integrity_rate": fk_checks["fk_integrity_rate"],
        "fk_checked_rows": fk_checks["fk_checked_rows"],
        "fk_failed_rows": fk_checks["fk_failed_rows"],
        "null_statistics": null_stats,
        "categorical_distribution": categorical_distribution,
    }
    return ValidationResult(quality_report=quality_report, violations=violations)


def _evaluate_foreign_key_integrity(
    schema: SchemaDefinition,
    table_map: dict[str, object],
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    total_fk_checked_rows = 0
    total_fk_failed_rows = 0

    for table in schema.tables:
        table_records = records_by_table.get(table.name, [])
        for fk_column in table.foreign_key_columns():
            foreign_key = fk_column.foreign_key
            if foreign_key is None:
                continue
            parent_table = table_map[foreign_key.referenced_table]
            parent_pk_column = parent_table.primary_key_column()
            parent_key_set = {
                str(parent_record[parent_pk_column])
                for parent_record in records_by_table.get(parent_table.name, [])
            }
            for row_index, row in enumerate(table_records, start=1):
                total_fk_checked_rows += 1
                fk_value = str(row[fk_column.name])
                if fk_value in parent_key_set:
                    continue
                total_fk_failed_rows += 1
                violations.append(
                    {
                        "table_name": table.name,
                        "row_index": row_index,
                        "rule_id": "FK_INTEGRITY",
                        "column_name": fk_column.name,
                        "column_value": fk_value,
                        "violation_message": (
                            f"foreign key value not found in parent table "
                            f"{foreign_key.referenced_table}.{foreign_key.referenced_column}"
                        ),
                    }
                )

    integrity_rate = 1.0
    if total_fk_checked_rows > 0:
        integrity_rate = (total_fk_checked_rows - total_fk_failed_rows) / total_fk_checked_rows
    return {
        "fk_integrity_rate": round(integrity_rate, 6),
        "fk_checked_rows": total_fk_checked_rows,
        "fk_failed_rows": total_fk_failed_rows,
    }


def _calculate_null_statistics(records_by_table: dict[str, list[dict[str, object]]]) -> dict[str, dict[str, int]]:
    null_statistics: dict[str, dict[str, int]] = {}
    for table_name, records in records_by_table.items():
        table_null_counts: dict[str, int] = {}
        for row in records:
            for column_name, value in row.items():
                if value is None or value == "":
                    table_null_counts[column_name] = table_null_counts.get(column_name, 0) + 1
        null_statistics[table_name] = table_null_counts
    return null_statistics


def _calculate_categorical_distribution(
    records_by_table: dict[str, list[dict[str, object]]]
) -> dict[str, dict[str, dict[str, int]]]:
    distribution: dict[str, dict[str, dict[str, int]]] = {}
    for table_name, records in records_by_table.items():
        table_distribution: dict[str, dict[str, int]] = {}
        if not records:
            distribution[table_name] = table_distribution
            continue

        column_names = list(records[0].keys())
        for column_name in column_names:
            observed_values = [row[column_name] for row in records]
            distinct_values = {str(value) for value in observed_values}
            if len(distinct_values) > 20:
                continue
            if _looks_like_json_column(values=observed_values):
                continue

            value_counts: dict[str, int] = {}
            for value in observed_values:
                value_key = str(value)
                value_counts[value_key] = value_counts.get(value_key, 0) + 1
            table_distribution[column_name] = value_counts
        distribution[table_name] = table_distribution
    return distribution


def _looks_like_json_column(values: list[object]) -> bool:
    first_value = values[0]
    if not isinstance(first_value, str):
        return False
    if not first_value.startswith("{"):
        return False
    try:
        json.loads(first_value)
    except json.JSONDecodeError:
        return False
    return True
