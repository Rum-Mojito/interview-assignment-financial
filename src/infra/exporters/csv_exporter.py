from __future__ import annotations

from pathlib import Path
import csv
import json


def export_synthetic_data_csv(
    records_by_table: dict[str, list[dict[str, object]]],
    synthetic_data_dir: Path,
) -> None:
    synthetic_data_dir.mkdir(parents=True, exist_ok=True)
    for table_name, records in records_by_table.items():
        output_path = synthetic_data_dir / f"{table_name}.csv"
        if not records:
            output_path.write_text("", encoding="utf-8")
            continue

        field_names = list(records[0].keys())
        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=field_names)
            writer.writeheader()
            for record in records:
                writer.writerow(record)


def export_quality_report(quality_report: dict[str, object], output_dir: Path) -> None:
    output_path = output_dir / "quality_report.json"
    output_path.write_text(json.dumps(quality_report, indent=2, ensure_ascii=True), encoding="utf-8")


def export_validation_violations(violations: list[dict[str, object]], output_dir: Path) -> None:
    output_path = output_dir / "validation_violations.csv"
    field_names = [
        "table_name",
        "row_index",
        "rule_id",
        "column_name",
        "column_value",
        "violation_message",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=field_names)
        writer.writeheader()
        for violation in violations:
            writer.writerow(violation)


def export_rule_violations(violations: list[dict[str, object]], output_dir: Path) -> None:
    output_path = output_dir / "rule_violations.csv"
    field_names = [
        "severity",
        "rule_id",
        "table_name",
        "row_index",
        "column_name",
        "column_value",
        "violation_message",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=field_names)
        writer.writeheader()
        for violation in violations:
            writer.writerow(violation)


def export_bad_case_injections(injection_logs: list[dict[str, object]], output_dir: Path) -> None:
    output_path = output_dir / "bad_case_injections.csv"
    field_names = [
        "table_name",
        "row_index",
        "column_name",
        "old_value",
        "new_value",
        "injection_type",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=field_names)
        writer.writeheader()
        for injection_log in injection_logs:
            writer.writerow(injection_log)


def export_anomaly_data(anomaly_rows: list[dict[str, object]], output_dir: Path) -> None:
    output_path = output_dir / "anomaly_data.csv"
    field_names = ["table_name", "row_index", "record_json"]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=field_names)
        writer.writeheader()
        for anomaly_row in anomaly_rows:
            writer.writerow(anomaly_row)
