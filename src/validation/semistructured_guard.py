from __future__ import annotations

from src.infra.config_store import SYNTH_COLUMN_PROFILES, load_schema_config
from src.schema.models import SchemaDefinition


def evaluate_semistructured_coverage(
    *,
    schema: SchemaDefinition,
    semantics_profile_id: str,
) -> dict[str, object]:
    payload = load_schema_config(SYNTH_COLUMN_PROFILES)
    profiles = payload.get("profiles", {})
    selected_profile = profiles.get(semantics_profile_id, {}) if isinstance(profiles, dict) else {}
    columns_cfg = selected_profile.get("columns", {}) if isinstance(selected_profile, dict) else {}
    if not isinstance(columns_cfg, dict):
        columns_cfg = {}

    json_total = 0
    json_structured_pack_bound = 0
    json_fallback_columns: list[str] = []
    xml_total = 0
    text_total = 0
    xml_fallback_columns: list[str] = []
    text_fallback_columns: list[str] = []

    for table in schema.tables:
        for column in table.columns:
            table_column = f"{table.name}.{column.name}"
            dist_type = _distribution_type(columns_cfg=columns_cfg, table_column=table_column)
            if column.normalized_type == "json":
                json_total += 1
                if dist_type == "structured_json_object":
                    json_structured_pack_bound += 1
                else:
                    json_fallback_columns.append(table_column)
            elif column.normalized_type == "xml":
                xml_total += 1
                if dist_type is None:
                    xml_fallback_columns.append(table_column)
            elif column.normalized_type == "text":
                text_total += 1
                if dist_type is None:
                    text_fallback_columns.append(table_column)

    json_structured_rate = 1.0 if json_total == 0 else round(json_structured_pack_bound / json_total, 6)
    warnings: list[str] = []
    if json_fallback_columns:
        warnings.append(
            "json_columns_without_structured_pack: " + ",".join(sorted(json_fallback_columns))
        )
    if xml_fallback_columns:
        warnings.append(
            "xml_columns_using_legacy_fallback: " + ",".join(sorted(xml_fallback_columns))
        )
    if text_fallback_columns:
        warnings.append(
            "text_columns_using_legacy_fallback: " + ",".join(sorted(text_fallback_columns))
        )

    return {
        "semistructured_profile_id": semantics_profile_id,
        "json_total_columns": json_total,
        "json_structured_pack_bound_columns": json_structured_pack_bound,
        "json_structured_pack_bound_rate": json_structured_rate,
        "xml_total_columns": xml_total,
        "text_total_columns": text_total,
        "warnings": warnings,
    }


def _distribution_type(*, columns_cfg: dict[str, object], table_column: str) -> str | None:
    raw = columns_cfg.get(table_column)
    if not isinstance(raw, dict):
        return None
    distribution = raw.get("distribution")
    if not isinstance(distribution, dict):
        return None
    dist_type = distribution.get("type")
    return str(dist_type) if isinstance(dist_type, str) and dist_type else None

