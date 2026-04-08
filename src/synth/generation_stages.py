"""
Unified generation pipeline stages (Requirement 2 item 4).

Both rowwise and event_first execute the same named phases:
sample_roots → expand_children → post_adjust.

Telemetry records which tables participated in each phase for run_metadata.
"""

from __future__ import annotations

from src.schema.models import SchemaDefinition

PIPELINE_STAGE_SAMPLE_ROOTS = "sample_roots"
PIPELINE_STAGE_EXPAND_CHILDREN = "expand_children"
PIPELINE_STAGE_POST_ADJUST = "post_adjust"


def partition_root_child_tables(
    schema: SchemaDefinition,
    ordered_tables: list[str],
) -> tuple[list[str], list[str]]:
    """Tables with no FK columns are roots; others depend on parent PK pools."""

    table_map = schema.table_map()
    roots = [table_name for table_name in ordered_tables if not table_map[table_name].foreign_key_columns()]
    children = [table_name for table_name in ordered_tables if table_map[table_name].foreign_key_columns()]
    return roots, children


def stage_record(*, stage: str, pipeline_mode: str, tables: str = "", detail: str = "") -> dict[str, object]:
    payload: dict[str, object] = {"stage": stage, "pipeline_mode": pipeline_mode}
    if tables:
        payload["tables"] = tables
    if detail:
        payload["detail"] = detail
    return payload
