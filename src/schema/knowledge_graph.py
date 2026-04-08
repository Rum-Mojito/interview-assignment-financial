"""
Concept relation graph config (`concept_relation_graph.json`): `nodes` (concepts) and `edges` (parent→child FK semantics).

`compile_edges_to_relation_patterns` produces the legacy relation list shape used by
`infer_foreign_keys` and concept dependency expansion. Physical table names default from
`concepts.json` `default_table_name` unless overridden on an edge.
"""

from __future__ import annotations

from typing import Any


def compile_edges_to_relation_patterns(
    concepts_payload: dict[str, object],
    kg_payload: dict[str, object],
) -> list[dict[str, str]]:
    """
    Compile concept_relation_graph.json edges into relation rows compatible with relation_inference
    and scenario_generator closure (parent_concept, child_concept, parent_table, child_table, …).
    """

    concept_by_id: dict[str, dict[str, Any]] = {
        str(c["concept_id"]): c for c in concepts_payload.get("concepts", []) if isinstance(c, dict)
    }
    edges_obj = kg_payload.get("edges", [])
    if not isinstance(edges_obj, list):
        return []
    out: list[dict[str, str]] = []
    for edge in edges_obj:
        if not isinstance(edge, dict):
            continue
        from_c = str(edge.get("from_concept", "")).strip()
        to_c = str(edge.get("to_concept", "")).strip()
        if not from_c or not to_c:
            continue
        if from_c not in concept_by_id or to_c not in concept_by_id:
            continue
        fk_raw = edge.get("fk")
        fk = fk_raw if isinstance(fk_raw, dict) else {}
        child_col = str(fk.get("child_column", "")).strip()
        parent_col = str(fk.get("parent_column", "")).strip()
        if not child_col or not parent_col:
            continue
        parent_table = str(edge.get("parent_table", "")).strip()
        child_table = str(edge.get("child_table", "")).strip()
        if not parent_table:
            parent_table = str(concept_by_id[from_c].get("default_table_name", ""))
        if not child_table:
            child_table = str(concept_by_id[to_c].get("default_table_name", ""))
        out.append(
            {
                "parent_concept": from_c,
                "child_concept": to_c,
                "parent_table": parent_table,
                "child_table": child_table,
                "parent_column": parent_col,
                "child_column": child_col,
                "cardinality": str(edge.get("cardinality", "1_to_many")),
            }
        )
    return out


def load_compiled_relation_patterns() -> list[dict[str, str]]:
    """Load concepts + concept_relation_graph from disk and return compiled relation rows."""

    from src.schema.config_store import load_schema_config

    concepts_payload = load_schema_config("concepts.json")
    kg_payload = load_schema_config("concept_relation_graph.json")
    return compile_edges_to_relation_patterns(
        concepts_payload=concepts_payload,
        kg_payload=kg_payload,
    )
