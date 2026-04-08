from __future__ import annotations

from src.infra.config_store import SYNTH_TOPOLOGY, load_schema_config
from src.schema.models import SchemaDefinition
from src.synth.generation_chain_resolver import ResolvedEventChain, _pick_fk_column

def try_resolve_chain_from_concept_graph(
    *,
    schema: SchemaDefinition,
    concept_to_table: dict[str, str],
    path_id_filter: str | None = None,
) -> ResolvedEventChain | None:
    """
    Prefer topology from ``concept_relation_graph.json`` + ``synth/topology/generation_config_manifest.json``.

    Each ``graph_event_paths`` entry lists ``edge_ids`` that must form a linear chain in the graph.
    If ``path_id_filter`` is set, only that ``path_id`` is considered (must match manifest).
    """
    topology = load_schema_config(SYNTH_TOPOLOGY)
    paths = topology.get("graph_event_paths", [])
    if not isinstance(paths, list):
        return None

    graph_payload = load_schema_config("concept_relation_graph.json")
    edges_raw = graph_payload.get("edges", [])
    if not isinstance(edges_raw, list):
        return None

    edge_by_id: dict[str, dict[str, object]] = {}
    for edge in edges_raw:
        if not isinstance(edge, dict):
            continue
        eid = str(edge.get("id", ""))
        if eid:
            edge_by_id[eid] = edge

    for path_cfg in paths:
        if not isinstance(path_cfg, dict):
            continue
        if path_id_filter is not None:
            if str(path_cfg.get("path_id", "")) != path_id_filter:
                continue
        edge_ids = path_cfg.get("edge_ids", [])
        if not isinstance(edge_ids, list) or len(edge_ids) < 1:
            continue
        resolved = _resolve_linear_path_from_edges(
            schema=schema,
            concept_to_table=concept_to_table,
            edge_ids=[str(x) for x in edge_ids],
            edge_by_id=edge_by_id,
            path_cfg=path_cfg,
        )
        if resolved is not None:
            return resolved
    return None


def _resolve_linear_path_from_edges(
    *,
    schema: SchemaDefinition,
    concept_to_table: dict[str, str],
    edge_ids: list[str],
    edge_by_id: dict[str, dict[str, object]],
    path_cfg: dict[str, object],
) -> ResolvedEventChain | None:
    edges: list[dict[str, object]] = []
    for eid in edge_ids:
        edge = edge_by_id.get(eid)
        if edge is None:
            return None
        edges.append(edge)

    concept_path_list: list[str] = []
    for index, edge in enumerate(edges):
        from_c = str(edge.get("from_concept", ""))
        to_c = str(edge.get("to_concept", ""))
        if not from_c or not to_c:
            return None
        if index == 0:
            concept_path_list.append(from_c)
            concept_path_list.append(to_c)
        else:
            if concept_path_list[-1] != from_c:
                return None
            concept_path_list.append(to_c)

    concept_path = tuple(concept_path_list)
    if len(concept_path) < 2:
        return None

    for concept_id in concept_path:
        if concept_to_table.get(concept_id) is None:
            return None

    physical_tables = [concept_to_table[c] for c in concept_path]
    if len(set(physical_tables)) < len(physical_tables):
        return None

    table_map = schema.table_map()
    fk_columns: dict[tuple[str, str], str] = {}

    for edge in edges:
        child_c = str(edge.get("to_concept", ""))
        parent_c = str(edge.get("from_concept", ""))
        fk_meta = edge.get("fk")
        child_table_name = concept_to_table.get(child_c)
        parent_table_name = concept_to_table.get(parent_c)
        if not child_table_name or not parent_table_name:
            return None
        child_table = table_map.get(child_table_name)
        if child_table is None:
            return None
        tokens = _tokens_from_fk_meta(fk_meta)
        fk_col = _pick_fk_column(
            child_table=child_table,
            parent_table=parent_table_name,
            preferred_tokens=tokens,
        )
        if fk_col is None:
            return None
        fk_columns[(child_c, parent_c)] = fk_col

    for left, right in zip(concept_path[:-1], concept_path[1:]):
        if (right, left) not in fk_columns:
            return None

    pk_by_concept: dict[str, str] = {}
    for concept_id in concept_path:
        table_name = concept_to_table[concept_id]
        table_definition = table_map.get(table_name)
        if table_definition is None:
            return None
        pk_by_concept[concept_id] = table_definition.primary_key_column()

    path_id = str(path_cfg.get("path_id", "graph_path"))
    cardinality_profile_id = str(path_cfg.get("cardinality_profile_id", "party_deposit_ledger"))
    engine = str(path_cfg.get("engine", "event_first"))
    threshold_raw = path_cfg.get("confidence_threshold", 0.45)
    confidence_threshold = float(threshold_raw) if isinstance(threshold_raw, (int, float)) else 0.45

    if engine == "event_first" and len(concept_path) < 2:
        return None

    return ResolvedEventChain(
        chain_id=path_id,
        concept_path=concept_path,
        table_by_concept={c: concept_to_table[c] for c in concept_path},
        pk_column_by_concept=pk_by_concept,
        fk_child_to_parent_column=fk_columns,
        cardinality_profile_id=cardinality_profile_id,
        engine=engine,
        confidence_threshold=confidence_threshold,
        topology_source="concept_relation_graph",
        graph_edge_ids=tuple(edge_ids),
    )


def _tokens_from_fk_meta(fk_meta: object) -> tuple[str, ...]:
    if not isinstance(fk_meta, dict):
        return ("id",)
    child_col = str(fk_meta.get("child_column", "") or "").lower()
    if not child_col:
        return ("id",)
    without_suffix = child_col[:-3] if child_col.endswith("_id") else child_col
    without_suffix = without_suffix.strip("_")
    segments = [segment for segment in without_suffix.split("_") if segment]
    if segments:
        return tuple(segments)
    return (child_col,)