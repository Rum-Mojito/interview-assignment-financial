from __future__ import annotations

from dataclasses import dataclass

from src.infra.config_store import SYNTH_TOPOLOGY, load_schema_config
from src.schema.models import ColumnDefinition, SchemaDefinition, TableDefinition

@dataclass(frozen=True)
class ResolvedEventChain:
    """Config-driven chain: ordered concepts mapped to physical tables with FK columns."""

    chain_id: str
    concept_path: tuple[str, ...]
    table_by_concept: dict[str, str]
    pk_column_by_concept: dict[str, str]
    fk_child_to_parent_column: dict[tuple[str, str], str]
    cardinality_profile_id: str
    engine: str
    confidence_threshold: float
    topology_source: str = "generation_chains_file"
    graph_edge_ids: tuple[str, ...] | None = None


def load_generation_chains_payload() -> dict[str, object]:
    payload = load_schema_config(SYNTH_TOPOLOGY)
    chains = payload.get("chains", [])
    if isinstance(chains, list):
        return {"chains": chains}
    return {"chains": []}


def try_resolve_configured_chain(
    *,
    schema: SchemaDefinition,
    concept_to_table: dict[str, str],
    chain_id_filter: str | None = None,
) -> ResolvedEventChain | None:
    payload = load_generation_chains_payload()
    chains = payload.get("chains", [])
    if not isinstance(chains, list):
        return None

    for chain_cfg in chains:
        if not isinstance(chain_cfg, dict):
            continue
        if chain_id_filter is not None:
            if str(chain_cfg.get("chain_id", "")) != chain_id_filter:
                continue
        resolved = _try_resolve_single_chain(
            schema=schema,
            concept_to_table=concept_to_table,
            chain_cfg=chain_cfg,
        )
        if resolved is not None:
            return resolved
    return None


def _try_resolve_single_chain(
    *,
    schema: SchemaDefinition,
    concept_to_table: dict[str, str],
    chain_cfg: dict[str, object],
) -> ResolvedEventChain | None:
    concept_path_raw = chain_cfg.get("concept_path", [])
    if not isinstance(concept_path_raw, list) or len(concept_path_raw) < 2:
        return None

    concept_path = tuple(str(c) for c in concept_path_raw)
    for concept_id in concept_path:
        if concept_to_table.get(concept_id) is None:
            return None

    physical_tables = [concept_to_table[c] for c in concept_path]
    if len(set(physical_tables)) < len(physical_tables):
        return None

    table_map = schema.table_map()
    fk_columns: dict[tuple[str, str], str] = {}
    expectations = chain_cfg.get("fk_expectations", [])
    if not isinstance(expectations, list):
        return None

    for expectation in expectations:
        if not isinstance(expectation, dict):
            continue
        child_c = str(expectation.get("child_concept", ""))
        parent_c = str(expectation.get("parent_concept", ""))
        tokens_raw = expectation.get("preferred_child_column_tokens", ["customer"])
        if isinstance(tokens_raw, (list, tuple)):
            tokens = tuple(str(t) for t in tokens_raw)
        else:
            tokens = ("customer",)

        if child_c not in concept_path or parent_c not in concept_path:
            continue
        child_table_name = concept_to_table.get(child_c)
        parent_table_name = concept_to_table.get(parent_c)
        if not child_table_name or not parent_table_name:
            return None
        child_table = table_map.get(child_table_name)
        if child_table is None:
            return None
        fk_col = _pick_fk_column(
            child_table=child_table,
            parent_table=parent_table_name,
            preferred_tokens=tokens,
        )
        if fk_col is None:
            return None
        fk_columns[(child_c, parent_c)] = fk_col

    # Require consecutive pairs along concept_path to be covered
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

    chain_id = str(chain_cfg.get("chain_id", "unnamed_chain"))
    cardinality_profile_id = str(chain_cfg.get("cardinality_profile_id", "party_deposit_ledger"))
    engine = str(chain_cfg.get("engine", "event_first"))
    threshold_raw = chain_cfg.get("confidence_threshold", 0.45)
    confidence_threshold = float(threshold_raw) if isinstance(threshold_raw, (int, float)) else 0.45

    if engine == "event_first" and len(concept_path) < 2:
        return None

    return ResolvedEventChain(
        chain_id=chain_id,
        concept_path=concept_path,
        table_by_concept={c: concept_to_table[c] for c in concept_path},
        pk_column_by_concept=pk_by_concept,
        fk_child_to_parent_column=fk_columns,
        cardinality_profile_id=cardinality_profile_id,
        engine=engine,
        confidence_threshold=confidence_threshold,
        topology_source="generation_chains_file",
        graph_edge_ids=None,
    )


def _pick_fk_column(
    *,
    child_table: TableDefinition,
    parent_table: str,
    preferred_tokens: tuple[str, ...],
) -> str | None:
    candidates: list[ColumnDefinition] = child_table.foreign_key_columns()
    matching: list[ColumnDefinition] = [
        column for column in candidates if column.foreign_key and column.foreign_key.referenced_table == parent_table
    ]
    if not matching:
        return None
    if len(matching) == 1:
        return matching[0].name
    lowered = [column for column in matching if any(token in column.name.lower() for token in preferred_tokens)]
    if len(lowered) == 1:
        return lowered[0].name
    return matching[0].name


def resolved_chain_to_json_payload(chain: ResolvedEventChain) -> dict[str, object]:
    fk_list = [
        {
            "child_concept": child,
            "parent_concept": parent,
            "fk_column_on_child": col,
        }
        for (child, parent), col in chain.fk_child_to_parent_column.items()
    ]
    payload: dict[str, object] = {
        "chain_id": chain.chain_id,
        "concept_path": list(chain.concept_path),
        "table_by_concept": dict(chain.table_by_concept),
        "pk_column_by_concept": dict(chain.pk_column_by_concept),
        "fk_child_to_parent": fk_list,
        "cardinality_profile_id": chain.cardinality_profile_id,
        "engine": chain.engine,
        "confidence_threshold": chain.confidence_threshold,
        "topology_source": chain.topology_source,
    }
    if chain.graph_edge_ids is not None:
        payload["graph_edge_ids"] = list(chain.graph_edge_ids)
    return payload
