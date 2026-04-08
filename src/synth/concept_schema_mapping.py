from __future__ import annotations

from dataclasses import dataclass
from itertools import product

from src.infra.config_store import SYNTH_TOPOLOGY, load_schema_config
from src.schema.models import SchemaDefinition
from src.synth.concept_graph_topology import try_resolve_chain_from_concept_graph
from src.synth.generation_chain_resolver import ResolvedEventChain, try_resolve_configured_chain


# Common physical abbreviations → concept_id (business naming, not schema-specific).
_TABLE_ABBREV_TO_CONCEPT: dict[str, str] = {
    "cust": "customer",
    "acct": "account",
    "txn": "transaction",
}


@dataclass(frozen=True)
class TableConceptAssignment:
    table_name: str
    concept_id: str
    score: float
    primary_domain_id: str
    match_reason: str


@dataclass(frozen=True)
class ConceptSchemaMappingResult:
    """Schema → project concept vocabulary (shared with Requirement 1)."""

    assignments: tuple[TableConceptAssignment, ...]
    table_to_concept: dict[str, str]
    concept_to_table: dict[str, str]
    confidence_score: float
    inferred_primary_domain_id: str | None
    inferred_secondary_domain_id: str | None
    resolved_event_chain: ResolvedEventChain | None
    needs_review: bool
    generation_mode_recommended: str  # "event_first" | "rowwise_fallback"
    mode_decision_reason: str
    mode_decision_details: dict[str, object]


def map_schema_to_concepts(
    schema: SchemaDefinition,
    graph_path_id: str | None = None,
) -> ConceptSchemaMappingResult:
    """
    Match physical tables to concepts using aliases from ``concepts.json`` and FK shape.

    When a coherent customer→account→transaction chain exists, recommend event-first generation.
    """
    concepts_payload = load_schema_config("concepts.json")
    concept_rows = concepts_payload.get("concepts", [])
    if not isinstance(concept_rows, list):
        concept_rows = []

    table_map = schema.table_map()
    candidates: list[tuple[float, str, str, str, str]] = []
    table_best_candidate: dict[str, dict[str, object]] = {}
    # score, table_name, concept_id, domain_id, reason — one best concept per table
    for table_name in sorted(table_map.keys()):
        best_concept_id = ""
        best_score = 0.0
        best_domain = ""
        best_reason = "no_concept_match"
        for concept in concept_rows:
            if not isinstance(concept, dict):
                continue
            cid = str(concept.get("concept_id", ""))
            score, reason = _score_table_for_concept(table_name=table_name, concept=concept)
            if score > best_score:
                best_score = score
                best_concept_id = cid
                best_domain = str(concept.get("primary_domain_id", "") or "")
                best_reason = reason

        table_best_candidate[table_name] = {
            "best_concept_id": best_concept_id,
            "best_score": round(best_score, 4),
            "best_domain_id": best_domain,
            "best_reason": best_reason,
            "accepted": bool(best_score >= 0.12 and best_concept_id),
        }
        if best_score >= 0.12 and best_concept_id:
            candidates.append(
                (best_score, table_name, best_concept_id, best_domain, best_reason)
            )

    candidates.sort(key=lambda row: row[0], reverse=True)
    used_tables: set[str] = set()
    used_concepts: set[str] = set()
    assignments: list[TableConceptAssignment] = []
    needs_review = False

    for score, table_name, concept_id, domain_id, reason in candidates:
        if table_name in used_tables:
            continue
        if concept_id in used_concepts:
            needs_review = True
            continue
        used_tables.add(table_name)
        used_concepts.add(concept_id)
        assignments.append(
            TableConceptAssignment(
                table_name=table_name,
                concept_id=concept_id,
                score=round(score, 4),
                primary_domain_id=domain_id,
                match_reason=reason,
            )
        )

    table_to_concept = {assignment.table_name: assignment.concept_id for assignment in assignments}
    concept_to_table = {assignment.concept_id: assignment.table_name for assignment in assignments}

    resolved = try_resolve_chain_from_concept_graph(
        schema=schema,
        concept_to_table=concept_to_table,
        path_id_filter=graph_path_id,
    )
    if resolved is None:
        resolved = try_resolve_configured_chain(
            schema=schema,
            concept_to_table=concept_to_table,
            chain_id_filter=graph_path_id,
        )
    if resolved is None:
        recovered_mapping, recovered_chain = _try_recover_prefixed_graph_chain(
            schema=schema,
            path_id_filter=graph_path_id,
            concept_rows=concept_rows,
        )
        if recovered_chain is not None:
            concept_to_table.update(recovered_mapping)
            for concept_id, table_name in recovered_mapping.items():
                table_to_concept[table_name] = concept_id
            resolved = recovered_chain

    confidence_score = _compute_confidence(
        assignments=assignments,
        resolved_event_chain=resolved,
        table_count=len(table_map),
    )

    inferred_primary_domain, inferred_secondary_domain = _infer_top_two_domains_from_assignments(
        assignments=assignments
    )

    threshold = resolved.confidence_threshold if resolved is not None else 0.45
    if resolved is not None and confidence_score >= threshold:
        mode = "event_first"
        mode_reason = "resolved_chain_and_confidence_pass"
    else:
        mode = "rowwise_fallback"
        if resolved is None:
            mode_reason = "no_resolved_event_chain"
        elif confidence_score < threshold:
            mode_reason = "confidence_below_chain_threshold"
        else:
            mode_reason = "fallback_unspecified"

    table_names = sorted(table_map.keys())
    unmatched_tables = [
        table_name
        for table_name in table_names
        if table_name not in table_to_concept
    ]
    details = {
        "table_count": len(table_names),
        "mapped_table_count": len(table_to_concept),
        "unmatched_tables": unmatched_tables,
        "confidence_score": round(confidence_score, 4),
        "confidence_threshold": threshold,
        "graph_path_id_filter": graph_path_id,
        "resolved_chain_present": resolved is not None,
        "table_best_candidate": table_best_candidate,
    }

    return ConceptSchemaMappingResult(
        assignments=tuple(assignments),
        table_to_concept=table_to_concept,
        concept_to_table=dict(concept_to_table),
        confidence_score=round(confidence_score, 4),
        inferred_primary_domain_id=inferred_primary_domain,
        inferred_secondary_domain_id=inferred_secondary_domain,
        resolved_event_chain=resolved,
        needs_review=needs_review or resolved is None,
        generation_mode_recommended=mode,
        mode_decision_reason=mode_reason,
        mode_decision_details=details,
    )


def _score_table_for_concept(*, table_name: str, concept: dict[str, object]) -> tuple[float, str]:
    normalized = _normalize_table_name_for_matching(table_name)
    concept_id = str(concept.get("concept_id", ""))
    default_table = str(concept.get("default_table_name", "") or "").lower()

    score = 0.0
    reason_parts: list[str] = []

    if default_table and normalized == default_table:
        score += 0.55
        reason_parts.append("default_table_name")

    aliases = concept.get("aliases", [])
    if isinstance(aliases, list):
        for alias in aliases:
            if not isinstance(alias, str):
                continue
            a = alias.lower().strip()
            if a == normalized:
                score += 0.48
                reason_parts.append(f"alias_exact:{a}")
            elif a in normalized or normalized in a:
                score += 0.22
                reason_parts.append(f"alias_partial:{a}")

    abbrev_target = _TABLE_ABBREV_TO_CONCEPT.get(normalized)
    if abbrev_target == concept_id:
        score += 0.38
        reason_parts.append("abbrev_hint")

    return min(score, 1.0), "+".join(reason_parts) if reason_parts else "no_match"


def _normalize_table_name_for_matching(table_name: str) -> str:
    """
    Normalize physical table names so prefixed multi-schema tables can still match concepts.

    Examples:
    - crmA_customers -> customers
    - treA_funding_deals -> funding_deals
    """
    normalized = table_name.lower().strip()
    if "_" not in normalized:
        return normalized
    parts = normalized.split("_")
    if len(parts) <= 1:
        return normalized
    first = parts[0]
    # remove one namespace segment only for short/system-like prefixes (crma_, trda_, s1_, ...)
    # while keeping semantic names like trading_accounts intact.
    has_digit = any(ch.isdigit() for ch in first)
    if len(first) <= 4 or has_digit:
        return "_".join(parts[1:])
    return normalized


def _try_recover_prefixed_graph_chain(
    *,
    schema: SchemaDefinition,
    path_id_filter: str | None,
    concept_rows: list[object],
) -> tuple[dict[str, str], ResolvedEventChain | None]:
    """
    Build a prefix-coherent concept_to_table candidate map for graph-chain resolution.

    This is a targeted fallback for multi-schema inputs where table names are namespaced,
    e.g. crmA_customers / crmA_accounts / crmA_transactions.
    """
    topology = load_schema_config(SYNTH_TOPOLOGY)
    graph_paths = topology.get("graph_event_paths", [])
    if not isinstance(graph_paths, list):
        return {}, None

    concept_default_table: dict[str, str] = {}
    for concept in concept_rows:
        if not isinstance(concept, dict):
            continue
        cid = str(concept.get("concept_id", ""))
        default_table = str(concept.get("default_table_name", "")).lower().strip()
        if cid and default_table:
            concept_default_table[cid] = default_table

    table_names = sorted(schema.table_map().keys())

    for path_cfg in graph_paths:
        if not isinstance(path_cfg, dict):
            continue
        path_id = str(path_cfg.get("path_id", ""))
        if path_id_filter is not None and path_id != path_id_filter:
            continue
        concept_path = _concept_path_from_manifest_edges(path_cfg=path_cfg)
        if not concept_path:
            continue

        candidate_lists: list[list[str]] = []
        for concept_id in concept_path:
            default_table = concept_default_table.get(concept_id, "")
            if not default_table:
                candidate_lists = []
                break
            candidates = [
                table_name
                for table_name in table_names
                if _normalize_table_name_for_matching(table_name) == default_table
                or table_name.lower() == default_table
            ]
            if not candidates:
                candidate_lists = []
                break
            candidate_lists.append(candidates)
        if not candidate_lists:
            continue

        for combination in product(*candidate_lists):
            if len(set(combination)) < len(combination):
                continue
            concept_to_table_candidate = {
                concept_id: table_name
                for concept_id, table_name in zip(concept_path, combination)
            }
            resolved = try_resolve_chain_from_concept_graph(
                schema=schema,
                concept_to_table=concept_to_table_candidate,
                path_id_filter=path_id,
            )
            if resolved is not None:
                return concept_to_table_candidate, resolved
    return {}, None


def _concept_path_from_manifest_edges(*, path_cfg: dict[str, object]) -> tuple[str, ...]:
    edge_ids = path_cfg.get("edge_ids", [])
    if not isinstance(edge_ids, list) or not edge_ids:
        return ()
    edge_id_list = [str(edge_id) for edge_id in edge_ids]
    graph_payload = load_schema_config("concept_relation_graph.json")
    edges = graph_payload.get("edges", [])
    if not isinstance(edges, list):
        return ()
    edge_by_id = {
        str(edge.get("id", "")): edge
        for edge in edges
        if isinstance(edge, dict) and edge.get("id")
    }
    concept_path: list[str] = []
    for index, edge_id in enumerate(edge_id_list):
        edge = edge_by_id.get(edge_id)
        if edge is None:
            return ()
        from_concept = str(edge.get("from_concept", ""))
        to_concept = str(edge.get("to_concept", ""))
        if not from_concept or not to_concept:
            return ()
        if index == 0:
            concept_path.extend([from_concept, to_concept])
        else:
            if concept_path[-1] != from_concept:
                return ()
            concept_path.append(to_concept)
    return tuple(concept_path)


def _infer_top_two_domains_from_assignments(
    *,
    assignments: list[TableConceptAssignment],
) -> tuple[str | None, str | None]:
    if not assignments:
        return None, None
    domain_scores: dict[str, float] = {}
    for assignment in assignments:
        domain_id = assignment.primary_domain_id
        if not domain_id:
            continue
        domain_scores[domain_id] = domain_scores.get(domain_id, 0.0) + assignment.score
    if not domain_scores:
        return None, None
    ranked_domains = sorted(
        domain_scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    primary = ranked_domains[0][0] if ranked_domains else None
    secondary = ranked_domains[1][0] if len(ranked_domains) > 1 else None
    return primary, secondary


def _compute_confidence(
    *,
    assignments: list[TableConceptAssignment],
    resolved_event_chain: ResolvedEventChain | None,
    table_count: int,
) -> float:
    if table_count == 0:
        return 0.0
    if not assignments:
        return 0.15

    covered = len(assignments)
    coverage_ratio = min(1.0, covered / max(table_count, 1))
    mean_score = sum(assignment.score for assignment in assignments) / max(len(assignments), 1)

    base = 0.45 * mean_score + 0.25 * coverage_ratio
    if resolved_event_chain is not None:
        base += 0.25
    return min(base, 1.0)


def concept_mapping_to_json_payload(result: ConceptSchemaMappingResult) -> dict[str, object]:
    """Serializable snapshot for ``concept_mapping.json`` and quality reports."""

    from src.synth.generation_chain_resolver import resolved_chain_to_json_payload

    chain_payload: dict[str, object] | None = None
    if result.resolved_event_chain is not None:
        chain_payload = resolved_chain_to_json_payload(result.resolved_event_chain)

    return {
        "confidence_score": result.confidence_score,
        "inferred_primary_domain_id": result.inferred_primary_domain_id,
        "inferred_secondary_domain_id": result.inferred_secondary_domain_id,
        "needs_review": result.needs_review,
        "generation_mode_recommended": result.generation_mode_recommended,
        "mode_decision_reason": result.mode_decision_reason,
        "mode_decision_details": dict(result.mode_decision_details),
        "table_to_concept": dict(result.table_to_concept),
        "concept_to_table": dict(result.concept_to_table),
        "assignments": [
            {
                "table_name": assignment.table_name,
                "concept_id": assignment.concept_id,
                "score": assignment.score,
                "primary_domain_id": assignment.primary_domain_id,
                "match_reason": assignment.match_reason,
            }
            for assignment in result.assignments
        ],
        "resolved_event_chain": chain_payload,
    }
