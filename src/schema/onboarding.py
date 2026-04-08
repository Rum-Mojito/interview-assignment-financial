from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

from src.schema.config_store import load_schema_config, write_schema_config

SUPPORTED_NORMALIZED_TYPES = {
    "string",
    "integer",
    "decimal",
    "timestamp",
    "json",
    "xml",
    "text",
    "categorical",
}


def build_onboarding_session_payload(
    scenario_text: str,
    system_name: str,
    entity_names: list[str],
    relations: list[dict[str, str]],
    entity_columns_by_entity: dict[str, list[dict[str, str]]] | None,
    source: str,
) -> dict[str, object]:
    normalized_system_name = normalize_identifier(system_name)
    normalized_entity_names = [normalize_identifier(entity_name) for entity_name in entity_names]
    normalized_entity_names = [item for item in normalized_entity_names if item]
    normalized_relations = normalize_relations(relations=relations, allowed_entities=set(normalized_entity_names))
    normalized_entity_columns = normalize_entity_columns_by_entity(
        entity_columns_by_entity=entity_columns_by_entity or {},
        allowed_entities=set(normalized_entity_names),
    )
    return {
        "version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "scenario_text": scenario_text,
        "system_name": normalized_system_name,
        "entity_names": normalized_entity_names,
        "relations": normalized_relations,
        "entity_columns_by_entity": normalized_entity_columns,
    }


def write_onboarding_session_file(session_payload: dict[str, object], output_path: Path) -> None:
    output_path.write_text(_to_json(session_payload), encoding="utf-8")


def apply_onboarding_session(session_payload: dict[str, object]) -> dict[str, object]:
    scenario_text = str(session_payload.get("scenario_text", ""))
    system_name = normalize_identifier(str(session_payload.get("system_name", "")))
    entity_names = [normalize_identifier(str(item)) for item in list(session_payload.get("entity_names", []))]
    entity_names = [item for item in entity_names if item]
    relations = normalize_relations(
        relations=list(session_payload.get("relations", [])),
        allowed_entities=set(entity_names),
    )
    entity_columns_by_entity = normalize_entity_columns_by_entity(
        entity_columns_by_entity=dict(session_payload.get("entity_columns_by_entity", {})),
        allowed_entities=set(entity_names),
    )
    if not system_name:
        raise ValueError("onboarding session must include non-empty system_name")
    if not entity_names:
        raise ValueError("onboarding session must include at least one entity")

    concepts_payload = load_schema_config("concepts.json")
    field_packs_payload = load_schema_config("field_packs.json")
    system_profiles_payload = load_schema_config("system_profiles.json")
    knowledge_graph_payload = load_schema_config("concept_relation_graph.json")
    feedback_weights_payload = load_schema_config("feedback_weights.json")

    concept_ids = _upsert_concepts_and_packs(
        concepts_payload=concepts_payload,
        field_packs_payload=field_packs_payload,
        entity_names=entity_names,
        relations=relations,
        entity_columns_by_entity=entity_columns_by_entity,
    )
    _upsert_system_profile(
        system_profiles_payload=system_profiles_payload,
        system_name=system_name,
        concept_ids=concept_ids,
    )
    _upsert_knowledge_graph_edges(
        knowledge_graph_payload=knowledge_graph_payload,
        relations=relations,
    )
    _upsert_feedback_weights(
        feedback_weights_payload=feedback_weights_payload,
        scenario_text=scenario_text,
        concept_ids=concept_ids,
    )

    write_schema_config("concepts.json", concepts_payload)
    write_schema_config("field_packs.json", field_packs_payload)
    write_schema_config("system_profiles.json", system_profiles_payload)
    write_schema_config("concept_relation_graph.json", knowledge_graph_payload)
    write_schema_config("feedback_weights.json", feedback_weights_payload)

    return {
        "system_name": system_name,
        "concept_ids": concept_ids,
        "relation_count": len(relations),
        "customized_column_entities": sorted(entity_columns_by_entity.keys()),
    }


def normalize_identifier(raw_text: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", raw_text.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def parse_relation_input(relation_text: str, allowed_entities: set[str]) -> list[dict[str, str]]:
    relation_candidates: list[dict[str, str]] = []
    segments = [segment.strip() for segment in relation_text.split(",") if segment.strip()]
    for segment in segments:
        if "->" not in segment:
            continue
        child_raw, parent_raw = segment.split("->", 1)
        relation_candidates.append(
            {
                "child": normalize_identifier(child_raw),
                "parent": normalize_identifier(parent_raw),
            }
        )
    return normalize_relations(relations=relation_candidates, allowed_entities=allowed_entities)


def parse_columns_input(columns_text: str) -> list[dict[str, str]]:
    columns: list[dict[str, str]] = []
    segments = [segment.strip() for segment in columns_text.split(",") if segment.strip()]
    seen_names: set[str] = set()
    for segment in segments:
        if ":" not in segment:
            continue
        column_name_raw, normalized_type_raw = segment.split(":", 1)
        column_name = normalize_identifier(column_name_raw)
        normalized_type = normalized_type_raw.strip().lower()
        if not column_name:
            continue
        if normalized_type not in SUPPORTED_NORMALIZED_TYPES:
            continue
        if column_name in seen_names:
            continue
        seen_names.add(column_name)
        columns.append({"name": column_name, "normalized_type": normalized_type})
    return columns


def parse_entity_columns_text(
    entity_columns_text: str,
    allowed_entities: set[str],
) -> dict[str, list[dict[str, str]]]:
    parsed_mapping: dict[str, list[dict[str, str]]] = {}
    lines = [line.strip() for line in entity_columns_text.splitlines() if line.strip()]
    for line in lines:
        if ":" not in line:
            continue
        entity_raw, columns_raw = line.split(":", 1)
        entity_name = normalize_identifier(entity_raw)
        if entity_name not in allowed_entities:
            continue
        columns = parse_columns_input(columns_text=columns_raw)
        if columns:
            parsed_mapping[entity_name] = columns
    return parsed_mapping


def normalize_relations(relations: list[dict[str, str]], allowed_entities: set[str]) -> list[dict[str, str]]:
    normalized_relations: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for relation in relations:
        child = normalize_identifier(str(relation.get("child", "")))
        parent = normalize_identifier(str(relation.get("parent", "")))
        if child not in allowed_entities or parent not in allowed_entities:
            continue
        if child == parent:
            continue
        pair = (child, parent)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        normalized_relations.append({"child": child, "parent": parent})
    return normalized_relations


def normalize_entity_columns_by_entity(
    entity_columns_by_entity: dict[str, list[dict[str, str]]],
    allowed_entities: set[str],
) -> dict[str, list[dict[str, str]]]:
    normalized_mapping: dict[str, list[dict[str, str]]] = {}
    for entity_name_raw, columns_raw in entity_columns_by_entity.items():
        entity_name = normalize_identifier(str(entity_name_raw))
        if entity_name not in allowed_entities:
            continue
        normalized_columns: list[dict[str, str]] = []
        seen_names: set[str] = set()
        for column_raw in list(columns_raw):
            column_name = normalize_identifier(str(column_raw.get("name", "")))
            normalized_type = str(column_raw.get("normalized_type", "")).lower().strip()
            if not column_name:
                continue
            if normalized_type not in SUPPORTED_NORMALIZED_TYPES:
                continue
            if column_name in seen_names:
                continue
            seen_names.add(column_name)
            normalized_columns.append({"name": column_name, "normalized_type": normalized_type})
        if normalized_columns:
            normalized_mapping[entity_name] = normalized_columns
    return normalized_mapping


def _upsert_concepts_and_packs(
    concepts_payload: dict[str, object],
    field_packs_payload: dict[str, object],
    entity_names: list[str],
    relations: list[dict[str, str]],
    entity_columns_by_entity: dict[str, list[dict[str, str]]],
) -> list[str]:
    concepts_list = list(concepts_payload.get("concepts", []))
    concepts_by_id = {str(item["concept_id"]): item for item in concepts_list}
    packs = dict(field_packs_payload.get("packs", {}))

    parent_by_child = {relation["child"]: relation["parent"] for relation in relations}
    concept_ids: list[str] = []
    for entity_name in entity_names:
        concept_id = normalize_identifier(entity_name)
        if not concept_id:
            continue
        concept_ids.append(concept_id)
        table_name = pluralize_table_name(concept_id)

        pk_pack = f"pk_{concept_id}"
        core_pack = f"{concept_id}_core"
        parent_concept_id = parent_by_child.get(concept_id)
        required_packs = [pk_pack]
        if parent_concept_id:
            required_packs.append(f"fk_{parent_concept_id}")
        required_packs.append(core_pack)

        if concept_id in concepts_by_id:
            concept_item = concepts_by_id[concept_id]
            existing_aliases = [str(alias) for alias in concept_item.get("aliases", [])]
            for alias in [concept_id, concept_id.replace("_", " "), table_name, table_name.replace("_", " ")]:
                if alias not in existing_aliases:
                    existing_aliases.append(alias)
            concept_item["aliases"] = existing_aliases
            concept_item["default_table_name"] = table_name
            merged_required_packs = [str(pack_name) for pack_name in concept_item.get("required_packs", [])]
            for pack_name in required_packs:
                if pack_name not in merged_required_packs:
                    merged_required_packs.append(pack_name)
            concept_item["required_packs"] = merged_required_packs
            concept_item["optional_packs"] = []
        else:
            new_concept = {
                "concept_id": concept_id,
                "primary_domain_id": "party_legal_relationship",
                "aliases": [concept_id, concept_id.replace("_", " "), table_name, table_name.replace("_", " ")],
                "default_table_name": table_name,
                "required_packs": required_packs,
                "optional_packs": [],
            }
            concepts_list.append(new_concept)
            concepts_by_id[concept_id] = new_concept

        pk_column_name = f"{concept_id}_id"
        if pk_pack not in packs:
            packs[pk_pack] = [{"name": pk_column_name, "normalized_type": "string", "is_primary_key": True}]
        customized_columns = list(entity_columns_by_entity.get(concept_id, []))
        if customized_columns:
            packs[core_pack] = customized_columns
        elif core_pack not in packs:
            packs[core_pack] = _default_core_columns(concept_id=concept_id)

    for relation in relations:
        parent_concept_id = normalize_identifier(relation["parent"])
        fk_pack = f"fk_{parent_concept_id}"
        if fk_pack not in packs:
            packs[fk_pack] = [{"name": f"{parent_concept_id}_id", "normalized_type": "string"}]

    concepts_payload["concepts"] = concepts_list
    field_packs_payload["packs"] = packs
    unique_concept_ids = list(dict.fromkeys(concept_ids))
    return unique_concept_ids


def _upsert_system_profile(
    system_profiles_payload: dict[str, object],
    system_name: str,
    concept_ids: list[str],
) -> None:
    profiles = dict(system_profiles_payload.get("profiles", {}))
    profiles[system_name] = {
        "default_concepts": concept_ids,
        "required_concepts": concept_ids[: max(min(len(concept_ids), 2), 1)],
    }
    system_profiles_payload["profiles"] = profiles


def _upsert_knowledge_graph_edges(
    knowledge_graph_payload: dict[str, object],
    relations: list[dict[str, str]],
) -> None:
    edges = list(knowledge_graph_payload.get("edges", []))
    if not isinstance(edges, list):
        edges = []
    existing_keys = {
        (str(item.get("from_concept", "")), str(item.get("to_concept", "")))
        for item in edges
        if isinstance(item, dict)
    }
    nodes = list(knowledge_graph_payload.get("nodes", []))
    if not isinstance(nodes, list):
        nodes = []
    node_ids = {
        str(n.get("concept_id", "")).strip()
        for n in nodes
        if isinstance(n, dict) and str(n.get("concept_id", "")).strip()
    }

    for relation in relations:
        child_concept = normalize_identifier(relation["child"])
        parent_concept = normalize_identifier(relation["parent"])
        edge_key = (parent_concept, child_concept)
        if edge_key in existing_keys:
            continue
        edge_id = f"{parent_concept}_to_{child_concept}"
        edges.append(
            {
                "id": edge_id,
                "from_concept": parent_concept,
                "to_concept": child_concept,
                "cardinality": "1_to_many",
                "fk": {
                    "child_column": f"{parent_concept}_id",
                    "parent_column": f"{parent_concept}_id",
                },
            }
        )
        existing_keys.add(edge_key)
        for cid in (parent_concept, child_concept):
            if cid not in node_ids:
                nodes.append({"concept_id": cid, "label": ""})
                node_ids.add(cid)
    knowledge_graph_payload["edges"] = edges
    knowledge_graph_payload["nodes"] = nodes


def _upsert_feedback_weights(
    feedback_weights_payload: dict[str, object],
    scenario_text: str,
    concept_ids: list[str],
) -> None:
    alias_weights = dict(feedback_weights_payload.get("alias_weights", {}))
    concept_bias = dict(feedback_weights_payload.get("concept_bias", {}))
    normalized_text = scenario_text.lower()

    for concept_id in concept_ids:
        alias = concept_id.replace("_", " ")
        if alias in normalized_text:
            alias_weights[alias] = max(float(alias_weights.get(alias, 1.0)), 1.8)
        concept_bias[concept_id] = max(float(concept_bias.get(concept_id, 0.0)), 0.8)

    feedback_weights_payload["alias_weights"] = alias_weights
    feedback_weights_payload["concept_bias"] = concept_bias
    feedback_weights_payload["updated_at"] = datetime.now(timezone.utc).isoformat()


def pluralize_table_name(concept_id: str) -> str:
    if concept_id.endswith("s"):
        return concept_id
    if concept_id.endswith("y") and len(concept_id) > 1 and concept_id[-2] not in "aeiou":
        return concept_id[:-1] + "ies"
    return concept_id + "s"


def _default_core_columns(concept_id: str) -> list[dict[str, str]]:
    return [
        {"name": f"{concept_id}_status", "normalized_type": "categorical"},
        {"name": f"{concept_id}_amount", "normalized_type": "decimal"},
        {"name": f"{concept_id}_time", "normalized_type": "timestamp"},
        {"name": f"{concept_id}_json", "normalized_type": "json"},
    ]


def _to_json(payload: dict[str, object]) -> str:
    import json

    return json.dumps(payload, indent=2, ensure_ascii=True)
