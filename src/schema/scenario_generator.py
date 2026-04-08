from __future__ import annotations

import re
from collections import Counter

from src.schema.config_store import load_schema_config
from src.schema.models import ColumnDefinition, ForeignKey, SchemaDefinition, TableDefinition
from src.schema.relation_inference import infer_foreign_keys

ENTITY_SCORE_THRESHOLD = 2.0
UNKNOWN_CONFIDENCE_THRESHOLD = 0.65
LOW_MARGIN_THRESHOLD = 0.12

# Candidate schema scoring (_build_candidate_report): two-term blend + optional profile bonus.
# Structure richness uses FK and semi-structured *densities* (per total column count), not fixed caps.
TEMPLATE_CONCEPT_COVERAGE_BASELINE = 0.4
CANDIDATE_WEIGHT_SCENARIO_FIT = 0.5
CANDIDATE_WEIGHT_STRUCTURE_RICHNESS = 0.5
PROFILE_MATCH_BONUS_POINTS = 0.1


def _sql_escape_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _allowed_values_from_payload_safe(
    column_payload: dict[str, object],
    normalized_type: str,
) -> tuple[str, ...] | None:
    """Parse optional allowed_values (categorical closed domain). Invalid shapes ignored here; validator enforces."""

    raw = column_payload.get("allowed_values")
    if raw is None:
        return None
    if normalized_type != "categorical":
        return None
    if not isinstance(raw, list):
        return None
    cleaned = tuple(str(x).strip() for x in raw if str(x).strip())
    return cleaned if cleaned else None


def _column_definition_from_field_pack_column(column_payload: dict[str, object]) -> ColumnDefinition:
    normalized_type = str(column_payload["normalized_type"])
    allowed = _allowed_values_from_payload_safe(column_payload, normalized_type)
    return ColumnDefinition(
        name=str(column_payload["name"]),
        raw_type=_map_normalized_type_to_sql(normalized_type),
        normalized_type=normalized_type,  # type: ignore[arg-type]
        is_primary_key=bool(column_payload.get("is_primary_key", False)),
        foreign_key=None,
        allowed_values=allowed,
    )


def generate_schema_from_scenario(scenario_text: str) -> SchemaDefinition:
    selected_schema, _ = generate_schema_from_scenario_with_report(scenario_text=scenario_text)
    return selected_schema


def generate_schema_from_scenario_with_report(scenario_text: str) -> tuple[SchemaDefinition, dict[str, object]]:
    """
    Generate schema: alias match → closure → domain signal/extension → infer system profile →
    business_line appends → prune cross-profile CRM noise (trading/credit vs universal graph) →
    closure → single concept assembly. No scenario_templates keyword path.
    If no concepts survive assembly, return unknown-domain draft schema (insufficient_concept_match).
    """

    concepts_payload = _load_concepts_payload()
    system_profiles = _load_system_profiles_payload()
    feedback_weights = _load_feedback_weights_payload()
    matched_concepts, concept_report_rows = _extract_concepts_from_scenario_text(
        concepts_payload=concepts_payload,
        scenario_text=scenario_text,
        feedback_weights=feedback_weights,
    )
    matched_concepts_for_schema: list[dict[str, object]] = []
    domain_extension_rationales: list[dict[str, object]] = []
    domain_signal_rationales: list[dict[str, object]] = []
    domain_theme_signal_report: dict[str, object] = {}
    inferred_system_name: str | None = None
    business_line_rationales: list[dict[str, object]] = []
    if matched_concepts or (scenario_text or "").strip():
        (
            matched_concepts_for_schema,
            domain_extension_rationales,
            domain_signal_rationales,
            domain_theme_signal_report,
            inferred_system_name,
            business_line_rationales,
        ) = _finalize_concepts_for_schema_assembly(
            concepts_payload=concepts_payload,
            matched_concepts=matched_concepts,
            system_profiles_payload=system_profiles,
            scenario_text=scenario_text,
        )

    candidates: list[dict[str, object]] = []
    if matched_concepts_for_schema:
        concept_schema = _build_schema_from_concepts(
            concepts_payload=concepts_payload,
            concept_ids=[concept["concept_id"] for concept in matched_concepts_for_schema],
        )
        concept_schema = infer_foreign_keys(schema=concept_schema)
        candidates.append(
            _build_candidate_report(
                schema=concept_schema,
                source="concept_assembly",
                inferred_system_name=inferred_system_name,
                matched_concept_ids=[concept["concept_id"] for concept in matched_concepts_for_schema],
            )
        )
    else:
        candidates.append(
            _build_candidate_report(
                schema=build_unknown_domain_draft_schema(),
                source="insufficient_concept_match",
                inferred_system_name=None,
                matched_concept_ids=[],
            )
        )

    sorted_candidates = sorted(candidates, key=lambda item: float(item["score"]), reverse=True)
    selected_candidate = sorted_candidates[0]
    selected_schema = selected_candidate["schema"]
    top2 = sorted_candidates[:2]
    runner_up_score = float(top2[1]["score"]) if len(top2) > 1 else 0.0
    selected_score = float(selected_candidate["score"])
    score_margin = selected_score - runner_up_score
    concept_count = len(matched_concepts_for_schema) if matched_concepts_for_schema else len(matched_concepts)
    unknown_domain = _detect_unknown_domain(
        inferred_system_name=inferred_system_name,
        concept_count=concept_count,
        selected_score=selected_score,
        score_margin=score_margin,
        selected_source=selected_candidate["source"],
    )
    confidence_score = round(min(max(selected_score + (score_margin * 0.3), 0.0), 1.0), 6)

    matched_concept_ids_for_domains = list(selected_candidate.get("matched_concept_ids", []))
    domain_summary = _summarize_primary_domains_for_concept_ids(
        concepts_payload=concepts_payload,
        concept_ids=matched_concept_ids_for_domains,
    )

    report = {
        "strategy": "concept_assembly_with_system_inference_and_business_lines",
        "entity_score_threshold": ENTITY_SCORE_THRESHOLD,
        "scenario_text": scenario_text,
        "inferred_system_name": inferred_system_name,
        "matched_primary_domain_ids": domain_summary["ordered_domain_ids"],
        "primary_domain_id_counts": domain_summary["counts"],
        "primary_domain_focus": domain_summary["focus_domain_id"],
        "concepts": concept_report_rows,
        "domain_extension_rule_hits": domain_extension_rationales,
        "domain_theme_signals": domain_theme_signal_report,
        "domain_signal_inference": domain_signal_rationales,
        "business_line_extensions": business_line_rationales,
        "selected_source": selected_candidate["source"],
        "selected_table_names": [table.name for table in selected_schema.tables],
        "unknown_domain": unknown_domain,
        "needs_review": unknown_domain,
        "confidence_score": confidence_score,
        "score_margin": round(score_margin, 6),
        "candidates": [
            {
                "source": candidate["source"],
                "score": candidate["score"],
                "table_names": [table.name for table in candidate["schema"].tables],
                "concept_coverage": candidate["concept_coverage"],
                "relation_count": candidate["relation_count"],
                "semistructured_column_count": candidate["semistructured_column_count"],
                "column_count": candidate["column_count"],
                "fk_density": candidate["fk_density"],
                "semistructured_ratio": candidate["semistructured_ratio"],
                "structure_richness": candidate["structure_richness"],
            }
            for candidate in top2
        ],
    }
    if unknown_domain:
        draft_schema = build_unknown_domain_draft_schema()
        report["draft_table_names"] = [table.name for table in draft_schema.tables]
    return selected_schema, report


def build_schema_from_system_profile(
    system_name: str,
    table_names: list[str] | None = None,
) -> tuple[SchemaDefinition, list[str]]:
    """
    Assembly from system_profiles default_concepts: relation closure, domain extension rules,
    business_line rules for that profile id, field_packs merge, infer_foreign_keys.
    Returns (schema, final ordered concept_ids used for tables).
    """

    concepts_payload = _load_concepts_payload()
    system_profiles_payload = _load_system_profiles_payload()
    selected_profile = _select_system_profile(
        system_profiles_payload=system_profiles_payload,
        system_name=system_name,
    )
    concept_ids = list(selected_profile["default_concepts"])
    if table_names:
        concept_ids = _resolve_concept_ids_from_table_names(
            concepts_payload=concepts_payload,
            table_names=table_names,
        )
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    concept_ids, _ = _apply_domain_extension_rules(concept_ids=concept_ids)
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    concept_ids, _ = _apply_business_line_rules(
        concept_ids=concept_ids,
        inferred_system_name=system_name.strip().lower(),
    )
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    schema = _build_schema_from_concepts(concepts_payload=concepts_payload, concept_ids=concept_ids)
    return infer_foreign_keys(schema=schema), concept_ids


def generate_schema_from_system(
    system_name: str,
    table_names: list[str] | None = None,
) -> SchemaDefinition:
    schema, _ = build_schema_from_system_profile(system_name=system_name, table_names=table_names)
    return schema


def schema_to_sql_ddl(schema: SchemaDefinition) -> str:
    ddl_blocks: list[str] = []
    for table in schema.tables:
        column_lines: list[str] = []
        for column in table.columns:
            line = f"    {column.name} {_map_normalized_type_to_sql(column.normalized_type)}"
            if column.allowed_values:
                in_list = ", ".join(_sql_escape_string_literal(v) for v in column.allowed_values)
                line += f" CHECK ({column.name} IN ({in_list}))"
            if column.is_primary_key:
                line += " PRIMARY KEY"
            column_lines.append(line)

        for column in table.columns:
            if column.foreign_key is None:
                continue
            foreign_key = column.foreign_key
            column_lines.append(
                "    "
                f"FOREIGN KEY({column.name}) "
                f"REFERENCES {foreign_key.referenced_table}({foreign_key.referenced_column})"
            )

        ddl_blocks.append(f"CREATE TABLE {table.name} (\n" + ",\n".join(column_lines) + "\n);")
    return "\n\n".join(ddl_blocks) + "\n"


def schema_to_json_payload(schema: SchemaDefinition) -> dict[str, object]:
    return {
        "tables": [
            {
                "name": table.name,
                "columns": [
                    {
                        "name": column.name,
                        "type": column.normalized_type,
                        "is_primary_key": column.is_primary_key,
                        "foreign_key": (
                            {
                                "referenced_table": column.foreign_key.referenced_table,
                                "referenced_column": column.foreign_key.referenced_column,
                            }
                            if column.foreign_key is not None
                            else None
                        ),
                        **(
                            {"allowed_values": list(column.allowed_values)}
                            if column.allowed_values is not None
                            else {}
                        ),
                    }
                    for column in table.columns
                ],
            }
            for table in schema.tables
        ]
    }


def _load_concepts_payload() -> dict[str, object]:
    return load_schema_config(config_name="concepts.json")


def _load_field_packs_payload() -> dict[str, object]:
    return load_schema_config(config_name="field_packs.json")


def _load_system_profiles_payload() -> dict[str, object]:
    return load_schema_config(config_name="system_profiles.json")


def _load_feedback_weights_payload() -> dict[str, object]:
    return load_schema_config(config_name="feedback_weights.json")


def _load_relation_patterns_payload() -> dict[str, object]:
    from src.schema.knowledge_graph import load_compiled_relation_patterns

    return {"relations": load_compiled_relation_patterns()}


def _select_system_profile(
    system_profiles_payload: dict[str, object],
    system_name: str,
) -> dict[str, object]:
    profiles = dict(system_profiles_payload.get("profiles", {}))
    profile = profiles.get(system_name.strip().lower())
    if profile is None:
        raise ValueError(f"unknown system_name: {system_name}")
    return profile


def _extract_concepts_from_scenario_text(
    concepts_payload: dict[str, object],
    scenario_text: str,
    feedback_weights: dict[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    normalized_text = _normalize_text_for_matching(scenario_text)
    alias_weights = dict(feedback_weights.get("alias_weights", {}))
    concept_bias = dict(feedback_weights.get("concept_bias", {}))
    matched_concepts: list[dict[str, object]] = []
    report_rows: list[dict[str, object]] = []
    for concept_payload in list(concepts_payload.get("concepts", [])):
        concept_id = str(concept_payload["concept_id"])
        aliases = [str(alias).lower().strip() for alias in concept_payload.get("aliases", [])]
        matched_aliases: list[str] = []
        concept_score = float(concept_bias.get(concept_id, 0.0))
        for alias in aliases:
            if not alias:
                continue
            if _alias_matches_text(alias=alias, normalized_text=normalized_text):
                matched_aliases.append(alias)
                token_count = len(alias.split())
                alias_weight = float(alias_weights.get(alias, 1.0))
                concept_score += (2.0 + (0.5 * max(token_count - 1, 0))) * alias_weight

        selected = concept_score >= ENTITY_SCORE_THRESHOLD
        report_rows.append(
            {
                "concept_id": concept_id,
                "table_name": concept_payload.get("default_table_name"),
                "score": round(concept_score, 3),
                "matched_aliases": matched_aliases,
                "selected": selected,
            }
        )
        if selected:
            matched_concepts.append(concept_payload)
    return matched_concepts, report_rows


def _build_schema_from_concepts(
    concepts_payload: dict[str, object],
    concept_ids: list[str],
) -> SchemaDefinition:
    concept_map = {str(concept["concept_id"]): concept for concept in list(concepts_payload.get("concepts", []))}
    field_packs_payload = _load_field_packs_payload()
    field_pack_map = dict(field_packs_payload.get("packs", {}))
    tables: list[TableDefinition] = []
    for concept_id in concept_ids:
        concept_payload = concept_map.get(concept_id)
        if concept_payload is None:
            continue
        selected_pack_names = list(concept_payload.get("required_packs", []))
        merged_columns_by_name: dict[str, ColumnDefinition] = {}
        for pack_name in selected_pack_names:
            for column_payload in list(field_pack_map.get(pack_name, [])):
                if not isinstance(column_payload, dict):
                    continue
                merged_columns_by_name[str(column_payload["name"])] = _column_definition_from_field_pack_column(
                    column_payload
                )
        columns: list[ColumnDefinition] = []
        for column_name in merged_columns_by_name:
            columns.append(merged_columns_by_name[column_name])
        tables.append(TableDefinition(name=str(concept_payload["default_table_name"]), columns=columns))
    return SchemaDefinition(tables=tables)


def _resolve_concept_ids_from_table_names(
    concepts_payload: dict[str, object],
    table_names: list[str],
) -> list[str]:
    normalized_table_names = {table_name.strip().lower() for table_name in table_names if table_name.strip()}
    concept_ids: list[str] = []
    for concept_payload in list(concepts_payload.get("concepts", [])):
        table_name = str(concept_payload.get("default_table_name", "")).lower()
        if table_name in normalized_table_names:
            concept_ids.append(str(concept_payload["concept_id"]))
    return concept_ids


def _expand_concepts_with_relation_dependencies(concept_ids: list[str]) -> list[str]:
    relation_patterns = list(_load_relation_patterns_payload().get("relations", []))
    selected_concepts = set(concept_ids)
    changed = True
    while changed:
        changed = False
        for relation in relation_patterns:
            child_concept = str(relation["child_concept"])
            parent_concept = str(relation["parent_concept"])
            if child_concept in selected_concepts and parent_concept not in selected_concepts:
                selected_concepts.add(parent_concept)
                changed = True
    return list(selected_concepts)


def _order_concepts_by_config_file(concepts_payload: dict[str, object], concept_id_set: set[str]) -> list[str]:
    ordered: list[str] = []
    for concept_payload in list(concepts_payload.get("concepts", [])):
        concept_id = str(concept_payload["concept_id"])
        if concept_id in concept_id_set:
            ordered.append(concept_id)
    return ordered


def _expand_concepts_with_relation_dependencies_ordered(
    concepts_payload: dict[str, object],
    concept_ids: list[str],
) -> list[str]:
    expanded_set = set(_expand_concepts_with_relation_dependencies(concept_ids=concept_ids))
    return _order_concepts_by_config_file(concepts_payload, expanded_set)


def _load_domain_extension_rules_payload() -> dict[str, object]:
    return load_schema_config("domain_extension_rules.json")


def _domain_taxonomy_signal_keyword_hits_scenario(*, keyword: str, scenario_text: str) -> bool:
    """
    Word- and phrase-boundary match for taxonomy signal_keywords (same tokenization as concept aliases).

    Why: substring checks (e.g. `k in lowered`) false-positive short tokens such as `str` inside `instrument`.
    English plural tolerance: taxonomy lists `instrument` while scenarios often say `instruments`.
    """

    phrase = _normalize_text_for_matching(keyword)
    if not phrase:
        return False
    normalized_scenario = _normalize_text_for_matching(scenario_text)
    if _alias_matches_text(alias=phrase, normalized_text=normalized_scenario):
        return True
    tokens = phrase.split()
    if len(tokens) == 1 and not phrase.endswith("s"):
        plural = f"{phrase}s"
        if _alias_matches_text(alias=plural, normalized_text=normalized_scenario):
            return True
    if len(tokens) == 1 and phrase.endswith("s") and len(phrase) > 1:
        singular = phrase[:-1]
        if _alias_matches_text(alias=singular, normalized_text=normalized_scenario):
            return True
    return False


def _profile_allowed_concept_ids(system_profiles_payload: dict[str, object], profile_key: str) -> set[str]:
    profiles = dict(system_profiles_payload.get("profiles", {}))
    profile = profiles.get(profile_key.strip().lower())
    if not isinstance(profile, dict):
        return set()
    allowed: set[str] = set()
    for bucket in ("default_concepts", "required_concepts"):
        for item in list(profile.get(bucket, []) or []):
            allowed.add(str(item))
    return allowed


def _crm_pack_concept_ids(system_profiles_payload: dict[str, object]) -> set[str]:
    return _profile_allowed_concept_ids(system_profiles_payload, "crm")


def _prune_cross_profile_closure_noise(
    *,
    concepts_payload: dict[str, object],
    system_profiles_payload: dict[str, object],
    concept_ids: list[str],
    inferred_system_name: str | None,
    seed_concept_ids: list[str],
) -> list[str]:
    """
    Relation closure walks parent edges from generic `account`; in a universal-bank graph that pulls
    CRM acquisition spine (sales_opportunity → lead → marketing_campaign) into trading/credit runs.

    Drop CRM-pack concepts that were not explicitly alias-hit in the scenario unless they belong to
    the inferred non-CRM profile bundle; then re-expand FK parents on the slimmer set.
    """

    if inferred_system_name is None:
        return concept_ids
    profile_key = str(inferred_system_name).strip().lower()
    if profile_key == "crm":
        return concept_ids

    seed_set = {str(x) for x in seed_concept_ids}
    allowed_for_profile = _profile_allowed_concept_ids(system_profiles_payload, profile_key)
    crm_pack = _crm_pack_concept_ids(system_profiles_payload)
    current = set(concept_ids)

    if profile_key == "trading" and "trading_account" in current:
        # "trading accounts" often hits generic `account` via the `accounts` alias; brokerage stories
        # should anchor on `trading_accounts` instead of the retail product-account spine.
        current.discard("account")

    if profile_key in {"trading", "credit"}:
        for cid in list(current):
            if cid in crm_pack and cid not in seed_set and cid not in allowed_for_profile:
                current.discard(cid)

    return _order_concepts_by_config_file(concepts_payload, current)


def _extract_domain_theme_signals_from_text(*, scenario_text: str) -> dict[str, object]:
    """
    Match scenario text against domain_taxonomy signal_keywords (Level-1 and Level-2).
    Used to infer extra concepts independent of concept alias scores.
    """

    tax = load_schema_config("domain_taxonomy.json")
    matched_domains_order: list[str] = []
    matched_domains_set: set[str] = set()
    matched_themes_order: list[str] = []
    matched_themes_set: set[str] = set()
    domain_hits: list[dict[str, str]] = []
    theme_hits: list[dict[str, str]] = []

    for domain in tax.get("level1_domains", []):
        if not isinstance(domain, dict):
            continue
        did = str(domain.get("domain_id", "")).strip()
        if not did:
            continue
        for kw in domain.get("signal_keywords", []) or []:
            k = str(kw).strip()
            if not k:
                continue
            if _domain_taxonomy_signal_keyword_hits_scenario(keyword=k, scenario_text=scenario_text):
                if did not in matched_domains_set:
                    matched_domains_set.add(did)
                    matched_domains_order.append(did)
                domain_hits.append({"domain_id": did, "keyword": k.strip().lower()})
                break
        for theme in domain.get("level2_themes", []) or []:
            if not isinstance(theme, dict):
                continue
            tid = str(theme.get("theme_id", "")).strip()
            if not tid:
                continue
            for kw in theme.get("signal_keywords", []) or []:
                k = str(kw).strip()
                if not k:
                    continue
                if _domain_taxonomy_signal_keyword_hits_scenario(keyword=k, scenario_text=scenario_text):
                    if tid not in matched_themes_set:
                        matched_themes_set.add(tid)
                        matched_themes_order.append(tid)
                    theme_hits.append({"theme_id": tid, "domain_id": did, "keyword": k.strip().lower()})
                    break

    return {
        "matched_domain_ids": matched_domains_order,
        "matched_theme_ids": matched_themes_order,
        "domain_hits": domain_hits,
        "theme_hits": theme_hits,
        "matched_domain_set": matched_domains_set,
        "matched_theme_set": matched_themes_set,
    }


def _apply_domain_signal_inference_rules(
    *,
    concept_ids: list[str],
    signal_bundle: dict[str, object],
) -> tuple[list[str], list[dict[str, object]]]:
    payload = load_schema_config("domain_signal_inference_rules.json")
    rules = list(payload.get("rules", []))
    current_set = set(concept_ids)
    current_list = list(concept_ids)
    rationales: list[dict[str, object]] = []
    domain_set: set[str] = signal_bundle.get("matched_domain_set", set())  # type: ignore[assignment]
    if not isinstance(domain_set, set):
        domain_set = set(domain_set)
    theme_set: set[str] = signal_bundle.get("matched_theme_set", set())  # type: ignore[assignment]
    if not isinstance(theme_set, set):
        theme_set = set(theme_set)

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        req_domains = [str(x) for x in list(rule.get("when_domains_matched_all", []))]
        if req_domains and not set(req_domains).issubset(domain_set):
            continue
        any_themes = [str(x) for x in list(rule.get("when_themes_match_any", []))]
        if any_themes and not (set(any_themes) & theme_set):
            continue
        for append_id in list(rule.get("append_concepts", [])):
            append_key = str(append_id)
            already = append_key in current_set
            if not already:
                current_list.append(append_key)
                current_set.add(append_key)
            rationales.append(
                {
                    "rule_id": rule.get("id"),
                    "target_concept_id": append_key,
                    "already_satisfied_by_prior_match": already,
                    "business_rationale": rule.get("business_rationale", ""),
                }
            )
    return current_list, rationales


def _apply_domain_extension_rules(
    concept_ids: list[str],
) -> tuple[list[str], list[dict[str, object]]]:
    payload = _load_domain_extension_rules_payload()
    rules = list(payload.get("rules", []))
    current_set = set(concept_ids)
    current_list = list(concept_ids)
    rationales: list[dict[str, object]] = []
    for rule in rules:
        required = [str(item) for item in list(rule.get("when_matched_contains_all", []))]
        if not required or not set(required).issubset(current_set):
            continue
        for append_id in list(rule.get("append_concepts", [])):
            append_key = str(append_id)
            if append_key in current_set:
                continue
            current_list.append(append_key)
            current_set.add(append_key)
            rationales.append(
                {
                    "rule_id": rule.get("id"),
                    "appended_concept_id": append_key,
                    "business_rationale": rule.get("business_rationale", ""),
                }
            )
    return current_list, rationales


def _load_business_lines_payload() -> dict[str, object]:
    return load_schema_config(config_name="business_lines.json")


def _apply_business_line_rules(
    concept_ids: list[str],
    inferred_system_name: str | None,
) -> tuple[list[str], list[dict[str, object]]]:
    """
    Append concepts for the inferred system profile id (same keys as system_profiles.json)
    when when_matched_contains_all is satisfied; runs after domain_extension_rules and closures.
    """

    if inferred_system_name is None:
        return concept_ids, []
    payload = _load_business_lines_payload()
    lines_obj = payload.get("lines", {})
    if not isinstance(lines_obj, dict):
        return concept_ids, []
    line_key = str(inferred_system_name).strip().lower()
    line_body = lines_obj.get(line_key)
    if not isinstance(line_body, dict):
        return concept_ids, []
    when_all = [str(x) for x in list(line_body.get("when_matched_contains_all", []))]
    append_concepts = [str(x) for x in list(line_body.get("append_concepts", []))]
    if not append_concepts:
        return concept_ids, []
    current_set = set(concept_ids)
    if when_all and not set(when_all).issubset(current_set):
        return concept_ids, []
    current_list = list(concept_ids)
    rationales: list[dict[str, object]] = []
    for append_id in append_concepts:
        if append_id in current_set:
            continue
        current_list.append(append_id)
        current_set.add(append_id)
        rationales.append(
            {
                "business_line": line_key,
                "appended_concept_id": append_id,
                "business_rationale": line_body.get("business_rationale", ""),
            }
        )
    return current_list, rationales


def _finalize_concepts_for_schema_assembly(
    concepts_payload: dict[str, object],
    matched_concepts: list[dict[str, object]],
    system_profiles_payload: dict[str, object],
    scenario_text: str,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, object],
    str | None,
    list[dict[str, object]],
]:
    base_ids = [str(concept_payload["concept_id"]) for concept_payload in matched_concepts]
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=base_ids,
    )
    signal_bundle = _extract_domain_theme_signals_from_text(scenario_text=scenario_text)
    concept_ids, signal_rationales = _apply_domain_signal_inference_rules(
        concept_ids=concept_ids,
        signal_bundle=signal_bundle,
    )
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    concept_ids, domain_rationales = _apply_domain_extension_rules(concept_ids=concept_ids)
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    inferred_system_name = _infer_system_name_from_concepts(
        system_profiles_payload=system_profiles_payload,
        matched_concept_ids=concept_ids,
        seed_concept_ids=base_ids,
    )
    concept_ids, business_line_rationales = _apply_business_line_rules(
        concept_ids=concept_ids,
        inferred_system_name=inferred_system_name,
    )
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    concept_ids = _prune_cross_profile_closure_noise(
        concepts_payload=concepts_payload,
        system_profiles_payload=system_profiles_payload,
        concept_ids=concept_ids,
        inferred_system_name=inferred_system_name,
        seed_concept_ids=base_ids,
    )
    concept_ids = _expand_concepts_with_relation_dependencies_ordered(
        concepts_payload=concepts_payload,
        concept_ids=concept_ids,
    )
    concept_map = {str(c["concept_id"]): c for c in list(concepts_payload.get("concepts", []))}
    ordered_payloads = [concept_map[cid] for cid in concept_ids if cid in concept_map]
    signal_report: dict[str, object] = {
        "matched_domain_ids": list(signal_bundle.get("matched_domain_ids", [])),
        "matched_theme_ids": list(signal_bundle.get("matched_theme_ids", [])),
        "domain_hits": list(signal_bundle.get("domain_hits", [])),
        "theme_hits": list(signal_bundle.get("theme_hits", [])),
    }
    return (
        ordered_payloads,
        domain_rationales,
        signal_rationales,
        signal_report,
        inferred_system_name,
        business_line_rationales,
    )


def _infer_system_name_from_concepts(
    system_profiles_payload: dict[str, object],
    matched_concept_ids: list[str],
    seed_concept_ids: list[str] | None = None,
) -> str | None:
    if not matched_concept_ids:
        return None
    seed_set = set(seed_concept_ids or [])
    matched_set = set(matched_concept_ids)
    profile_scores: list[tuple[str, float]] = []
    for system_name, profile_payload in dict(system_profiles_payload.get("profiles", {})).items():
        default_concepts = set(profile_payload.get("default_concepts", []))
        required_concepts = set(profile_payload.get("required_concepts", []))
        overlap = len(default_concepts.intersection(matched_set))
        required_overlap = len(required_concepts.intersection(matched_set))
        seed_overlap = len(default_concepts.intersection(seed_set))
        seed_required_overlap = len(required_concepts.intersection(seed_set))

        # Bias system inference toward core chain coverage.
        # Also prioritize direct alias hits over closure-added concepts.
        weighted_score = (
            float(overlap)
            + (2.0 * float(required_overlap))
            + (2.0 * float(seed_overlap))
            + (3.0 * float(seed_required_overlap))
        )
        profile_scores.append((system_name, weighted_score))
    profile_scores.sort(key=lambda item: item[1], reverse=True)
    if not profile_scores or profile_scores[0][1] == 0:
        return None
    return profile_scores[0][0]


def _summarize_primary_domains_for_concept_ids(
    concepts_payload: dict[str, object],
    concept_ids: list[str],
) -> dict[str, object]:
    """Aggregate primary_domain_id from concepts.json for Requirement 1 reporting."""

    concept_map = {
        str(c["concept_id"]): c
        for c in concepts_payload.get("concepts", [])
        if isinstance(c, dict) and str(c.get("concept_id", ""))
    }
    domains_ordered: list[str] = []
    for cid in concept_ids:
        payload = concept_map.get(str(cid))
        if payload is None:
            continue
        pid = str(payload.get("primary_domain_id", "")).strip()
        if pid:
            domains_ordered.append(pid)
    counts = dict(Counter(domains_ordered))
    ordered_unique = list(dict.fromkeys(domains_ordered))
    focus_domain_id: str | None = None
    if counts:
        focus_domain_id = max(counts.items(), key=lambda item: item[1])[0]
    return {
        "ordered_domain_ids": ordered_unique,
        "counts": counts,
        "focus_domain_id": focus_domain_id,
    }


def _build_candidate_report(
    schema: SchemaDefinition,
    source: str,
    inferred_system_name: str | None,
    matched_concept_ids: list[str],
) -> dict[str, object]:
    relation_count = sum(
        1 for table in schema.tables for column in table.columns if column.foreign_key is not None
    )
    semistructured_column_count = sum(
        1
        for table in schema.tables
        for column in table.columns
        if column.normalized_type in {"json", "xml", "text"}
    )
    column_count = sum(len(table.columns) for table in schema.tables)

    if column_count == 0:
        fk_density = 0.0
        semistructured_ratio = 0.0
    else:
        fk_density = min(float(relation_count) / float(column_count), 1.0)
        semistructured_ratio = min(float(semistructured_column_count) / float(column_count), 1.0)

    structure_richness = (fk_density + semistructured_ratio) / 2.0

    if matched_concept_ids:
        raw_coverage = float(len(matched_concept_ids)) / float(max(len(schema.tables), 1))
        concept_coverage = min(raw_coverage, 1.0)
    else:
        concept_coverage = float(TEMPLATE_CONCEPT_COVERAGE_BASELINE)

    score = (CANDIDATE_WEIGHT_SCENARIO_FIT * concept_coverage) + (
        CANDIDATE_WEIGHT_STRUCTURE_RICHNESS * structure_richness
    )
    if inferred_system_name is not None and source == "concept_assembly":
        score += PROFILE_MATCH_BONUS_POINTS

    return {
        "source": source,
        "schema": schema,
        "score": round(score, 6),
        "concept_coverage": round(concept_coverage, 6),
        "relation_count": relation_count,
        "semistructured_column_count": semistructured_column_count,
        "column_count": column_count,
        "fk_density": round(fk_density, 6),
        "semistructured_ratio": round(semistructured_ratio, 6),
        "structure_richness": round(structure_richness, 6),
        "matched_concept_ids": list(matched_concept_ids),
    }


def _detect_unknown_domain(
    inferred_system_name: str | None,
    concept_count: int,
    selected_score: float,
    score_margin: float,
    selected_source: str,
) -> bool:
    if selected_source == "insufficient_concept_match":
        return True
    if inferred_system_name is None and concept_count < 2:
        return True
    if selected_score < UNKNOWN_CONFIDENCE_THRESHOLD:
        return True
    if score_margin < LOW_MARGIN_THRESHOLD and concept_count < 3:
        return True
    return False


def build_unknown_domain_draft_schema() -> SchemaDefinition:
    """Build a conservative generic financial draft for unknown domains."""

    return SchemaDefinition(
        tables=[
            TableDefinition(
                name="business_parties",
                columns=[
                    ColumnDefinition("party_id", "TEXT", "string", is_primary_key=True),
                    ColumnDefinition("party_name", "TEXT", "string"),
                    ColumnDefinition("party_type", "TEXT", "categorical"),
                    ColumnDefinition("jurisdiction", "TEXT", "categorical"),
                    ColumnDefinition("profile_json", "JSON", "json"),
                ],
            ),
            TableDefinition(
                name="financial_contracts",
                columns=[
                    ColumnDefinition("contract_id", "TEXT", "string", is_primary_key=True),
                    ColumnDefinition(
                        "party_id",
                        "TEXT",
                        "string",
                        foreign_key=ForeignKey("business_parties", "party_id"),
                    ),
                    ColumnDefinition("contract_type", "TEXT", "categorical"),
                    ColumnDefinition("principal_amount", "NUMERIC", "decimal"),
                    ColumnDefinition("effective_time", "TIMESTAMP", "timestamp"),
                    ColumnDefinition("contract_xml", "XML", "xml"),
                ],
            ),
            TableDefinition(
                name="financial_events",
                columns=[
                    ColumnDefinition("event_id", "TEXT", "string", is_primary_key=True),
                    ColumnDefinition(
                        "contract_id",
                        "TEXT",
                        "string",
                        foreign_key=ForeignKey("financial_contracts", "contract_id"),
                    ),
                    ColumnDefinition("event_type", "TEXT", "categorical"),
                    ColumnDefinition("event_amount", "NUMERIC", "decimal"),
                    ColumnDefinition("event_time", "TIMESTAMP", "timestamp"),
                    ColumnDefinition("event_json", "JSON", "json"),
                    ColumnDefinition("event_note_text", "TEXT", "text"),
                ],
            ),
        ]
    )


def _map_normalized_type_to_sql(normalized_type: str) -> str:
    if normalized_type == "integer":
        return "INTEGER"
    if normalized_type == "decimal":
        return "NUMERIC"
    if normalized_type == "timestamp":
        return "TIMESTAMP"
    if normalized_type == "json":
        return "JSON"
    if normalized_type == "xml":
        return "XML"
    return "TEXT"


def _normalize_text_for_matching(input_text: str) -> str:
    normalized_text = re.sub(r"[^a-zA-Z0-9]+", " ", input_text.lower()).strip()
    return normalized_text


def _alias_matches_text(alias: str, normalized_text: str) -> bool:
    alias_pattern = r"\b" + r"\s+".join(re.escape(token) for token in alias.split()) + r"\b"
    return re.search(alias_pattern, normalized_text) is not None
