"""
Validate JSON under src/project_config for internal consistency and official scenario coverage.

Domain knowledge lives in these files; this module fails fast on contradictions (missing packs,
orphan relations, invalid business_lines keys, broken domain rules).
"""

from __future__ import annotations

from src.schema.config_store import (
    SYNTH_CARDINALITY_PROFILES,
    SYNTH_COLUMN_PROFILES,
    SYNTH_GENERATION_RULES,
    SYNTH_JSON_OBJECT_PACKS,
    SYNTH_LIFECYCLE_CONSTRAINTS,
    SYNTH_MANIFEST,
    SYNTH_SCENARIO_OVERLAYS,
    SYNTH_STATUS_NORMALIZATION,
    SYNTH_TOPOLOGY,
    load_schema_config,
)
from src.schema.knowledge_graph import compile_edges_to_relation_patterns
def validate_financial_schema_configs() -> None:
    """Raise ValueError with a bullet list when any check fails."""

    errors: list[str] = []
    errors.extend(_validate_concepts_and_packs())
    errors.extend(_validate_categorical_allowed_values())
    errors.extend(_validate_concept_relation_graph())
    errors.extend(_validate_system_profiles())
    errors.extend(_validate_domain_extension_rules())
    errors.extend(_validate_business_lines())
    errors.extend(_validate_domain_taxonomy())
    errors.extend(_validate_domain_signal_inference_rules())
    errors.extend(_validate_concepts_primary_domain())
    errors.extend(_validate_cardinality_generation_topology())
    errors.extend(_validate_generation_config_manifest())
    errors.extend(_validate_scenario_overlays())
    errors.extend(_validate_lifecycle_constraints())
    errors.extend(_validate_status_value_normalization())
    errors.extend(_validate_column_profiles_unified())
    errors.extend(_validate_column_semantics_profiles())
    errors.extend(_validate_json_object_packs())
    errors.extend(_validate_distribution_baselines())
    errors.extend(_validate_declarative_generation_rules())
    if errors:
        joined = "\n".join(f"  - {item}" for item in errors)
        raise ValueError(f"Schema config validation failed:\n{joined}")


def _validate_concepts_and_packs() -> list[str]:
    errors: list[str] = []
    concepts_payload = load_schema_config("concepts.json")
    field_packs_payload = load_schema_config("field_packs.json")
    packs = field_packs_payload.get("packs", {})
    if not isinstance(packs, dict):
        return ["field_packs.json: packs must be an object"]

    concepts = concepts_payload.get("concepts", [])
    seen_ids: set[str] = set()
    seen_tables: set[str] = set()
    for concept in concepts:
        concept_id = str(concept.get("concept_id", ""))
        if not concept_id:
            errors.append("concept entry missing concept_id")
            continue
        if concept_id in seen_ids:
            errors.append(f"duplicate concept_id: {concept_id}")
        seen_ids.add(concept_id)
        table_key = str(concept.get("default_table_name", "")).lower()
        if table_key:
            if table_key in seen_tables:
                errors.append(f"duplicate default_table_name: {concept.get('default_table_name')}")
            seen_tables.add(table_key)
        for pack_name in list(concept.get("required_packs", [])):
            if str(pack_name) not in packs:
                errors.append(f"concept {concept_id}: required_packs references missing pack {pack_name!r}")
        for pack_name in list(concept.get("optional_packs", [])):
            if str(pack_name) not in packs:
                errors.append(f"concept {concept_id}: optional_packs references missing pack {pack_name!r}")
    return errors


def _validate_categorical_allowed_values() -> list[str]:
    """allowed_values only for categorical; non-empty unique strings when present."""

    errors: list[str] = []
    packs = load_schema_config("field_packs.json").get("packs", {})
    if isinstance(packs, dict):
        for pack_name, columns in packs.items():
            if not isinstance(columns, list):
                continue
            for col_idx, col in enumerate(columns):
                if not isinstance(col, dict):
                    continue
                nt = str(col.get("normalized_type", ""))
                av = col.get("allowed_values")
                if av is None:
                    continue
                if nt != "categorical":
                    errors.append(
                        f"field_packs[{pack_name!r}] column[{col_idx}] {col.get('name')!r}: "
                        "allowed_values only allowed when normalized_type is categorical"
                    )
                    continue
                if not isinstance(av, list):
                    errors.append(
                        f"field_packs[{pack_name!r}] column[{col_idx!r}]: allowed_values must be a JSON array"
                    )
                    continue
                vals = [str(x).strip() for x in av if str(x).strip()]
                if not vals:
                    errors.append(
                        f"field_packs[{pack_name!r}] column {col.get('name')!r}: "
                        "allowed_values must be non-empty when present"
                    )
                    continue
                if len(vals) != len(set(vals)):
                    errors.append(
                        f"field_packs[{pack_name!r}] column {col.get('name')!r}: allowed_values contains duplicates"
                    )

    return errors


def _validate_concept_relation_graph() -> list[str]:
    errors: list[str] = []
    concepts_payload = load_schema_config("concepts.json")
    concept_by_id = {str(c["concept_id"]): c for c in concepts_payload.get("concepts", [])}
    valid_ids = set(concept_by_id.keys())
    try:
        kg_payload = load_schema_config("concept_relation_graph.json")
    except OSError:
        return ["concept_relation_graph.json: file missing"]
    nodes_obj = kg_payload.get("nodes", [])
    if not isinstance(nodes_obj, list):
        return ["concept_relation_graph.json: nodes must be a list"]
    for index, node in enumerate(nodes_obj):
        if not isinstance(node, dict):
            errors.append(f"concept_relation_graph.nodes[{index}]: value must be an object")
            continue
        cid = str(node.get("concept_id", "")).strip()
        if cid and cid not in valid_ids:
            errors.append(f"concept_relation_graph.nodes[{index}]: unknown concept_id {cid!r}")
    edges_obj = kg_payload.get("edges", [])
    if not isinstance(edges_obj, list):
        return ["concept_relation_graph.json: edges must be a list"]
    edge_seen: set[tuple[str, str]] = set()
    for index, edge in enumerate(edges_obj):
        if not isinstance(edge, dict):
            errors.append(f"concept_relation_graph.edges[{index}]: value must be an object")
            continue
        from_c = str(edge.get("from_concept", "")).strip()
        to_c = str(edge.get("to_concept", "")).strip()
        if not from_c or from_c not in valid_ids:
            errors.append(f"concept_relation_graph.edges[{index}]: unknown or missing from_concept {from_c!r}")
        if not to_c or to_c not in valid_ids:
            errors.append(f"concept_relation_graph.edges[{index}]: unknown or missing to_concept {to_c!r}")
        fk_raw = edge.get("fk")
        fk = fk_raw if isinstance(fk_raw, dict) else {}
        if not str(fk.get("child_column", "")).strip() or not str(fk.get("parent_column", "")).strip():
            errors.append(
                f"concept_relation_graph.edges[{index}] {edge.get('id', '')!r}: "
                "fk.child_column and fk.parent_column are required"
            )
        dup_key = (from_c, to_c)
        if from_c and to_c and dup_key in edge_seen:
            errors.append(
                f"concept_relation_graph.edges[{index}]: duplicate edge from_concept={from_c!r} to_concept={to_c!r}"
            )
        if from_c and to_c:
            edge_seen.add(dup_key)
    relations = compile_edges_to_relation_patterns(concepts_payload=concepts_payload, kg_payload=kg_payload)
    for index, relation in enumerate(relations):
        parent_concept = str(relation.get("parent_concept", ""))
        child_concept = str(relation.get("child_concept", ""))
        if parent_concept in concept_by_id:
            expected_table = str(concept_by_id[parent_concept].get("default_table_name", ""))
            actual_table = str(relation.get("parent_table", ""))
            if expected_table and actual_table and expected_table != actual_table:
                errors.append(
                    f"concept_relation_graph compiled[{index}]: parent_table {actual_table!r} != "
                    f"concept {parent_concept!r} default_table_name {expected_table!r}"
                )
        if child_concept in concept_by_id:
            expected_table = str(concept_by_id[child_concept].get("default_table_name", ""))
            actual_table = str(relation.get("child_table", ""))
            if expected_table and actual_table and expected_table != actual_table:
                errors.append(
                    f"concept_relation_graph compiled[{index}]: child_table {actual_table!r} != "
                    f"concept {child_concept!r} default_table_name {expected_table!r}"
                )
    return errors


def _validate_system_profiles() -> list[str]:
    errors: list[str] = []
    concepts_payload = load_schema_config("concepts.json")
    valid_ids = {str(c["concept_id"]) for c in concepts_payload.get("concepts", [])}
    profiles = load_schema_config("system_profiles.json").get("profiles", {})
    for system_name, profile in profiles.items():
        default_concepts = [str(x) for x in list(profile.get("default_concepts", []))]
        required_concepts = [str(x) for x in list(profile.get("required_concepts", []))]
        default_set = set(default_concepts)
        for concept_id in default_concepts:
            if concept_id not in valid_ids:
                errors.append(f"system_profiles[{system_name!r}]: unknown default_concepts entry {concept_id!r}")
        for concept_id in required_concepts:
            if concept_id not in valid_ids:
                errors.append(f"system_profiles[{system_name!r}]: unknown required_concepts entry {concept_id!r}")
            if concept_id not in default_set:
                errors.append(
                    f"system_profiles[{system_name!r}]: required_concepts {concept_id!r} "
                    "must appear in default_concepts"
                )
    return errors


def _validate_business_lines() -> list[str]:
    errors: list[str] = []
    concepts_payload = load_schema_config("concepts.json")
    valid_ids = {str(c["concept_id"]) for c in concepts_payload.get("concepts", [])}
    profile_keys = set(load_schema_config("system_profiles.json").get("profiles", {}).keys())
    try:
        payload = load_schema_config("business_lines.json")
    except OSError:
        return ["business_lines.json: file missing"]
    lines_obj = payload.get("lines", {})
    if not isinstance(lines_obj, dict):
        return ["business_lines.json: lines must be an object"]
    for line_key, line_body in lines_obj.items():
        lk = str(line_key)
        if lk not in profile_keys:
            errors.append(
                f"business_lines.lines[{lk!r}]: no matching system_profiles profile; "
                "keys must align with system_profiles.json ids"
            )
        if not isinstance(line_body, dict):
            errors.append(f"business_lines.lines[{lk!r}]: value must be an object")
            continue
        for concept_id in list(line_body.get("when_matched_contains_all", [])):
            if str(concept_id) not in valid_ids:
                errors.append(
                    f"business_lines.lines[{lk!r}]: unknown when_matched_contains_all {concept_id!r}"
                )
        for concept_id in list(line_body.get("append_concepts", [])):
            if str(concept_id) not in valid_ids:
                errors.append(f"business_lines.lines[{lk!r}]: unknown append_concepts {concept_id!r}")
    return errors


def _validate_domain_extension_rules() -> list[str]:
    errors: list[str] = []
    concepts_payload = load_schema_config("concepts.json")
    valid_ids = {str(c["concept_id"]) for c in concepts_payload.get("concepts", [])}
    try:
        payload = load_schema_config("domain_extension_rules.json")
    except OSError:
        return ["domain_extension_rules.json: file missing"]
    for index, rule in enumerate(list(payload.get("rules", []))):
        for concept_id in list(rule.get("when_matched_contains_all", [])):
            if str(concept_id) not in valid_ids:
                errors.append(
                    f"domain_extension_rules[{index}] {rule.get('id')!r}: "
                    f"unknown when_matched_contains_all {concept_id!r}"
                )
        for concept_id in list(rule.get("append_concepts", [])):
            if str(concept_id) not in valid_ids:
                errors.append(
                    f"domain_extension_rules[{index}] {rule.get('id')!r}: unknown append_concepts {concept_id!r}"
                )
    return errors


def _validate_domain_taxonomy() -> list[str]:
    """Level-1 financial knowledge taxonomy: unique domain_id, optional level2 shape, legacy bundle refs."""

    errors: list[str] = []
    try:
        payload = load_schema_config("domain_taxonomy.json")
    except OSError:
        return ["domain_taxonomy.json: file missing"]
    meta = payload.get("meta", {})
    if not isinstance(meta, dict):
        return ["domain_taxonomy.json: meta must be an object"]
    if not str(meta.get("version", "")).strip():
        errors.append("domain_taxonomy.json: meta.version is required")

    level1 = payload.get("level1_domains", [])
    if not isinstance(level1, list) or not level1:
        errors.append("domain_taxonomy.json: level1_domains must be a non-empty array")
        return errors

    seen_domain: set[str] = set()
    for idx, domain in enumerate(level1):
        if not isinstance(domain, dict):
            errors.append(f"domain_taxonomy.json: level1_domains[{idx}] must be an object")
            continue
        did = str(domain.get("domain_id", "")).strip()
        if not did:
            errors.append(f"domain_taxonomy.json: level1_domains[{idx}]: domain_id is required")
            continue
        if did in seen_domain:
            errors.append(f"domain_taxonomy.json: duplicate domain_id {did!r}")
        seen_domain.add(did)
        if not str(domain.get("title_zh", "")).strip():
            errors.append(f"domain_taxonomy.json: domain {did!r}: title_zh is required")
        if not str(domain.get("summary", "")).strip():
            errors.append(f"domain_taxonomy.json: domain {did!r}: summary is required")
        sk = domain.get("signal_keywords")
        if sk is not None:
            if not isinstance(sk, list) or not sk:
                errors.append(f"domain_taxonomy.json: domain {did!r}: signal_keywords must be a non-empty array when present")
            else:
                for kw in sk:
                    if not str(kw).strip():
                        errors.append(f"domain_taxonomy.json: domain {did!r}: signal_keywords contains empty entry")
        l2 = domain.get("level2_themes", [])
        if l2 is not None and not isinstance(l2, list):
            errors.append(f"domain_taxonomy.json: domain {did!r}: level2_themes must be an array when present")
        elif isinstance(l2, list):
            for j, theme in enumerate(l2):
                if not isinstance(theme, dict):
                    errors.append(f"domain_taxonomy.json: domain {did!r} level2[{j}] must be an object")
                    continue
                if not str(theme.get("theme_id", "")).strip():
                    errors.append(f"domain_taxonomy.json: domain {did!r} level2[{j}]: theme_id is required")
                if not str(theme.get("title_zh", "")).strip():
                    errors.append(f"domain_taxonomy.json: domain {did!r} level2[{j}]: title_zh is required")
                tsk = theme.get("signal_keywords")
                if tsk is not None:
                    if not isinstance(tsk, list):
                        errors.append(
                            f"domain_taxonomy.json: domain {did!r} level2[{j}]: signal_keywords must be an array when present"
                        )
                    else:
                        for kw in tsk:
                            if not str(kw).strip():
                                errors.append(
                                    f"domain_taxonomy.json: domain {did!r} level2[{j}]: signal_keywords contains empty entry"
                                )

    legacy = payload.get("legacy_bundle_to_domains", {})
    if legacy is not None:
        if not isinstance(legacy, dict):
            errors.append("domain_taxonomy.json: legacy_bundle_to_domains must be an object")
        else:
            mapping = {k: v for k, v in legacy.items() if k != "description"}
            for bundle_key, domain_list in mapping.items():
                if not isinstance(domain_list, list):
                    errors.append(f"domain_taxonomy.json: legacy_bundle_to_domains[{bundle_key!r}] must be an array")
                    continue
                for ref in domain_list:
                    r = str(ref).strip()
                    if r and r not in seen_domain:
                        errors.append(
                            f"domain_taxonomy.json: legacy_bundle_to_domains[{bundle_key!r}] "
                            f"references unknown domain_id {r!r}"
                        )

    return errors


def _validate_domain_signal_inference_rules() -> list[str]:
    """domain_signal_inference_rules.json references valid domains, themes, and concept ids."""

    errors: list[str] = []
    try:
        tax = load_schema_config("domain_taxonomy.json")
    except OSError:
        return []
    domain_ids = {str(d.get("domain_id", "")).strip() for d in tax.get("level1_domains", []) if isinstance(d, dict)}
    domain_ids.discard("")
    theme_ids: set[str] = set()
    for d in tax.get("level1_domains", []):
        if not isinstance(d, dict):
            continue
        for th in d.get("level2_themes", []) or []:
            if isinstance(th, dict):
                tid = str(th.get("theme_id", "")).strip()
                if tid:
                    theme_ids.add(tid)

    concepts_payload = load_schema_config("concepts.json")
    concept_ids = {str(c.get("concept_id", "")) for c in concepts_payload.get("concepts", []) if isinstance(c, dict)}
    concept_ids.discard("")

    try:
        payload = load_schema_config("domain_signal_inference_rules.json")
    except OSError:
        return ["domain_signal_inference_rules.json: file missing"]
    rules = payload.get("rules", [])
    if not isinstance(rules, list):
        return ["domain_signal_inference_rules.json: rules must be an array"]

    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append(f"domain_signal_inference_rules.json: rules[{idx}] must be an object")
            continue
        rid = str(rule.get("id", "")).strip() or f"rules[{idx}]"
        for did in list(rule.get("when_domains_matched_all", [])):
            ds = str(did).strip()
            if ds and ds not in domain_ids:
                errors.append(f"{rid}: unknown when_domains_matched_all domain_id {ds!r}")
        for tid in list(rule.get("when_themes_match_any", [])):
            ts = str(tid).strip()
            if ts and ts not in theme_ids:
                errors.append(f"{rid}: unknown when_themes_match_any theme_id {ts!r}")
        for cid in list(rule.get("append_concepts", [])):
            cs = str(cid).strip()
            if cs and cs not in concept_ids:
                errors.append(f"{rid}: unknown append_concepts {cs!r}")
    return errors


def _validate_concepts_primary_domain() -> list[str]:
    """Each concept must declare primary_domain_id exactly once, referencing domain_taxonomy.json."""

    errors: list[str] = []
    try:
        taxonomy = load_schema_config("domain_taxonomy.json")
    except OSError:
        return []
    level1 = taxonomy.get("level1_domains", [])
    if not isinstance(level1, list):
        return []
    valid_ids = {str(d.get("domain_id", "")).strip() for d in level1 if isinstance(d, dict)}
    valid_ids.discard("")

    concepts_payload = load_schema_config("concepts.json")
    for concept in concepts_payload.get("concepts", []):
        if not isinstance(concept, dict):
            continue
        cid = str(concept.get("concept_id", ""))
        pid = str(concept.get("primary_domain_id", "")).strip()
        if not pid:
            errors.append(f"concept {cid!r}: primary_domain_id is required")
            continue
        if pid not in valid_ids:
            errors.append(f"concept {cid!r}: primary_domain_id {pid!r} not in domain_taxonomy.json level1_domains")
        theme_ids = concept.get("theme_ids")
        if theme_ids is not None:
            if not isinstance(theme_ids, list):
                errors.append(f"concept {cid!r}: theme_ids must be an array when present")
                continue
            for raw_tid in theme_ids:
                tid = str(raw_tid).strip()
                if not _theme_id_exists_in_taxonomy(taxonomy=taxonomy, theme_id=tid):
                    errors.append(f"concept {cid!r}: unknown theme_id {tid!r} (must match a level2 theme_id under some domain)")
    return errors


def _validate_generation_config_manifest() -> list[str]:
    errors: list[str] = []
    try:
        manifest = load_schema_config(SYNTH_MANIFEST)
    except OSError as exc:
        return [f"{SYNTH_MANIFEST}: {exc}"]
    version = manifest.get("config_version", "")
    if not str(version).strip():
        errors.append(f"{SYNTH_MANIFEST}: config_version should be non-empty")
    topology = load_schema_config(SYNTH_TOPOLOGY)
    graph_paths = topology.get("graph_event_paths", [])
    graph_edges = load_schema_config("concept_relation_graph.json").get("edges", [])
    edge_ids = {
        str(edge.get("id", ""))
        for edge in graph_edges
        if isinstance(edge, dict) and edge.get("id")
    }
    if isinstance(graph_paths, list):
        for index, path in enumerate(graph_paths):
            if not isinstance(path, dict):
                continue
            for eid in path.get("edge_ids", []) or []:
                if str(eid) not in edge_ids:
                    errors.append(
                        f"{SYNTH_TOPOLOGY} graph_event_paths[{index}]: unknown edge_id {eid!r}"
                    )
    sem_default = str(manifest.get("column_semantics_profile_id_default", "") or "").strip()
    if sem_default:
        sem_payload = load_schema_config(SYNTH_COLUMN_PROFILES)
        sem_profiles = sem_payload.get("profiles", {})
        if isinstance(sem_profiles, dict) and sem_default not in sem_profiles:
            errors.append(
                f"{SYNTH_MANIFEST}: column_semantics_profile_id_default {sem_default!r} "
                f"not found in {SYNTH_COLUMN_PROFILES} profiles"
            )
    return errors


def _validate_column_semantics_profiles() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config("column_semantics_profiles.json")
    except OSError:
        return errors
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        errors.append("column_semantics_profiles.json: profiles must be an object")
        return errors
    default_id = str(payload.get("default_profile_id", "") or "").strip()
    if default_id and default_id not in profiles:
        errors.append(
            f"column_semantics_profiles.json: default_profile_id {default_id!r} not found under profiles"
        )
    known_rules = {
        "SOFT_ACCOUNT_TYPE_ENUM",
        "SOFT_CUSTOMER_AGE_RANGE",
        "SOFT_COUNTRY_ENUM",
        "HARD_TRANSACTION_AMOUNT_POSITIVE",
        "HARD_TRANSACTION_CURRENCY_ENUM",
    }
    for profile_id, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        columns = profile.get("columns", {})
        if not isinstance(columns, dict):
            continue
        for col_key, spec in columns.items():
            if not isinstance(spec, dict):
                continue
            for rid in spec.get("aligned_validation_rule_ids", []) or []:
                rs = str(rid)
                if rs and rs not in known_rules:
                    errors.append(
                        f"column_semantics_profiles.json profile {profile_id!r} column {col_key!r}: "
                        f"unknown aligned_validation_rule_ids entry {rs!r} (add to validator allowlist when introducing new rules)"
                    )
    return errors


def _validate_column_profiles_unified() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config(SYNTH_COLUMN_PROFILES)
    except Exception:
        return errors
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        return [f"{SYNTH_COLUMN_PROFILES}: profiles must be an object"]
    for profile_id, profile in profiles.items():
        if not isinstance(profile, dict):
            errors.append(f"{SYNTH_COLUMN_PROFILES} profile {profile_id!r}: must be object")
            continue
        baseline = profile.get("evaluation_baselines")
        if baseline is not None and not isinstance(baseline, dict):
            errors.append(
                f"{SYNTH_COLUMN_PROFILES} profile {profile_id!r}: evaluation_baselines must be object when present"
            )
    return errors


def _validate_scenario_overlays() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config(SYNTH_SCENARIO_OVERLAYS)
    except Exception as exc:
        return [f"{SYNTH_SCENARIO_OVERLAYS}: {exc}"]
    scenarios = payload.get("scenarios", {})
    if not isinstance(scenarios, dict):
        return [f"{SYNTH_SCENARIO_OVERLAYS}: scenarios must be an object"]
    for scenario_id, scenario in scenarios.items():
        if not isinstance(scenario, dict):
            errors.append(f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r}: must be object")
            continue
        conditions = scenario.get("activation_conditions", {})
        if conditions is not None and not isinstance(conditions, dict):
            errors.append(
                f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r}: activation_conditions must be object when present"
            )
        if isinstance(conditions, dict):
            for key in (
                "requires_tables",
                "domain_ids",
                "concept_path_ids",
                "table_feature_flags",
                "requires_columns",
            ):
                value = conditions.get(key)
                if value is not None and not isinstance(value, list):
                    errors.append(
                        f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r}: activation_conditions.{key} must be array"
                    )
            required_values = conditions.get("requires_column_values")
            if required_values is not None and not isinstance(required_values, dict):
                errors.append(
                    f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r}: activation_conditions.requires_column_values must be object"
                )
        dist_map = scenario.get("account_type_distributions", {})
        if not isinstance(dist_map, dict) or not dist_map:
            errors.append(
                f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r}: account_type_distributions must be non-empty object"
            )
            continue
        default_rule = dist_map.get("default", {})
        default_allowed_values = (
            default_rule.get("allowed_values", [])
            if isinstance(default_rule, dict)
            else []
        )
        for segment_id, rule in dist_map.items():
            if not isinstance(rule, dict):
                errors.append(
                    f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r} segment {segment_id!r}: must be object"
                )
                continue
            allowed_values = rule.get("allowed_values")
            if (
                (not isinstance(allowed_values, list) or not allowed_values)
                and segment_id != "default"
                and isinstance(default_allowed_values, list)
                and default_allowed_values
            ):
                allowed_values = default_allowed_values
            weights = rule.get("weights", {})
            if not isinstance(allowed_values, list) or not allowed_values:
                errors.append(
                    f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r} segment {segment_id!r}: "
                    "allowed_values must be non-empty array"
                )
                continue
            if not isinstance(weights, dict) or not weights:
                errors.append(
                    f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r} segment {segment_id!r}: "
                    "weights must be non-empty object"
                )
                continue
            for value in allowed_values:
                if str(value) not in weights:
                    errors.append(
                        f"{SYNTH_SCENARIO_OVERLAYS} scenario {scenario_id!r} segment {segment_id!r}: "
                        f"missing weight for allowed value {value!r}"
                    )
    return errors


def _validate_lifecycle_constraints() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    except OSError as exc:
        return [f"{SYNTH_LIFECYCLE_CONSTRAINTS}: {exc}"]
    if not isinstance(payload, dict):
        return [f"{SYNTH_LIFECYCLE_CONSTRAINTS}: root must be object"]

    for index, rule in enumerate(payload.get("state_machine_rules", []) or []):
        if not isinstance(rule, dict):
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: must be object")
            continue
        for key in ("rule_id", "table_name", "status_column", "sequence_time_column"):
            if not str(rule.get(key, "")).strip():
                errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: {key} is required")
        entity_keys = rule.get("entity_key_columns", [])
        if not isinstance(entity_keys, list) or not entity_keys:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: entity_key_columns must be non-empty array"
            )
        initial_states = rule.get("initial_states", [])
        if initial_states is not None and not isinstance(initial_states, list):
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: initial_states must be array")
        singleton_allowed_states = rule.get("singleton_allowed_states", [])
        if singleton_allowed_states is not None and not isinstance(singleton_allowed_states, list):
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: singleton_allowed_states must be array"
            )
        allowed = rule.get("allowed_transitions", {})
        if not isinstance(allowed, dict) or not allowed:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: allowed_transitions must be non-empty object"
            )
        else:
            for from_state, to_states in allowed.items():
                if not str(from_state).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: transition from_state is empty"
                    )
                if not isinstance(to_states, list):
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} state_machine_rules[{index}]: "
                        "allowed_transitions values must be arrays"
                    )

    for index, rule in enumerate(payload.get("temporal_order_rules", []) or []):
        if not isinstance(rule, dict):
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}]: must be object")
            continue
        if not str(rule.get("rule_id", "")).strip():
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}]: rule_id is required")
        if not str(rule.get("table_name", "")).strip():
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}]: table_name is required")
        constraints = rule.get("constraints", [])
        if not isinstance(constraints, list) or not constraints:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}]: constraints must be non-empty array"
            )
            continue
        for c_index, item in enumerate(constraints):
            if not isinstance(item, dict):
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}].constraints[{c_index}]: must be object"
                )
                continue
            left_column = str(item.get("left_column", "")).strip()
            right_column = str(item.get("right_column", "")).strip()
            operator = str(item.get("operator", "")).strip()
            if not left_column or not right_column:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}].constraints[{c_index}]: "
                    "left_column/right_column are required"
                )
            if operator not in {"<=", "<"}:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}].constraints[{c_index}]: "
                    "operator must be <= or <"
                )
            apply_when_column = str(item.get("apply_when_column", "")).strip()
            apply_when_in = item.get("apply_when_in")
            if apply_when_column and (not isinstance(apply_when_in, list) or not apply_when_in):
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} temporal_order_rules[{index}].constraints[{c_index}]: "
                    "apply_when_in must be non-empty array when apply_when_column is provided"
                )

    for index, rule in enumerate(payload.get("cross_table_temporal_rules", []) or []):
        if not isinstance(rule, dict):
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules[{index}]: must be object")
            continue
        for key in (
            "rule_id",
            "left_table_name",
            "left_time_column",
            "right_table_name",
            "right_time_column",
        ):
            if not str(rule.get(key, "")).strip():
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules[{index}]: {key} is required"
                )
        left_keys = rule.get("left_key_columns", [])
        right_keys = rule.get("right_foreign_key_columns", [])
        if not isinstance(left_keys, list) or not left_keys:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules[{index}]: left_key_columns must be non-empty array"
            )
        if not isinstance(right_keys, list) or not right_keys:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules"
                f"[{index}]: right_foreign_key_columns must be non-empty array"
            )
        if isinstance(left_keys, list) and isinstance(right_keys, list) and len(left_keys) != len(right_keys):
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules[{index}]: "
                "left_key_columns and right_foreign_key_columns must have same length"
            )
        operator = str(rule.get("operator", "")).strip()
        if operator not in {"<=", "<"}:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} cross_table_temporal_rules[{index}]: operator must be <= or <"
            )

    for index, rule in enumerate(payload.get("business_conservation_rules", []) or []):
        if not isinstance(rule, dict):
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: must be object")
            continue
        rule_id = str(rule.get("rule_id", "")).strip()
        if not rule_id:
            errors.append(f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: rule_id is required")
        rule_type = str(rule.get("type", "")).strip()
        if rule_type == "intra_row_numeric_compare":
            for key in ("table_name", "left_column", "right_column"):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
            if str(rule.get("operator", "")).strip() not in {"<=", "<", ">=", ">"}:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: "
                    "operator must be one of <=,<,>=,>"
                )
        elif rule_type == "intra_row_numeric_range":
            for key in ("table_name", "column_name"):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
        elif rule_type == "state_requires_non_null_time":
            for key in ("table_name", "state_column", "time_column"):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
            required_states = rule.get("required_states", [])
            if not isinstance(required_states, list) or not required_states:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: required_states must be non-empty array"
                )
        elif rule_type == "aggregate_child_amount_le_parent_limit":
            for key in (
                "parent_table_name",
                "parent_key_column",
                "parent_limit_column",
                "child_table_name",
                "child_fk_column",
                "child_amount_column",
            ):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
        elif rule_type == "current_state_matches_parent_status":
            for key in (
                "parent_table_name",
                "parent_key_column",
                "parent_status_column",
                "history_table_name",
                "history_fk_column",
                "history_status_column",
                "history_current_flag_column",
                "history_current_flag_value",
            ):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
        elif rule_type == "current_flag_must_be_latest_time":
            for key in (
                "table_name",
                "entity_key_column",
                "time_column",
                "current_flag_column",
                "current_flag_value",
            ):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
        elif rule_type == "child_state_requires_parent_json_value_in":
            for key in (
                "parent_table_name",
                "parent_key_column",
                "parent_json_column",
                "parent_json_path",
                "child_table_name",
                "child_fk_column",
                "child_state_column",
            ):
                if not str(rule.get(key, "")).strip():
                    errors.append(
                        f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: {key} is required"
                    )
            restricted_states = rule.get("restricted_child_states", [])
            if not isinstance(restricted_states, list) or not restricted_states:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: "
                    "restricted_child_states must be non-empty array"
                )
            allowed_parent_values = rule.get("allowed_parent_values", [])
            if not isinstance(allowed_parent_values, list) or not allowed_parent_values:
                errors.append(
                    f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: "
                    "allowed_parent_values must be non-empty array"
                )
        else:
            errors.append(
                f"{SYNTH_LIFECYCLE_CONSTRAINTS} business_conservation_rules[{index}]: unsupported type {rule_type!r}"
            )

    return errors


def _validate_status_value_normalization() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config(SYNTH_STATUS_NORMALIZATION)
    except OSError:
        return errors
    if not isinstance(payload, dict):
        return [f"{SYNTH_STATUS_NORMALIZATION}: root must be object"]
    rules = payload.get("rules", [])
    if not isinstance(rules, list):
        return [f"{SYNTH_STATUS_NORMALIZATION}: rules must be array"]
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append(f"{SYNTH_STATUS_NORMALIZATION} rules[{index}]: must be object")
            continue
        if not str(rule.get("table_name", "")).strip():
            errors.append(f"{SYNTH_STATUS_NORMALIZATION} rules[{index}]: table_name is required")
        if not str(rule.get("column_name", "")).strip():
            errors.append(f"{SYNTH_STATUS_NORMALIZATION} rules[{index}]: column_name is required")
        canonical_values = rule.get("canonical_values", {})
        if not isinstance(canonical_values, dict) or not canonical_values:
            errors.append(
                f"{SYNTH_STATUS_NORMALIZATION} rules[{index}]: canonical_values must be non-empty object"
            )
            continue
        for canonical_key, aliases in canonical_values.items():
            if not str(canonical_key).strip():
                errors.append(
                    f"{SYNTH_STATUS_NORMALIZATION} rules[{index}]: canonical key must be non-empty string"
                )
            if not isinstance(aliases, list) or not aliases:
                errors.append(
                    f"{SYNTH_STATUS_NORMALIZATION} rules[{index}] canonical {canonical_key!r}: aliases must be non-empty array"
                )
    return errors


def _validate_json_object_packs() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config(SYNTH_JSON_OBJECT_PACKS)
    except Exception as exc:
        return [f"{SYNTH_JSON_OBJECT_PACKS}: {exc}"]
    packs = payload.get("packs", {})
    if not isinstance(packs, dict):
        return [f"{SYNTH_JSON_OBJECT_PACKS}: packs must be an object"]
    field_packs = load_schema_config("field_packs.json").get("packs", {})
    for pack_id, pack in packs.items():
        if not isinstance(pack, dict):
            errors.append(f"{SYNTH_JSON_OBJECT_PACKS} pack {pack_id!r}: pack must be object")
            continue
        fields = pack.get("fields", [])
        if not isinstance(fields, list):
            errors.append(f"{SYNTH_JSON_OBJECT_PACKS} pack {pack_id!r}: fields must be array")
            continue
        for item in fields:
            if not isinstance(item, dict):
                continue
            path = str(item.get("path", ""))
            generator = item.get("generator", {})
            if not path or not isinstance(generator, dict):
                errors.append(f"{SYNTH_JSON_OBJECT_PACKS} pack {pack_id!r}: each field needs path and generator")
                continue
            if str(generator.get("type", "")) == "from_field_pack_allowed_values":
                ref_pack = str(generator.get("field_pack_id", ""))
                ref_field = str(generator.get("field_name", ""))
                if not isinstance(field_packs, dict) or ref_pack not in field_packs:
                    errors.append(
                        f"{SYNTH_JSON_OBJECT_PACKS} pack {pack_id!r} field {path!r}: "
                        f"field_pack_id {ref_pack!r} not found in field_packs.json"
                    )
                    continue
                cols = field_packs.get(ref_pack, [])
                ok = False
                if isinstance(cols, list):
                    for col in cols:
                        if (
                            isinstance(col, dict)
                            and str(col.get("name", "")).lower() == ref_field.lower()
                            and isinstance(col.get("allowed_values"), list)
                            and col.get("allowed_values")
                        ):
                            ok = True
                            break
                if not ok:
                    errors.append(
                        f"{SYNTH_JSON_OBJECT_PACKS} pack {pack_id!r} field {path!r}: "
                        f"field {ref_field!r} has no allowed_values in {ref_pack!r}"
                    )
    return errors


def _validate_declarative_generation_rules() -> list[str]:
    errors: list[str] = []
    try:
        payload = _load_generation_rules_for_validation()
    except OSError as exc:
        return [f"{SYNTH_GENERATION_RULES}/declarative_generation_rules.json: {exc}"]
    if "active_generation_rule_pack_ids" in payload:
        errors.append(
            "declarative_generation_rules.json: deprecated key 'active_generation_rule_pack_ids' is not allowed; "
            "use 'generation_behavior_pack_ids'"
        )
    if "default_pack_ids" in payload:
        errors.append(
            "declarative_generation_rules.json: deprecated key 'default_pack_ids' is not allowed"
        )
    if "packs" in payload:
        errors.append(
            "declarative_generation_rules.json: deprecated key 'packs' is not allowed; use 'behavior_packs'"
        )
    machines = payload.get("state_machines", [])
    if machines is not None and not isinstance(machines, list):
        errors.append("declarative_generation_rules.json: state_machines must be array when present")

    packs = _load_generation_rule_packs_for_validation()
    pack_ids = set(packs.keys()) if isinstance(packs, dict) else set()
    active_pack_ids = payload.get("generation_behavior_pack_ids", [])
    for raw_id in active_pack_ids or []:
        pid = str(raw_id)
        if pid and pid not in pack_ids:
            errors.append(
                f"declarative_generation_rules.json: generation_behavior_pack_ids references unknown pack {pid!r}"
            )
    lifecycle = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    lifecycle_ids: set[str] = set()
    for raw_rule in lifecycle.get("state_machine_rules", []) or []:
        if isinstance(raw_rule, dict):
            rid = str(raw_rule.get("rule_id", "")).strip()
            if rid:
                lifecycle_ids.add(rid)
    for index, binding in enumerate(payload.get("column_to_state_machine", []) or []):
        if not isinstance(binding, dict):
            continue
        lifecycle_rule_id = str(binding.get("lifecycle_rule_id", "") or "")
        if not lifecycle_rule_id:
            errors.append(
                f"declarative_generation_rules.json column_to_state_machine[{index}]: "
                "lifecycle_rule_id is required"
            )
            continue
        if lifecycle_rule_id not in lifecycle_ids:
            errors.append(
                f"declarative_generation_rules.json column_to_state_machine[{index}]: "
                f"unknown lifecycle_rule_id {lifecycle_rule_id!r}"
            )
    return errors


def _load_generation_rules_for_validation() -> dict[str, object]:
    payload = load_schema_config(SYNTH_GENERATION_RULES)
    return payload if isinstance(payload, dict) else {}


def _load_generation_rule_packs_for_validation() -> dict[str, object]:
    payload = load_schema_config(SYNTH_GENERATION_RULES)
    packs = payload.get("behavior_packs", {})
    return packs if isinstance(packs, dict) else {}


def _validate_distribution_baselines() -> list[str]:
    errors: list[str] = []
    try:
        payload = load_schema_config("distribution_baselines.json")
    except OSError:
        return errors
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        return ["distribution_baselines.json: profiles must be an object"]
    for profile_id, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        columns = profile.get("columns", {})
        if not isinstance(columns, dict):
            continue
        for table_column, baseline in columns.items():
            if not isinstance(baseline, dict):
                continue
            baseline_type = str(baseline.get("type", ""))
            if baseline_type == "categorical_probs":
                probs = baseline.get("probs", {})
                if not isinstance(probs, dict) or not probs:
                    errors.append(
                        f"distribution_baselines.json profile {profile_id!r} column {table_column!r}: "
                        "categorical_probs requires non-empty probs object"
                    )
            elif baseline_type == "numeric_buckets":
                edges = baseline.get("edges", [])
                probs = baseline.get("probs", [])
                if not isinstance(edges, list) or not isinstance(probs, list):
                    errors.append(
                        f"distribution_baselines.json profile {profile_id!r} column {table_column!r}: "
                        "numeric_buckets requires edges/probs arrays"
                    )
                    continue
                if len(edges) < 2 or len(probs) != len(edges) - 1:
                    errors.append(
                        f"distribution_baselines.json profile {profile_id!r} column {table_column!r}: "
                        "numeric_buckets must satisfy len(probs) == len(edges)-1"
                    )
            else:
                errors.append(
                    f"distribution_baselines.json profile {profile_id!r} column {table_column!r}: "
                    f"unsupported baseline type {baseline_type!r}"
                )
    return errors


def _validate_cardinality_generation_topology() -> list[str]:
    errors: list[str] = []
    card = load_schema_config(SYNTH_CARDINALITY_PROFILES)
    profiles = card.get("profiles", {})
    if not isinstance(profiles, dict):
        errors.append(f"{SYNTH_CARDINALITY_PROFILES}: profiles must be an object")
        return errors
    default_id = str(card.get("default_rowwise_profile_id", "") or "")
    if default_id and default_id not in profiles:
        errors.append(
            f"{SYNTH_CARDINALITY_PROFILES}: default_rowwise_profile_id {default_id!r} not found under profiles"
        )

    concepts_payload = load_schema_config("concepts.json")
    concept_ids = {
        str(c.get("concept_id", ""))
        for c in concepts_payload.get("concepts", [])
        if isinstance(c, dict) and c.get("concept_id")
    }

    gen = load_schema_config(SYNTH_TOPOLOGY)
    chains = gen.get("chains", [])
    if not isinstance(chains, list):
        errors.append(f"{SYNTH_TOPOLOGY}: chains must be a list")
    else:
        for index, chain in enumerate(chains):
            if not isinstance(chain, dict):
                continue
            pid = str(chain.get("cardinality_profile_id", "") or "")
            if pid and pid not in profiles:
                errors.append(
                    f"{SYNTH_TOPOLOGY} chains[{index}]: cardinality_profile_id {pid!r} missing in cardinality_profiles"
                )
            for raw_c in chain.get("concept_path", []) or []:
                cid = str(raw_c)
                if cid and cid not in concept_ids:
                    errors.append(
                        f"{SYNTH_TOPOLOGY} chains[{index}]: concept_path references unknown concept_id {cid!r}"
                    )

    for pid, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        if str(profile.get("type", "")) != "segmented_three_tier":
            continue
        seg_cfg = profile.get("segments", {})
        if not isinstance(seg_cfg, dict) or not seg_cfg:
            errors.append(f"cardinality_profiles {pid!r}: segmented_three_tier needs non-empty segments")
            continue
        for seg_id, seg_payload in seg_cfg.items():
            if not isinstance(seg_payload, dict):
                errors.append(f"cardinality_profiles {pid!r} segment {seg_id!r}: must be an object")
                continue
            for key in ("accounts_per_customer", "transactions_per_account"):
                spec = seg_payload.get(key, {})
                if not isinstance(spec, dict):
                    errors.append(
                        f"cardinality_profiles {pid!r} segment {seg_id!r}: missing {key} object"
                    )
                    continue
                if str(spec.get("type", "")) != "uniform_int":
                    errors.append(
                        f"cardinality_profiles {pid!r} segment {seg_id!r}: {key} type must be uniform_int"
                    )
    return errors


def _theme_id_exists_in_taxonomy(*, taxonomy: dict[str, object], theme_id: str) -> bool:
    for domain in taxonomy.get("level1_domains", []):
        if not isinstance(domain, dict):
            continue
        for theme in domain.get("level2_themes", []) or []:
            if isinstance(theme, dict) and str(theme.get("theme_id", "")).strip() == theme_id:
                return True
    return False


if __name__ == "__main__":
    validate_financial_schema_configs()
    print("financial schema configs OK")
