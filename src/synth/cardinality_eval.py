from __future__ import annotations

import ast

class _CardinalityFormulaEvaluator(ast.NodeVisitor):
    """Safe evaluation: only ``base``, integers, + - * / //, parentheses."""

    def __init__(self, base_value: int) -> None:
        self._base_value = base_value

    def evaluate(self, formula: str) -> int:
        tree = ast.parse(formula.strip(), mode="eval")
        result = self.visit(tree.body)
        if not isinstance(result, int):
            raise ValueError(f"cardinality formula must yield integer, got {result!r}")
        return max(0, result)

    def visit_BinOp(self, node: ast.BinOp) -> int:
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.FloorDiv):
            return left // max(right, 1)
        raise ValueError(f"unsupported operator in cardinality formula: {type(node.op).__name__}")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> int:
        if isinstance(node.op, ast.USub):
            return -self.visit(node.operand)
        raise ValueError("unsupported unary op in cardinality formula")

    def visit_Constant(self, node: ast.Constant) -> int:
        if isinstance(node.value, int):
            return int(node.value)
        raise ValueError(f"only integer constants allowed in cardinality formula, got {node.value!r}")

    def visit_Name(self, node: ast.Name) -> int:
        if node.id == "base":
            return self._base_value
        raise ValueError(f"unknown name in cardinality formula: {node.id}")


def evaluate_cardinality_formula(formula: str, base: int) -> int:
    return _CardinalityFormulaEvaluator(base_value=base).evaluate(formula=formula)


def decide_rowwise_table_count(
    *,
    profile_payload: dict[str, object],
    profile_id: str,
    table_name: str,
    record_count: int,
    has_foreign_keys: bool,
) -> int:
    profiles = profile_payload.get("profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError("cardinality_profiles: missing profiles object")
    profile = profiles.get(_canonical_cardinality_profile_id(profile_id))
    if not isinstance(profile, dict):
        raise ValueError(f"cardinality_profiles: unknown profile_id={profile_id}")

    base = int(record_count)
    profile_type = str(profile.get("type", ""))

    if profile_type == "rule_list":
        rules = profile.get("rules", [])
        if not isinstance(rules, list):
            raise ValueError(f"profile {profile_id}: rules must be a list")
        normalized_name = table_name.lower()
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            cond = rule.get("if", {})
            if not isinstance(cond, dict):
                continue
            if not _rowwise_rule_matches(
                condition=cond,
                table_name_lower=normalized_name,
                has_foreign_keys=has_foreign_keys,
            ):
                continue
            formula = str(rule.get("formula", "base"))
            return evaluate_cardinality_formula(formula=formula, base=base)
        return base

    raise ValueError(f"profile {profile_id}: unsupported type {profile_type}")


def decide_per_concept_count(
    *,
    profile_payload: dict[str, object],
    profile_id: str,
    concept_id: str,
    record_count: int,
) -> int:
    profiles = profile_payload.get("profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError("cardinality_profiles: missing profiles object")
    profile = profiles.get(_canonical_cardinality_profile_id(profile_id))
    if not isinstance(profile, dict):
        raise ValueError(f"cardinality_profiles: unknown profile_id={profile_id}")
    if str(profile.get("type", "")) != "per_concept":
        raise ValueError(f"profile {profile_id}: expected type per_concept")
    per_concept = profile.get("per_concept", {})
    if not isinstance(per_concept, dict):
        raise ValueError(f"profile {profile_id}: missing per_concept")
    formula = per_concept.get(concept_id)
    if formula is None:
        raise ValueError(f"profile {profile_id}: no formula for concept_id={concept_id}")
    base = int(record_count)
    return evaluate_cardinality_formula(formula=str(formula), base=base)


def _rowwise_rule_matches(
    *,
    condition: dict[str, object],
    table_name_lower: str,
    has_foreign_keys: bool,
) -> bool:
    if not condition:
        return True
    if "table_name_lower" in condition:
        if table_name_lower != str(condition["table_name_lower"]).lower():
            return False
    if "has_foreign_keys" in condition:
        if bool(condition["has_foreign_keys"]) != has_foreign_keys:
            return False
    return True


def load_cardinality_profiles_payload() -> dict[str, object]:
    from src.infra.config_store import SYNTH_CARDINALITY_PROFILES, load_schema_config

    payload = load_schema_config(SYNTH_CARDINALITY_PROFILES)
    profiles_raw = payload.get("profiles", {})
    if not isinstance(profiles_raw, dict):
        return payload
    resolved_profiles = _resolve_profiles_with_inheritance(profiles=profiles_raw)
    normalized = dict(payload)
    normalized["profiles"] = resolved_profiles
    return normalized


def get_cardinality_profile_type(*, profile_id: str) -> str:
    payload = load_cardinality_profiles_payload()
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        return ""
    profile = profiles.get(_canonical_cardinality_profile_id(profile_id))
    if not isinstance(profile, dict):
        return ""
    return str(profile.get("type", "") or "")


def _resolve_profiles_with_inheritance(*, profiles: dict[str, object]) -> dict[str, object]:
    resolved: dict[str, object] = {}
    resolving: set[str] = set()

    def resolve(profile_id: str) -> dict[str, object]:
        if profile_id in resolved:
            cached = resolved[profile_id]
            return dict(cached) if isinstance(cached, dict) else {}
        if profile_id in resolving:
            raise ValueError(f"cardinality_profiles: cyclic extends detected at profile_id={profile_id}")
        raw_profile = profiles.get(profile_id)
        if not isinstance(raw_profile, dict):
            raise ValueError(f"cardinality_profiles: unknown profile_id={profile_id}")

        resolving.add(profile_id)
        parent_id_raw = raw_profile.get("extends")
        parent_profile: dict[str, object] = {}
        if isinstance(parent_id_raw, str) and parent_id_raw.strip():
            parent_profile = resolve(parent_id_raw.strip())
        merged_profile = _merge_profile_dict(parent=parent_profile, child=raw_profile)
        resolving.remove(profile_id)
        resolved[profile_id] = merged_profile
        return dict(merged_profile)

    for profile_id in profiles:
        resolve(str(profile_id))
    return resolved


def _canonical_cardinality_profile_id(profile_id: str) -> str:
    normalized = str(profile_id or "").strip()
    return normalized


def _merge_profile_dict(*, parent: dict[str, object], child: dict[str, object]) -> dict[str, object]:
    merged = dict(parent)
    for key, child_value in child.items():
        if key == "extends":
            continue
        if key in {"per_concept", "segments"}:
            parent_value = merged.get(key, {})
            if isinstance(parent_value, dict) and isinstance(child_value, dict):
                combined = dict(parent_value)
                combined.update(child_value)
                merged[key] = combined
                continue
        if key == "rules":
            parent_rules = merged.get("rules", [])
            if isinstance(parent_rules, list) and isinstance(child_value, list):
                # Child rules have priority because rowwise evaluator uses first-match wins.
                merged[key] = list(child_value) + list(parent_rules)
                continue
        merged[key] = child_value
    return merged
