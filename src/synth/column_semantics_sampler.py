"""
Column-level distributions and merge strategies aligned with validation rule_id (see rule_catalog).

Generation reads column_profiles_unified (see SYNTH_COLUMN_PROFILES); validation reads the same rule_id via rule_engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Any
import random
import json
import hashlib
from zoneinfo import ZoneInfo

from src.infra.config_store import SYNTH_COLUMN_PROFILES, SYNTH_JSON_OBJECT_PACKS, SYNTH_MANIFEST, load_schema_config
from src.schema.models import ColumnDefinition, TableDefinition
from src.validation.rule_catalog import (
    age_bounds_for_rule_id,
    allowed_enum_for_rule_id,
)

SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
DEFAULT_MONEY_SCALE = 2


@dataclass(frozen=True)
class CrossColumnWhenThen:
    constraint_id: str
    table_name: str
    when_column: str
    when_equals: str
    then_column: str
    then_distribution: dict[str, Any]


def load_column_semantics_profile_id_default() -> str:
    manifest = load_schema_config(SYNTH_MANIFEST)
    override = manifest.get("column_semantics_profile_id_default")
    if isinstance(override, str) and override.strip():
        return override.strip()
    payload = _load_column_profiles_payload()
    return str(payload.get("default_profile_id", "minimal"))


def select_column_semantics_profile_id(*, inferred_primary_domain_id: str | None) -> str:
    """
    Choose column semantics profile by inferred domain, fallback to default.
    """
    manifest = load_schema_config(SYNTH_MANIFEST)
    domain_map = manifest.get("column_semantics_profile_by_domain", {})
    if isinstance(domain_map, dict) and inferred_primary_domain_id:
        profile_id = domain_map.get(inferred_primary_domain_id)
        if isinstance(profile_id, str) and profile_id.strip():
            return profile_id.strip()
    return load_column_semantics_profile_id_default()


def select_column_semantics_profile_ids(
    *,
    inferred_primary_domain_id: str | None,
    inferred_secondary_domain_id: str | None = None,
) -> tuple[str, ...]:
    manifest = load_schema_config(SYNTH_MANIFEST)
    domain_map = manifest.get("column_semantics_profile_by_domain", {})
    default_profile_id = load_column_semantics_profile_id_default()
    profiles_payload = _load_column_profiles_payload()
    all_profiles = profiles_payload.get("profiles", {})
    apply_all = bool(manifest.get("column_semantics_profile_apply_all", False))

    if apply_all and isinstance(all_profiles, dict) and all_profiles:
        selected_all: list[str] = []
        if default_profile_id in all_profiles:
            selected_all.append(default_profile_id)
        for profile_id in all_profiles.keys():
            normalized = str(profile_id).strip()
            if normalized and normalized not in selected_all:
                selected_all.append(normalized)
        if selected_all:
            return tuple(selected_all)

    selected: list[str] = []
    for domain_id in (inferred_primary_domain_id, inferred_secondary_domain_id):
        if not isinstance(domain_id, str) or not domain_id.strip():
            continue
        if not isinstance(domain_map, dict):
            continue
        profile_id = domain_map.get(domain_id)
        if isinstance(profile_id, str) and profile_id.strip():
            resolved = profile_id.strip()
            if resolved not in selected:
                selected.append(resolved)

    if not selected:
        selected.append(default_profile_id)
    return tuple(selected)


def compose_semantics_profile_id(*, profile_ids: tuple[str, ...]) -> str:
    cleaned = [profile_id.strip() for profile_id in profile_ids if profile_id.strip()]
    if not cleaned:
        return load_column_semantics_profile_id_default()
    return "+".join(cleaned)


def _profile_columns(profile_id: str) -> tuple[dict[str, Any], str, list[CrossColumnWhenThen]]:
    payload = _load_column_profiles_payload()
    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        return {}, "prefer_schema_enum_then_semantic", []
    requested_ids = [token.strip() for token in str(profile_id).split("+") if token.strip()]
    if not requested_ids:
        requested_ids = [str(payload.get("default_profile_id", "minimal"))]

    selected_profiles: list[dict[str, Any]] = []
    for requested_id in requested_ids:
        raw = profiles.get(requested_id)
        if isinstance(raw, dict):
            selected_profiles.append(raw)

    if not selected_profiles:
        fallback = profiles.get(str(payload.get("default_profile_id", "minimal")), {})
        if not isinstance(fallback, dict):
            return {}, "prefer_schema_enum_then_semantic", []
        selected_profiles = [fallback]

    merge_default = str(
        selected_profiles[0].get("merge_strategy_default", "prefer_schema_enum_then_semantic")
    )
    cols: dict[str, Any] = {}
    cross: list[CrossColumnWhenThen] = []
    for raw in selected_profiles:
        raw_cols = raw.get("columns", {})
        if isinstance(raw_cols, dict):
            for column_key, column_spec in raw_cols.items():
                if column_key not in cols and isinstance(column_spec, dict):
                    cols[str(column_key)] = dict(column_spec)
        for item in raw.get("cross_column_constraints", []) or []:
            if not isinstance(item, dict):
                continue
            when = item.get("when", {})
            then = item.get("then", {})
            if not isinstance(when, dict) or not isinstance(then, dict):
                continue
            cross.append(
                CrossColumnWhenThen(
                    constraint_id=str(item.get("constraint_id", "")),
                    table_name=str(item.get("table", "")),
                    when_column=str(when.get("column", "")),
                    when_equals=str(when.get("equals", "")),
                    then_column=str(then.get("column", "")),
                    then_distribution=(
                        dict(then.get("distribution", {}))
                        if isinstance(then.get("distribution"), dict)
                        else {}
                    ),
                )
            )
    return cols, merge_default, cross


def _load_column_profiles_payload() -> dict[str, Any]:
    payload = load_schema_config(SYNTH_COLUMN_PROFILES)
    return payload if isinstance(payload, dict) else {}


def _load_currency_decimal_scale_config() -> tuple[dict[str, int], int]:
    manifest = load_schema_config(SYNTH_MANIFEST)
    if not isinstance(manifest, dict):
        return {}, DEFAULT_MONEY_SCALE
    raw_default = manifest.get("currency_decimal_default_scale", DEFAULT_MONEY_SCALE)
    try:
        default_scale = max(0, int(raw_default))
    except (TypeError, ValueError):
        default_scale = DEFAULT_MONEY_SCALE
    raw_map = manifest.get("currency_decimal_scale_map", {})
    if not isinstance(raw_map, dict):
        return {}, default_scale
    normalized: dict[str, int] = {}
    for currency_code, scale in raw_map.items():
        code = str(currency_code).upper().strip()
        if not code:
            continue
        try:
            normalized[code] = max(0, int(scale))
        except (TypeError, ValueError):
            continue
    return normalized, default_scale


def _load_fx_rate_profile() -> tuple[str, dict[str, Decimal]]:
    manifest = load_schema_config(SYNTH_MANIFEST)
    if not isinstance(manifest, dict):
        return "CNY", {"CNY": Decimal("1")}
    raw_profile = manifest.get("fx_rate_profile", {})
    if not isinstance(raw_profile, dict):
        return "CNY", {"CNY": Decimal("1")}
    base_currency = str(raw_profile.get("base_currency", "CNY")).upper().strip() or "CNY"
    raw_rates = raw_profile.get("rates", {})
    normalized_rates: dict[str, Decimal] = {base_currency: Decimal("1")}
    if isinstance(raw_rates, dict):
        for currency_code, rate_value in raw_rates.items():
            code = str(currency_code).upper().strip()
            if not code:
                continue
            try:
                parsed_rate = Decimal(str(rate_value))
            except Exception:
                continue
            if parsed_rate <= Decimal("0"):
                continue
            normalized_rates[code] = parsed_rate
    return base_currency, normalized_rates


def _currency_scale_for_distribution(
    *,
    dist_obj: dict[str, Any],
    row_partial: dict[str, object] | None,
) -> int:
    scale_map, default_scale = _load_currency_decimal_scale_config()
    currency_column = str(dist_obj.get("currency_column", "currency")).strip() or "currency"
    if not row_partial:
        return default_scale
    currency_value = str(row_partial.get(currency_column, "")).upper().strip()
    if not currency_value:
        return default_scale
    return scale_map.get(currency_value, default_scale)


def _convert_base_to_target_amount(
    *,
    amount_in_base_currency: Decimal,
    target_currency: str,
    base_currency: str,
    rates: dict[str, Decimal],
) -> Decimal:
    normalized_target = target_currency.upper().strip()
    if not normalized_target or normalized_target == base_currency:
        return amount_in_base_currency
    target_rate = rates.get(normalized_target)
    if not target_rate or target_rate <= Decimal("0"):
        return amount_in_base_currency
    return amount_in_base_currency / target_rate


def _amount_string_from_decimal(*, amount: Decimal, scale: int) -> str:
    quantizer = Decimal(1) if scale <= 0 else Decimal("1").scaleb(-scale)
    return str(amount.quantize(quantizer, rounding=ROUND_HALF_UP))


def order_columns_for_generation(
    *,
    table: TableDefinition,
    profile_id: str,
) -> list[ColumnDefinition]:
    """Put 'when' columns before 'then' columns for cross-column constraints on this table."""

    _, _, cross_list = _profile_columns(profile_id)
    deps: dict[str, set[str]] = {c.name: set() for c in table.columns}
    for rule in cross_list:
        if rule.table_name and rule.table_name != table.name:
            continue
        if rule.then_column and rule.when_column and rule.then_column != rule.when_column:
            deps.setdefault(rule.then_column, set()).add(rule.when_column)
    cols_map, _, _ = _profile_columns(profile_id)
    table_prefix = f"{table.name}."
    for key, spec in cols_map.items():
        if not isinstance(spec, dict):
            continue
        if not key.startswith(table_prefix):
            continue
        column_name = key[len(table_prefix) :]
        dist_obj = spec.get("distribution")
        if not isinstance(dist_obj, dict):
            continue
        dist_type = str(dist_obj.get("type", "")).strip()
        if dist_type != "ordinal_by_anchor_softmax":
            continue
        anchor_column = str(dist_obj.get("anchor_column", "")).strip()
        if not anchor_column or anchor_column == column_name:
            continue
        deps.setdefault(column_name, set()).add(anchor_column)
    names = [c.name for c in table.columns]
    ordered: list[str] = []
    remaining = set(names)
    while remaining:
        ready = [n for n in remaining if not (deps.get(n, set()) & remaining)]
        if not ready:
            ready = list(remaining)
        ready.sort(key=lambda x: names.index(x))
        pick = ready[0]
        ordered.append(pick)
        remaining.remove(pick)
    name_to_col = {c.name: c for c in table.columns}
    return [name_to_col[n] for n in ordered if n in name_to_col]


def sample_column_value(
    *,
    table_name: str,
    column: ColumnDefinition,
    row_index: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    timestamp_anchor: datetime,
    table_to_concept: dict[str, str] | None,
    row_partial: dict[str, object],
    semantics_profile_id: str,
) -> object:
    if column.is_primary_key:
        return _generate_realistic_primary_key(
            table_name=table_name,
            column_name=column.name,
            row_index=row_index,
        )

    if column.foreign_key is not None:
        parent_table = column.foreign_key.referenced_table
        parent_pool = primary_key_cache.get(parent_table, [])
        if not parent_pool:
            raise ValueError(f"foreign key parent table has no key pool: {parent_table}")
        return rng.choice(parent_pool)

    normalized_table_name = _normalize_table_name_for_semantics(table_name)
    column_key = f"{table_name}.{column.name}"
    fallback_column_key = f"{normalized_table_name}.{column.name}"
    cols_map, merge_strategy, cross_rules = _profile_columns(semantics_profile_id)
    spec = cols_map.get(column_key)
    if not isinstance(spec, dict):
        spec = cols_map.get(fallback_column_key)
    if isinstance(spec, dict):
        dist = spec.get("distribution")
        if isinstance(dist, dict):
            for rule in cross_rules:
                if rule.table_name and rule.table_name != table_name:
                    continue
                if rule.then_column != column.name:
                    continue
                when_val = row_partial.get(rule.when_column)
                if str(when_val) == rule.when_equals and rule.then_distribution:
                    sampled_cc = _sample_from_distribution(
                        dist_obj=rule.then_distribution,
                        column=column,
                        rng=rng,
                        row_index=row_index,
                        timestamp_anchor=timestamp_anchor,
                        row_partial=row_partial,
                    )
                    if sampled_cc is not None:
                        return sampled_cc
            if merge_strategy == "prefer_schema_enum_then_semantic":
                merged = _merge_allowed_values_with_semantics_rng(
                    column=column, distribution=dist, rng=rng
                )
                if merged is not None:
                    return merged
            sampled = _sample_from_distribution(
                dist_obj=dist,
                column=column,
                rng=rng,
                row_index=row_index,
                timestamp_anchor=timestamp_anchor,
                row_partial=row_partial,
            )
            if sampled is not None:
                return sampled

    if column.normalized_type == "categorical" and column.allowed_values:
        return rng.choice(column.allowed_values)

    return _legacy_heuristic_column(
        table_name=table_name,
        column=column,
        row_index=row_index,
        rng=rng,
        timestamp_anchor=timestamp_anchor,
    )


def _merge_allowed_values_with_semantics_rng(
    *,
    column: ColumnDefinition,
    distribution: dict[str, Any],
    rng: random.Random,
) -> object | None:
    if column.allowed_values and distribution.get("type") == "weighted_enum":
        weights_map = distribution.get("weights", {})
        if not isinstance(weights_map, dict):
            return None
        allowed = [x for x in column.allowed_values if str(x) in weights_map]
        if not allowed:
            raise ValueError(
                "weighted_enum has no overlap with column.allowed_values: "
                f"column={column.name}, allowed_values={column.allowed_values}, "
                f"weighted_keys={list(weights_map.keys())}"
            )
        weights = [float(weights_map[str(x)]) for x in allowed]
        s = sum(weights) or 1.0
        weights = [w / s for w in weights]
        return str(rng.choices(allowed, weights=weights, k=1)[0])
    return None


@dataclass(frozen=True)
class DistributionSampleContext:
    column: ColumnDefinition
    rng: random.Random
    row_index: int
    timestamp_anchor: datetime
    row_partial: dict[str, object] | None


def _sample_from_distribution(
    *,
    dist_obj: dict[str, Any],
    column: ColumnDefinition,
    rng: random.Random,
    row_index: int,
    timestamp_anchor: datetime,
    row_partial: dict[str, object] | None = None,
) -> object | None:
    kind = str(dist_obj.get("type", ""))
    context = DistributionSampleContext(
        column=column,
        rng=rng,
        row_index=row_index,
        timestamp_anchor=timestamp_anchor,
        row_partial=row_partial,
    )
    sampler = _DISTRIBUTION_SAMPLERS.get(kind)
    if sampler is None:
        return None
    return sampler(dist_obj, context)


def _sample_uniform_int_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    minimum = int(dist_obj.get("min", 0))
    maximum = int(dist_obj.get("max", 0))
    if maximum < minimum:
        minimum, maximum = maximum, minimum
    return context.rng.randint(minimum, maximum)


def _sample_bucketed_int_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    buckets = dist_obj.get("buckets", [])
    if not isinstance(buckets, list) or not buckets:
        return None
    values: list[tuple[int, int, float]] = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        minimum = int(bucket.get("min", 0))
        maximum = int(bucket.get("max", 0))
        weight = max(0.0, float(bucket.get("weight", 0.0)))
        if maximum < minimum:
            minimum, maximum = maximum, minimum
        values.append((minimum, maximum, weight))
    if not values:
        return None
    weights = [item[2] for item in values]
    total_weight = sum(weights) or 1.0
    normalized = [weight / total_weight for weight in weights]
    chosen_min, chosen_max, _ = context.rng.choices(values, weights=normalized, k=1)[0]
    return context.rng.randint(chosen_min, chosen_max)


def _sample_weighted_enum_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    weights_map = dist_obj.get("weights", {})
    if isinstance(weights_map, dict) and context.column.allowed_values:
        return _merge_allowed_values_with_semantics_rng(
            column=context.column, distribution=dist_obj, rng=context.rng
        )
    if isinstance(weights_map, dict):
        keys = [str(key) for key in weights_map.keys()]
        weights = [max(0.0, float(weights_map[key])) for key in keys]
        total_weight = sum(weights) or 1.0
        normalized = [weight / total_weight for weight in weights]
        return str(context.rng.choices(keys, weights=normalized, k=1)[0])
    return None


def _sample_ordinal_by_anchor_softmax_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    anchor_column = str(dist_obj.get("anchor_column", "")).strip()
    if not anchor_column:
        return None

    anchor_scores = dist_obj.get("anchor_scores", {})
    enum_scores = dist_obj.get("enum_scores", {})
    if not isinstance(anchor_scores, dict) or not isinstance(enum_scores, dict):
        return None

    anchor_value = str((context.row_partial or {}).get(anchor_column, "")).strip()
    target_score_raw: object | None = anchor_scores.get(anchor_value)
    if target_score_raw is None:
        target_score_raw = dist_obj.get("anchor_default_score")
    if target_score_raw is None:
        return None
    try:
        target_score = float(target_score_raw)
    except (TypeError, ValueError):
        return None

    jitter_stddev = _safe_non_negative_float(dist_obj.get("jitter_stddev", 0.0), 0.0)
    if jitter_stddev > 0:
        target_score = context.rng.gauss(target_score, jitter_stddev)
    decay = max(1e-6, _safe_non_negative_float(dist_obj.get("decay", 1.0), 1.0))

    if context.column.allowed_values:
        candidate_values = [str(value) for value in context.column.allowed_values]
    else:
        candidate_values = [str(value) for value in enum_scores.keys()]
    if not candidate_values:
        return None

    scored_candidates: list[tuple[str, float]] = []
    for candidate in candidate_values:
        score_raw = enum_scores.get(candidate)
        if score_raw is None:
            continue
        try:
            enum_score = float(score_raw)
        except (TypeError, ValueError):
            continue
        distance = abs(enum_score - target_score)
        weight = math.exp(-decay * distance)
        scored_candidates.append((candidate, weight))

    if not scored_candidates:
        return None
    keys = [item[0] for item in scored_candidates]
    weights = [item[1] for item in scored_candidates]
    total_weight = sum(weights) or 1.0
    normalized = [weight / total_weight for weight in weights]
    return str(context.rng.choices(keys, weights=normalized, k=1)[0])


def _sample_uniform_from_rule_catalog_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    rule_id = str(dist_obj.get("rule_id", ""))
    values = allowed_enum_for_rule_id(rule_id)
    if values:
        return context.rng.choice(list(values))
    bounds = age_bounds_for_rule_id(rule_id)
    if bounds:
        return context.rng.randint(bounds[0], bounds[1])
    return None


def _sample_log_uniform_money_fx_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    minimum_base = Decimal(str(dist_obj.get("min_base", "100.00")))
    maximum_base = Decimal(str(dist_obj.get("max_base", "500000.00")))
    return _sample_log_uniform_money_fx_range(
        minimum_base=minimum_base,
        maximum_base=maximum_base,
        dist_obj=dist_obj,
        context=context,
    )


def _sample_mixture_log_uniform_money_fx_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    components = dist_obj.get("components", [])
    if not isinstance(components, list) or not components:
        return None
    parsed: list[tuple[Decimal, Decimal, float]] = []
    for item in components:
        if not isinstance(item, dict):
            continue
        minimum_base = Decimal(str(item.get("min_base", "100.00")))
        maximum_base = Decimal(str(item.get("max_base", "500000.00")))
        if maximum_base < minimum_base:
            minimum_base, maximum_base = maximum_base, minimum_base
        minimum_base = max(minimum_base, Decimal("0.01"))
        maximum_base = max(maximum_base, minimum_base)
        weight = max(0.0, float(item.get("weight", 0.0)))
        parsed.append((minimum_base, maximum_base, weight))
    if not parsed:
        return None
    weights = [item[2] for item in parsed]
    total_weight = sum(weights) or 1.0
    normalized = [weight / total_weight for weight in weights]
    minimum_base, maximum_base, _ = context.rng.choices(parsed, weights=normalized, k=1)[0]
    return _sample_log_uniform_money_fx_range(
        minimum_base=minimum_base,
        maximum_base=maximum_base,
        dist_obj=dist_obj,
        context=context,
    )


def _sample_log_uniform_money_fx_range(
    *,
    minimum_base: Decimal,
    maximum_base: Decimal,
    dist_obj: dict[str, Any],
    context: DistributionSampleContext,
) -> object | None:
    if maximum_base < minimum_base:
        minimum_base, maximum_base = maximum_base, minimum_base
    minimum_base = max(minimum_base, Decimal("0.01"))
    maximum_base = max(maximum_base, minimum_base)
    log_min = math.log(float(minimum_base))
    log_max = math.log(float(maximum_base))
    sampled_base = Decimal(str(math.exp(context.rng.uniform(log_min, log_max))))
    sampled_base = max(minimum_base, min(maximum_base, sampled_base))
    base_currency, rates = _load_fx_rate_profile()
    currency_column = str(dist_obj.get("currency_column", "currency")).strip() or "currency"
    target_currency = str((context.row_partial or {}).get(currency_column, base_currency)).upper().strip() or base_currency
    amount = _convert_base_to_target_amount(
        amount_in_base_currency=sampled_base,
        target_currency=target_currency,
        base_currency=base_currency,
        rates=rates,
    )
    scale = _currency_scale_for_distribution(dist_obj=dist_obj, row_partial=context.row_partial)
    return _amount_string_from_decimal(amount=amount, scale=scale)


def _sample_uniform_timestamp_days_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    day_offset = context.rng.randint(0, int(dist_obj.get("max_days", 90)))
    minute_offset = context.rng.randint(0, int(dist_obj.get("max_minutes", 600)))
    generated_datetime = context.timestamp_anchor + timedelta(days=day_offset, minutes=minute_offset)
    return generated_datetime.isoformat()


def _sample_weekday_timestamp_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    max_days = int(dist_obj.get("max_days", 60))
    minute_low = int(dist_obj.get("min_minutes", 9 * 60))
    minute_high = int(dist_obj.get("max_minutes", 18 * 60))
    if minute_high < minute_low:
        minute_low, minute_high = minute_high, minute_low

    attempts = 0
    while attempts < 12:
        day_offset = context.rng.randint(0, max_days)
        candidate = context.timestamp_anchor + timedelta(days=day_offset)
        if candidate.weekday() < 5:
            minute_offset = context.rng.randint(minute_low, minute_high)
            aligned = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
            return (aligned + timedelta(minutes=minute_offset)).isoformat()
        attempts += 1

    minute_offset = context.rng.randint(minute_low, minute_high)
    return (context.timestamp_anchor + timedelta(minutes=minute_offset)).isoformat()


def _sample_structured_json_object_distribution(
    dist_obj: dict[str, Any], context: DistributionSampleContext
) -> object | None:
    return _sample_structured_json_object(
        dist_obj=dist_obj,
        rng=context.rng,
        row_index=context.row_index,
        row_partial=context.row_partial,
    )


def _safe_non_negative_float(raw: object, default: float) -> float:
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return default


_DISTRIBUTION_SAMPLERS: dict[str, Any] = {
    "uniform_int": _sample_uniform_int_distribution,
    "bucketed_int": _sample_bucketed_int_distribution,
    "weighted_enum": _sample_weighted_enum_distribution,
    "ordinal_by_anchor_softmax": _sample_ordinal_by_anchor_softmax_distribution,
    "uniform_from_rule_catalog": _sample_uniform_from_rule_catalog_distribution,
    "log_uniform_money_fx": _sample_log_uniform_money_fx_distribution,
    "mixture_log_uniform_money_fx": _sample_mixture_log_uniform_money_fx_distribution,
    "uniform_timestamp_days": _sample_uniform_timestamp_days_distribution,
    "weekday_timestamp": _sample_weekday_timestamp_distribution,
    "structured_json_object": _sample_structured_json_object_distribution,
}


def _sample_structured_json_object(
    *,
    dist_obj: dict[str, Any],
    rng: random.Random,
    row_index: int,
    row_partial: dict[str, Any] | None,
) -> str:
    context = row_partial or {}
    pack_id = str(dist_obj.get("pack_id", "")).strip() or str(dist_obj.get("template_id", "")).strip()
    pack = _load_json_object_pack(pack_id=pack_id)
    if not pack:
        return json.dumps({"pack_id": pack_id or "unknown_pack", "row_index": row_index}, ensure_ascii=True)
    payload = _build_json_payload_from_pack(
        pack=pack,
        context=context,
        row_index=row_index,
        rng=rng,
    )
    return json.dumps(payload, ensure_ascii=True)


def _load_json_object_pack(*, pack_id: str) -> dict[str, Any]:
    if not pack_id:
        return {}
    payload = load_schema_config(SYNTH_JSON_OBJECT_PACKS)
    packs = payload.get("packs", {})
    if not isinstance(packs, dict):
        return {}
    picked = packs.get(pack_id)
    return dict(picked) if isinstance(picked, dict) else {}


def _build_json_payload_from_pack(
    *,
    pack: dict[str, Any],
    context: dict[str, Any],
    row_index: int,
    rng: random.Random,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    fields = pack.get("fields", [])
    if not isinstance(fields, list):
        return out
    for item in fields:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path", ""))
        generator = item.get("generator", {})
        if not path or not isinstance(generator, dict):
            continue
        value = _sample_json_value_from_generator(
            generator=generator,
            context=context,
            row_index=row_index,
            rng=rng,
        )
        _set_nested_path(out, path, value)
    return out


def _sample_json_value_from_generator(
    *,
    generator: dict[str, Any],
    context: dict[str, Any],
    row_index: int,
    rng: random.Random,
) -> Any:
    kind = str(generator.get("type", ""))
    if kind == "weighted_enum":
        weights = generator.get("weights", {})
        if not isinstance(weights, dict) or not weights:
            return "UNKNOWN"
        keys = [str(k) for k in weights.keys()]
        vals = [max(0.0, float(weights[k])) for k in keys]
        sw = sum(vals) or 1.0
        probs = [v / sw for v in vals]
        return rng.choices(keys, weights=probs, k=1)[0]
    if kind == "from_field_pack_allowed_values":
        field_pack_id = str(generator.get("field_pack_id", ""))
        field_name = str(generator.get("field_name", ""))
        allowed = _allowed_values_from_field_pack(
            field_pack_id=field_pack_id,
            field_name=field_name,
        )
        if not allowed:
            fallback = generator.get("fallback", [])
            if isinstance(fallback, list):
                allowed = [str(item) for item in fallback]
        if not allowed:
            return "UNKNOWN"
        weights_map = generator.get("weights", {})
        if isinstance(weights_map, dict) and weights_map:
            lowered = {str(k).lower(): float(v) for k, v in weights_map.items()}
            weights = [max(0.0, lowered.get(item.lower(), 0.0)) for item in allowed]
            sw = sum(weights)
            if sw > 0:
                probs = [w / sw for w in weights]
                return rng.choices(allowed, weights=probs, k=1)[0]
        return rng.choice(allowed)
    if kind == "bool_probability":
        p = float(generator.get("p_true", 0.5))
        return rng.random() < min(max(p, 0.0), 1.0)
    if kind == "int_range":
        low = int(generator.get("min", 0))
        high = int(generator.get("max", 0))
        if high < low:
            low, high = high, low
        return rng.randint(low, high)
    if kind == "enum_list_sample":
        pool = generator.get("pool", [])
        if not isinstance(pool, list) or not pool:
            return []
        keys = [str(x) for x in pool]
        k = int(generator.get("k", 1))
        k = max(0, min(k, len(keys)))
        return rng.sample(keys, k=k)
    if kind == "row_index_date":
        start = str(generator.get("start_date", "2024-01-01"))
        try:
            start_date = datetime.fromisoformat(f"{start}T00:00:00+08:00")
        except ValueError:
            start_date = datetime(2024, 1, 1, tzinfo=SHANGHAI_TIMEZONE)
        modulo_days = int(generator.get("modulo_days", 365))
        offset = row_index % max(modulo_days, 1)
        return (start_date + timedelta(days=offset)).date().isoformat()
    if kind == "from_context":
        key = str(generator.get("key", ""))
        transform = str(generator.get("transform", ""))
        value = context.get(key, generator.get("default"))
        if transform == "upper":
            return str(value or "").upper()
        if transform == "lower":
            return str(value or "").lower()
        return value
    if kind == "conditional_map":
        source_key = str(generator.get("source_key", ""))
        source_value = str(context.get(source_key, ""))
        mapping = generator.get("map", {})
        if isinstance(mapping, dict) and source_value in mapping:
            return mapping[source_value]
        default = generator.get("default")
        if isinstance(default, dict):
            return _sample_json_value_from_generator(
                generator=default,
                context=context,
                row_index=row_index,
                rng=rng,
            )
        return default
    if kind == "literal":
        return generator.get("value")
    return "UNKNOWN"


def _set_nested_path(payload: dict[str, Any], path: str, value: Any) -> None:
    parts = [part for part in path.split(".") if part]
    if not parts:
        return
    cursor = payload
    for part in parts[:-1]:
        node = cursor.get(part)
        if not isinstance(node, dict):
            node = {}
            cursor[part] = node
        cursor = node
    cursor[parts[-1]] = value


def _allowed_values_from_field_pack(*, field_pack_id: str, field_name: str) -> list[str]:
    if not field_pack_id or not field_name:
        return []
    payload = load_schema_config("field_packs.json")
    packs = payload.get("packs", {})
    if not isinstance(packs, dict):
        return []
    rows = packs.get(field_pack_id)
    if not isinstance(rows, list):
        return []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("name", "")).lower() != field_name.lower():
            continue
        allowed = row.get("allowed_values", [])
        if isinstance(allowed, list):
            return [str(item) for item in allowed]
    return []


def _legacy_heuristic_column(
    *,
    table_name: str,
    column: ColumnDefinition,
    row_index: int,
    rng: random.Random,
    timestamp_anchor: datetime,
) -> object:
    column_name = column.name.lower()
    if column.normalized_type == "integer":
        return rng.randint(18, 80)

    if column.normalized_type == "decimal":
        minimum_base = Decimal("100.00")
        maximum_base = Decimal("500000.00")
        log_min = math.log(float(minimum_base))
        log_max = math.log(float(maximum_base))
        sampled_base = Decimal(str(math.exp(rng.uniform(log_min, log_max))))
        base_currency, rates = _load_fx_rate_profile()
        amount = _convert_base_to_target_amount(
            amount_in_base_currency=sampled_base,
            target_currency=base_currency,
            base_currency=base_currency,
            rates=rates,
        )
        return _amount_string_from_decimal(amount=amount, scale=DEFAULT_MONEY_SCALE)

    if column.normalized_type == "timestamp":
        day_offset = rng.randint(0, 90)
        minute_offset = rng.randint(0, 600)
        generated_datetime = timestamp_anchor + timedelta(days=day_offset, minutes=minute_offset)
        return generated_datetime.isoformat()

    if column.normalized_type == "categorical":
        if column.allowed_values:
            return rng.choice(column.allowed_values)
        return _generate_categorical_value(
            table_name=table_name,
            column_name=column_name,
            column=column,
            rng=rng,
        )

    if column.normalized_type == "json":
        import json

        payload = {
            "generator_id": "legacy_heuristic_json",
            "payload_intent": "synthetic_placeholder_only",
            "table_name": table_name,
            "column_name": column.name,
            "row_index": row_index,
            "variant_token": rng.randint(1, 999_999),
        }
        return json.dumps(payload, ensure_ascii=True)

    if column.normalized_type == "xml":
        return (
            f"<record><column>{column.name}</column>"
            f"<risk>{rng.choice(['low', 'medium', 'high'])}</risk></record>"
        )

    if column.normalized_type == "text":
        return rng.choice(
            [
                "Customer profile verified by branch operations.",
                "Transaction reviewed and approved by risk control.",
                "Exposure monitoring note generated by system audit.",
            ]
        )

    if "name" in column_name:
        first_names = ["Li", "Wang", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao"]
        last_names = ["Wei", "Ming", "Jie", "Na", "Lei", "Fang", "Xin", "Hao"]
        return f"{rng.choice(first_names)} {rng.choice(last_names)}"
    if "country" in column_name:
        return rng.choice(["CN", "US", "SG", "HK", "GB"])
    return f"{column.name}_{row_index:08d}"


def _generate_categorical_value(
    *,
    table_name: str,
    column_name: str,
    column: ColumnDefinition,
    rng: random.Random,
) -> str:
    if "account_type" in column_name:
        return rng.choice(["savings", "checking", "margin"])
    if "country" in column_name:
        return rng.choice(["CN", "US", "SG", "HK", "GB"])
    if "currency" in column_name:
        return rng.choice(["CNY", "USD", "EUR", "SGD", "HKD"])
    if "status" in column_name:
        if column.allowed_values:
            return rng.choice([str(value) for value in column.allowed_values])
        return rng.choice(["open", "pending", "active", "closed"])
    return rng.choice(["A", "B", "C"])


def _normalize_table_name_for_semantics(table_name: str) -> str:
    name = table_name.lower().strip()
    if "_" not in name:
        return name
    parts = name.split("_")
    if len(parts) <= 1:
        return name
    first = parts[0]
    has_digit = any(ch.isdigit() for ch in first)
    if len(first) <= 4 or has_digit:
        return "_".join(parts[1:])
    return name


def _generate_realistic_primary_key(*, table_name: str, column_name: str, row_index: int) -> str:
    seed_key = f"{table_name}|{column_name}|{row_index}"
    digest = hashlib.sha256(seed_key.encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


