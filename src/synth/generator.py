from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import random
import json
import hashlib
import math
from typing import TYPE_CHECKING, Any

from zoneinfo import ZoneInfo

from src.infra.config_store import (
    SYNTH_COLUMN_PROFILES,
    SYNTH_LIFECYCLE_CONSTRAINTS,
    SYNTH_MANIFEST,
    SYNTH_SCENARIO_OVERLAYS,
    SYNTH_STATUS_NORMALIZATION,
    load_schema_config,
)
from src.schema.models import SchemaDefinition, TableDefinition
from src.synth.cardinality_eval import decide_rowwise_table_count, load_cardinality_profiles_payload
from src.synth.column_semantics_sampler import (
    compose_semantics_profile_id,
    order_columns_for_generation,
    sample_column_value,
    select_column_semantics_profile_ids,
)
from src.synth.declarative_fsm import apply_fsm_row_overrides
from src.synth.generation_stages import (
    PIPELINE_STAGE_EXPAND_CHILDREN,
    PIPELINE_STAGE_POST_ADJUST,
    PIPELINE_STAGE_SAMPLE_ROOTS,
    partition_root_child_tables,
    stage_record,
)

if TYPE_CHECKING:
    from src.synth.concept_schema_mapping import ConceptSchemaMappingResult


SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
MONEY_PRECISION_STANDARD = Decimal("0.01")
DEFAULT_MONEY_SCALE = 2


@dataclass(frozen=True)
class GeneratedDataset:
    """Hold generated records grouped by table name."""

    records_by_table: dict[str, list[dict[str, object]]]
    pipeline_telemetry: tuple[dict[str, object], ...] = ()
    scenario_matches: dict[str, str] | None = None
    post_adjust_warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class PostAdjustResult:
    scenario_matches: dict[str, str]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class OverlaySelectionDefaults:
    retail_fallback_scenario_id: str = "retail_deposit_standard"
    credit_fallback_scenario_id: str = "retail_plus_credit"
    customer_age_section_key: str = "customer_age_buckets_by_country"
    account_status_section_key: str = "account_status_by_account_type"
    account_status_by_journey_stage_section_key: str = "account_status_by_journey_stage"
    account_status_by_kyc_status_section_key: str = "account_status_by_kyc_status"
    account_status_history_source_system_section_key: str = (
        "account_status_history_source_system_distribution"
    )
    transaction_currency_section_key: str = "transaction_currency_by_country_account_type"
    exposure_ratio_section_key: str = "exposure_limit_ratio_by_segment"
    transaction_amount_section_key: str = "transaction_amount_mixture_by_account_type"
    customer_total_aum_section_key: str = "customer_total_aum_mixture_by_segment_tier"


def generate_dataset(
    schema: SchemaDefinition,
    ordered_tables: list[str],
    record_count: int,
    seed: int,
    *,
    concept_mapping: ConceptSchemaMappingResult | None = None,
) -> GeneratedDataset:
    semantics_profile_id = compose_semantics_profile_id(
        profile_ids=select_column_semantics_profile_ids(
            inferred_primary_domain_id=(
                concept_mapping.inferred_primary_domain_id if concept_mapping is not None else None
            ),
            inferred_secondary_domain_id=(
                concept_mapping.inferred_secondary_domain_id if concept_mapping is not None else None
            ),
        )
    )
    table_to_concept = concept_mapping.table_to_concept if concept_mapping else {}

    if (
        concept_mapping is not None
        and concept_mapping.resolved_event_chain is not None
        and concept_mapping.generation_mode_recommended == "event_first"
    ):
        from src.synth.event_first_generator import generate_event_first_linear_three

        try:
            return generate_event_first_linear_three(
                schema=schema,
                ordered_tables=ordered_tables,
                record_count=record_count,
                seed=seed,
                chain=concept_mapping.resolved_event_chain,
                semantics_profile_id=semantics_profile_id,
                table_to_concept=table_to_concept,
                inferred_primary_domain_id=concept_mapping.inferred_primary_domain_id,
            )
        except ValueError as exc:
            # Keep CLI single-input friendly: if event-first cannot build FK pools for
            # this schema shape, fall back to rowwise instead of failing the whole run.
            if "foreign key parent table has no key pool" not in str(exc):
                raise
            fallback_dataset = _generate_rowwise_pipeline(
                schema=schema,
                ordered_tables=ordered_tables,
                record_count=record_count,
                seed=seed,
                semantics_profile_id=semantics_profile_id,
                table_to_concept=table_to_concept,
                inferred_primary_domain_id=concept_mapping.inferred_primary_domain_id,
                graph_path_id=concept_mapping.resolved_event_chain.chain_id,
            )
            fallback_warnings = (
                *fallback_dataset.post_adjust_warnings,
                f"event_first_fallback_to_rowwise: {exc}",
            )
            return GeneratedDataset(
                records_by_table=fallback_dataset.records_by_table,
                pipeline_telemetry=fallback_dataset.pipeline_telemetry,
                scenario_matches=fallback_dataset.scenario_matches,
                post_adjust_warnings=fallback_warnings,
            )

    return _generate_rowwise_pipeline(
        schema=schema,
        ordered_tables=ordered_tables,
        record_count=record_count,
        seed=seed,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
        inferred_primary_domain_id=(
            concept_mapping.inferred_primary_domain_id if concept_mapping is not None else None
        ),
        graph_path_id=(
            concept_mapping.resolved_event_chain.chain_id
            if concept_mapping is not None and concept_mapping.resolved_event_chain is not None
            else None
        ),
    )


def _generate_rowwise_pipeline(
    *,
    schema: SchemaDefinition,
    ordered_tables: list[str],
    record_count: int,
    seed: int,
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
    inferred_primary_domain_id: str | None,
    graph_path_id: str | None,
) -> GeneratedDataset:
    rng = random.Random(seed)
    table_map = schema.table_map()
    roots, children = partition_root_child_tables(schema, ordered_tables)
    telemetry: list[dict[str, object]] = [
        stage_record(
            stage=PIPELINE_STAGE_SAMPLE_ROOTS,
            pipeline_mode="rowwise",
            tables=",".join(roots),
        ),
    ]

    records_by_table: dict[str, list[dict[str, object]]] = {}
    primary_key_cache: dict[str, list[str]] = {}

    for table_name in roots:
        table_definition = table_map[table_name]
        table_record_count = _decide_table_record_count_rowwise(
            table_name=table_name,
            record_count=record_count,
            has_foreign_keys=bool(table_definition.foreign_key_columns()),
        )
        table_records = _generate_table_records(
            table=table_definition,
            table_record_count=table_record_count,
            rng=rng,
            primary_key_cache=primary_key_cache,
            semantics_profile_id=semantics_profile_id,
            table_to_concept=table_to_concept,
        )
        records_by_table[table_name] = table_records
        primary_key_column = table_definition.primary_key_column()
        primary_key_cache[table_name] = [str(record[primary_key_column]) for record in table_records]

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_EXPAND_CHILDREN,
            pipeline_mode="rowwise",
            tables=",".join(children),
        )
    )

    for table_name in children:
        table_definition = table_map[table_name]
        table_record_count = _decide_table_record_count_rowwise(
            table_name=table_name,
            record_count=record_count,
            has_foreign_keys=bool(table_definition.foreign_key_columns()),
        )
        table_records = _generate_table_records(
            table=table_definition,
            table_record_count=table_record_count,
            rng=rng,
            primary_key_cache=primary_key_cache,
            semantics_profile_id=semantics_profile_id,
            table_to_concept=table_to_concept,
        )
        records_by_table[table_name] = table_records
        primary_key_column = table_definition.primary_key_column()
        primary_key_cache[table_name] = [str(record[primary_key_column]) for record in table_records]

    _enforce_multi_fk_parent_coherence(schema=schema, records_by_table=records_by_table)

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_POST_ADJUST,
            pipeline_mode="rowwise",
            detail="apply_post_generation_constraints",
        )
    )
    post_adjust_result = apply_post_generation_constraints(
        records_by_table=records_by_table,
        inferred_primary_domain_id=inferred_primary_domain_id,
        graph_path_id=graph_path_id,
    )
    return GeneratedDataset(
        records_by_table=records_by_table,
        pipeline_telemetry=tuple(telemetry),
        scenario_matches=post_adjust_result.scenario_matches,
        post_adjust_warnings=post_adjust_result.warnings,
    )


def _enforce_multi_fk_parent_coherence(
    *,
    schema: SchemaDefinition,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    table_map = schema.table_map()
    pk_index_by_table: dict[str, dict[str, dict[str, object]]] = {}
    for table_name, rows in records_by_table.items():
        table = table_map.get(table_name)
        if table is None or not rows:
            continue
        pk_column = table.primary_key_column()
        pk_index_by_table[table_name] = {str(row.get(pk_column, "")): row for row in rows}

    for child_table_name, child_rows in records_by_table.items():
        child_table = table_map.get(child_table_name)
        if child_table is None or not child_rows:
            continue
        child_fk_columns = child_table.foreign_key_columns()
        if len(child_fk_columns) < 2:
            continue

        child_fk_to_parent_table = {
            str(column.name): str(column.foreign_key.referenced_table)
            for column in child_fk_columns
            if column.foreign_key is not None
            and str(column.name).strip()
            and str(column.foreign_key.referenced_table).strip()
        }
        if len(child_fk_to_parent_table) < 2:
            continue

        coherence_rules: list[tuple[str, str, str, str]] = []
        # Rule shape:
        # (anchor_child_fk_column, dependent_child_fk_column, anchor_parent_table, parent_column_to_copy)
        for anchor_fk in child_fk_columns:
            if anchor_fk.foreign_key is None:
                continue
            anchor_fk_column = str(anchor_fk.name)
            anchor_parent_table = str(anchor_fk.foreign_key.referenced_table)
            anchor_parent_schema = table_map.get(anchor_parent_table)
            if anchor_parent_schema is None:
                continue
            for parent_fk in anchor_parent_schema.foreign_key_columns():
                if parent_fk.foreign_key is None:
                    continue
                dependent_column = str(parent_fk.name)
                dependent_parent_table = str(parent_fk.foreign_key.referenced_table)
                if dependent_column not in child_fk_to_parent_table:
                    continue
                if child_fk_to_parent_table[dependent_column] != dependent_parent_table:
                    continue
                coherence_rules.append(
                    (
                        anchor_fk_column,
                        dependent_column,
                        anchor_parent_table,
                        dependent_column,
                    )
                )
        if not coherence_rules:
            continue

        coherence_rules = sorted(set(coherence_rules))
        for row in child_rows:
            for anchor_fk_column, dependent_column, anchor_parent_table, parent_source_column in coherence_rules:
                anchor_value = str(row.get(anchor_fk_column, "")).strip()
                if not anchor_value:
                    continue
                parent_index = pk_index_by_table.get(anchor_parent_table, {})
                parent_row = parent_index.get(anchor_value)
                if not parent_row:
                    continue
                if parent_source_column not in parent_row:
                    continue
                row[dependent_column] = parent_row[parent_source_column]


def apply_post_generation_constraints(
    records_by_table: dict[str, list[dict[str, object]]],
    *,
    transactions_table: str = "transactions",
    accounts_table: str = "accounts",
    account_id_key: str = "account_id",
    transaction_time_key: str = "transaction_time",
    amount_key: str = "amount",
    balance_key: str = "balance",
    inferred_primary_domain_id: str | None = None,
    graph_path_id: str | None = None,
) -> PostAdjustResult:
    overlays = _load_account_type_scenario_overlays()
    overlay_defaults = _load_overlay_selection_defaults()
    feature_flags = _derive_table_feature_flags(records_by_table=records_by_table)
    scenario_matches: dict[str, str] = {}
    warnings: list[str] = []
    if not overlays:
        warnings.append("scenario_overlays_not_loaded: skip overlay-driven post-adjust branches")
    if "customers" not in records_by_table:
        warnings.append("missing_customers_table: customer-driven overlay branches skipped")
    if "accounts" not in records_by_table:
        warnings.append("missing_accounts_table: account-driven overlay branches skipped")
    if "transactions" not in records_by_table:
        warnings.append("missing_transactions_table: transaction-driven overlay branches skipped")

    retail_scenario_id = _select_overlay_scenario_id(
        overlays=overlays,
        records_by_table=records_by_table,
        has_lending_signal=False,
        fallback=overlay_defaults.retail_fallback_scenario_id,
        inferred_primary_domain_id=inferred_primary_domain_id,
        graph_path_id=graph_path_id,
        feature_flags=feature_flags,
    )
    _align_customer_age_with_scenario(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.customer_age_section_key,
    )
    _align_account_type_with_customer_profile(
        records_by_table=records_by_table,
        overlays=overlays,
        overlay_defaults=overlay_defaults,
        inferred_primary_domain_id=inferred_primary_domain_id,
        graph_path_id=graph_path_id,
        feature_flags=feature_flags,
    )
    credit_scenario_id = _select_overlay_scenario_id(
        overlays=overlays,
        records_by_table=records_by_table,
        has_lending_signal=True,
        fallback=overlay_defaults.credit_fallback_scenario_id,
        inferred_primary_domain_id=inferred_primary_domain_id,
        graph_path_id=graph_path_id,
        feature_flags=feature_flags,
    )
    _enforce_transaction_temporal_consistency(
        records_by_table=records_by_table,
        transactions_table=transactions_table,
        accounts_table=accounts_table,
        account_id_key=account_id_key,
        transaction_time_key=transaction_time_key,
    )
    _align_account_balance_with_transaction_conservation(
        records_by_table=records_by_table,
        accounts_table=accounts_table,
        transactions_table=transactions_table,
        account_id_key=account_id_key,
        amount_key=amount_key,
        balance_key=balance_key,
    )
    currency_scenario = _align_transaction_currency_with_customer_country(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.transaction_currency_section_key,
    )
    if currency_scenario:
        scenario_matches["transaction_currency"] = currency_scenario
    else:
        _align_transaction_currency_with_profile_baseline(
            records_by_table=records_by_table,
            inferred_primary_domain_id=inferred_primary_domain_id,
        )
    status_scenario = _align_account_status_with_scenario(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.account_status_section_key,
    )
    if status_scenario:
        scenario_matches["account_status"] = status_scenario
    status_journey_scenario = _align_account_status_with_journey_stage(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.account_status_by_journey_stage_section_key,
    )
    if status_journey_scenario:
        scenario_matches["account_status_journey_stage"] = status_journey_scenario
    status_kyc_scenario = _align_account_status_with_customer_kyc(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.account_status_by_kyc_status_section_key,
    )
    if status_kyc_scenario:
        scenario_matches["account_status_customer_kyc"] = status_kyc_scenario
    exposure_scenario = _align_credit_exposure_with_facility_limit(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=credit_scenario_id,
        section_key=overlay_defaults.exposure_ratio_section_key,
    )
    if exposure_scenario:
        scenario_matches["exposure_ratio"] = exposure_scenario
    amount_scenario = _align_transaction_amount_with_scenario(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.transaction_amount_section_key,
    )
    if amount_scenario:
        scenario_matches["transaction_amount"] = amount_scenario
    total_aum_scenario = _align_customer_total_aum_with_segment_tier(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=retail_scenario_id,
        section_key=overlay_defaults.customer_total_aum_section_key,
    )
    if total_aum_scenario:
        scenario_matches["customer_total_aum"] = total_aum_scenario
    _align_account_balance_with_transaction_conservation(
        records_by_table=records_by_table,
        accounts_table=accounts_table,
        transactions_table=transactions_table,
        account_id_key=account_id_key,
        amount_key=amount_key,
        balance_key=balance_key,
    )
    _apply_lifecycle_constraints_for_generation(records_by_table=records_by_table)
    _enforce_sales_opportunity_close_time_order(records_by_table=records_by_table)
    _synchronize_account_status_history_fact(
        records_by_table=records_by_table,
        overlays=overlays,
        scenario_id=status_scenario or retail_scenario_id,
        section_key=overlay_defaults.account_status_history_source_system_section_key,
    )
    _enrich_customer_profile_with_segment_context(records_by_table=records_by_table)
    _enrich_transaction_details_with_related_context(records_by_table=records_by_table)
    _align_collateral_haircut_with_asset_class(records_by_table=records_by_table)
    return PostAdjustResult(
        scenario_matches=scenario_matches,
        warnings=tuple(warnings),
    )


def _align_account_type_with_customer_profile(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    overlay_defaults: OverlaySelectionDefaults,
    inferred_primary_domain_id: str | None = None,
    graph_path_id: str | None = None,
    feature_flags: set[str] | None = None,
) -> None:
    customers = records_by_table.get("customers", [])
    accounts = records_by_table.get("accounts", [])
    if not customers or not accounts:
        return

    customer_by_id: dict[str, dict[str, object]] = {}
    for row in customers:
        customer_id = str(row.get("customer_id", ""))
        if customer_id:
            customer_by_id[customer_id] = row

    lending_keywords = {"loan", "credit", "revolving", "overdraft"}
    account_rows_by_signal: dict[bool, list[dict[str, object]]] = {
        False: [],
        True: [],
    }
    for account_row in accounts:
        product_hint = " ".join(
            str(account_row.get(key, "")).lower()
            for key in ("product_type", "product_category", "account_purpose", "facility_type")
        )
        has_lending_signal = any(keyword in product_hint for keyword in lending_keywords)
        account_rows_by_signal[has_lending_signal].append(account_row)

    for has_lending_signal, scoped_rows in account_rows_by_signal.items():
        if not scoped_rows:
            continue
        scenario_id = _select_overlay_scenario_id(
            overlays=overlays,
            records_by_table=records_by_table,
            has_lending_signal=has_lending_signal,
            fallback=(
                overlay_defaults.credit_fallback_scenario_id
                if has_lending_signal
                else overlay_defaults.retail_fallback_scenario_id
            ),
            inferred_primary_domain_id=inferred_primary_domain_id,
            graph_path_id=graph_path_id,
            feature_flags=feature_flags,
            check_column_values=False,
        )
        scenario = _overlay_scenario(overlays=overlays, scenario_id=scenario_id)
        for account_row in scoped_rows:
            customer_id = str(account_row.get("customer_id", ""))
            customer = customer_by_id.get(customer_id, {})
            country = str(customer.get("country", "")).upper()
            try:
                age = int(str(customer.get("age", "40")))
            except ValueError:
                age = 40

            sampled = _sample_account_type_from_overlay(
                account_row=account_row,
                scenario=scenario,
                age=age,
                country=country,
            )
            if sampled:
                account_row["account_type"] = sampled
                continue

            account_id = str(account_row.get("account_id", ""))
            bucket = sum(ord(ch) for ch in account_id) % 100
            account_row["account_type"] = "checking" if bucket < 40 else "savings"


def _load_account_type_scenario_overlays() -> dict[str, dict[str, object]]:
    payload = load_schema_config(SYNTH_SCENARIO_OVERLAYS)
    scenarios = payload.get("scenarios", {})
    if not isinstance(scenarios, dict):
        return {}
    output: dict[str, dict[str, object]] = {}
    for scenario_id, scenario in scenarios.items():
        if isinstance(scenario, dict):
            output[str(scenario_id)] = scenario
    return output


def _load_overlay_selection_defaults() -> OverlaySelectionDefaults:
    payload = load_schema_config(SYNTH_SCENARIO_OVERLAYS)
    raw_defaults = payload.get("selection_defaults", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_defaults, dict):
        return OverlaySelectionDefaults()
    retail = str(raw_defaults.get("retail_fallback_scenario_id", "")).strip()
    credit = str(raw_defaults.get("credit_fallback_scenario_id", "")).strip()
    customer_age_section_key = str(raw_defaults.get("customer_age_section_key", "")).strip()
    account_status_section_key = str(raw_defaults.get("account_status_section_key", "")).strip()
    account_status_by_journey_stage_section_key = str(
        raw_defaults.get("account_status_by_journey_stage_section_key", "")
    ).strip()
    account_status_by_kyc_status_section_key = str(
        raw_defaults.get("account_status_by_kyc_status_section_key", "")
    ).strip()
    account_status_history_source_system_section_key = str(
        raw_defaults.get("account_status_history_source_system_section_key", "")
    ).strip()
    transaction_currency_section_key = str(
        raw_defaults.get("transaction_currency_section_key", "")
    ).strip()
    exposure_ratio_section_key = str(raw_defaults.get("exposure_ratio_section_key", "")).strip()
    transaction_amount_section_key = str(raw_defaults.get("transaction_amount_section_key", "")).strip()
    customer_total_aum_section_key = str(
        raw_defaults.get("customer_total_aum_section_key", "")
    ).strip()
    return OverlaySelectionDefaults(
        retail_fallback_scenario_id=retail or "retail_deposit_standard",
        credit_fallback_scenario_id=credit or "retail_plus_credit",
        customer_age_section_key=customer_age_section_key or "customer_age_buckets_by_country",
        account_status_section_key=account_status_section_key or "account_status_by_account_type",
        account_status_by_journey_stage_section_key=(
            account_status_by_journey_stage_section_key or "account_status_by_journey_stage"
        ),
        account_status_by_kyc_status_section_key=(
            account_status_by_kyc_status_section_key or "account_status_by_kyc_status"
        ),
        account_status_history_source_system_section_key=(
            account_status_history_source_system_section_key
            or "account_status_history_source_system_distribution"
        ),
        transaction_currency_section_key=(
            transaction_currency_section_key or "transaction_currency_by_country_account_type"
        ),
        exposure_ratio_section_key=exposure_ratio_section_key or "exposure_limit_ratio_by_segment",
        transaction_amount_section_key=(
            transaction_amount_section_key or "transaction_amount_mixture_by_account_type"
        ),
        customer_total_aum_section_key=(
            customer_total_aum_section_key or "customer_total_aum_mixture_by_segment_tier"
        ),
    )


def _overlay_scenario(
    *,
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
) -> dict[str, object]:
    scenario = overlays.get(scenario_id, {})
    return scenario if isinstance(scenario, dict) else {}


def _overlay_section_dict(
    *,
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> dict[str, Any]:
    scenario = _overlay_scenario(overlays=overlays, scenario_id=scenario_id)
    section = scenario.get(section_key, {})
    return section if isinstance(section, dict) else {}


def _merge_overlay_section_dict(
    *,
    overlays: dict[str, dict[str, object]],
    scenario_ids: list[str],
    section_key: str,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for scenario_id in scenario_ids:
        section = _overlay_section_dict(
            overlays=overlays,
            scenario_id=scenario_id,
            section_key=section_key,
        )
        merged.update(section)
    return merged


def _select_overlay_scenario_id(
    *,
    overlays: dict[str, dict[str, object]],
    records_by_table: dict[str, list[dict[str, object]]],
    has_lending_signal: bool,
    fallback: str,
    inferred_primary_domain_id: str | None = None,
    graph_path_id: str | None = None,
    feature_flags: set[str] | None = None,
    check_column_values: bool = True,
) -> str:
    table_names = set(records_by_table.keys())
    for scenario_id, scenario in overlays.items():
        if not isinstance(scenario, dict):
            continue
        conditions = scenario.get("activation_conditions", {})
        if not isinstance(conditions, dict):
            continue
        required_tables = conditions.get("requires_tables", [])
        if isinstance(required_tables, list):
            required_set = {str(item) for item in required_tables if str(item)}
            if required_set and not required_set.issubset(table_names):
                continue
        requires_lending = conditions.get("requires_lending_signal")
        if isinstance(requires_lending, bool) and requires_lending != has_lending_signal:
            continue
        cond_domains = conditions.get("domain_ids", [])
        if isinstance(cond_domains, list) and cond_domains:
            if not inferred_primary_domain_id or inferred_primary_domain_id not in {str(x) for x in cond_domains}:
                continue
        cond_paths = conditions.get("concept_path_ids", [])
        if isinstance(cond_paths, list) and cond_paths:
            if not graph_path_id or graph_path_id not in {str(x) for x in cond_paths}:
                continue
        cond_flags = conditions.get("table_feature_flags", [])
        if isinstance(cond_flags, list) and cond_flags:
            required = {str(x) for x in cond_flags}
            actual = feature_flags or set()
            if not required.issubset(actual):
                continue
        required_columns = conditions.get("requires_columns", [])
        if isinstance(required_columns, list) and required_columns:
            if not _has_required_columns(
                records_by_table=records_by_table,
                required_columns=[str(x) for x in required_columns],
            ):
                continue
        if check_column_values:
            required_column_values = conditions.get("requires_column_values", {})
            if isinstance(required_column_values, dict) and required_column_values:
                if not _has_required_column_values(
                    records_by_table=records_by_table,
                    required_column_values=required_column_values,
                ):
                    continue
        return scenario_id
    return fallback


def _derive_table_feature_flags(*, records_by_table: dict[str, list[dict[str, object]]]) -> set[str]:
    flags: set[str] = set()
    if "customers" in records_by_table:
        flags.add("has_customers")
    if "accounts" in records_by_table:
        flags.add("has_accounts")
    if "transactions" in records_by_table:
        flags.add("has_transactions")
    if "crd_exposures" in records_by_table or "exposures" in records_by_table:
        flags.add("has_exposures")
    if "loan_facilities" in records_by_table or "facilities" in records_by_table:
        flags.add("has_facilities")
    return flags


def _has_required_columns(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    required_columns: list[str],
) -> bool:
    if not required_columns:
        return True
    for item in required_columns:
        token = str(item)
        if "." not in token:
            continue
        table_name, column_name = token.split(".", 1)
        rows = records_by_table.get(table_name, [])
        if not rows:
            return False
        if column_name not in rows[0]:
            return False
    return True


def _has_required_column_values(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    required_column_values: dict[str, object],
) -> bool:
    for token, expected_values_obj in required_column_values.items():
        key = str(token)
        if "." not in key:
            continue
        table_name, column_name = key.split(".", 1)
        rows = records_by_table.get(table_name, [])
        if not rows:
            return False
        expected_values = expected_values_obj if isinstance(expected_values_obj, list) else [expected_values_obj]
        expected_set = {str(item) for item in expected_values}
        observed = {str(row.get(column_name, "")) for row in rows}
        if expected_set.isdisjoint(observed):
            return False
    return True


def _sample_account_type_from_overlay(
    *,
    account_row: dict[str, object],
    scenario: dict[str, object],
    age: int,
    country: str,
) -> str:
    distributions = scenario.get("account_type_distributions", {})
    if not isinstance(distributions, dict):
        return ""
    segment_id = _segment_for_account_type(age=age, country=country, scenario=scenario)
    rule = distributions.get(segment_id) or distributions.get("default")
    if not isinstance(rule, dict):
        return ""
    default_rule = distributions.get("default", {})
    default_allowed = default_rule.get("allowed_values", []) if isinstance(default_rule, dict) else []
    rule_allowed = rule.get("allowed_values")
    allowed_source = rule_allowed if isinstance(rule_allowed, list) and rule_allowed else default_allowed
    allowed_values = [str(item) for item in allowed_source if str(item).strip()]
    weights_obj = rule.get("weights", {})
    if not allowed_values or not isinstance(weights_obj, dict):
        return ""
    weights = [max(0.0, float(weights_obj.get(value, 0.0))) for value in allowed_values]
    total_weight = sum(weights)
    if total_weight <= 0:
        return ""
    seed_key = str(account_row.get("account_id", ""))
    return _deterministic_weighted_choice(values=allowed_values, weights=weights, seed_key=seed_key)


def _segment_for_account_type(
    *,
    age: int,
    country: str,
    scenario: dict[str, object] | None = None,
) -> str:
    if age >= 60:
        return "senior"
    if country in {"HK", "SG"} and age <= 35:
        return "young_urban"
    if age <= 35:
        return "young"
    return "mass"


def _align_customer_age_with_scenario(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> None:
    customers = records_by_table.get("customers", [])
    if not customers:
        return
    age_cfg = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if not age_cfg:
        return
    for row in customers:
        country = str(row.get("country", "default")).upper()
        buckets = age_cfg.get(country) or age_cfg.get("default")
        if not isinstance(buckets, list) or not buckets:
            continue
        values: list[int] = []
        weights: list[float] = []
        for item in buckets:
            if not isinstance(item, dict):
                continue
            minimum = int(item.get("min", 18))
            maximum = int(item.get("max", 85))
            if maximum < minimum:
                minimum, maximum = maximum, minimum
            sampled_age = minimum + int(
                (maximum - minimum)
                * _deterministic_unit_interval(
                    seed_key=f"{row.get('customer_id','')}|age|{country}|{minimum}|{maximum}"
                )
            )
            values.append(sampled_age)
            weights.append(max(0.0, float(item.get("weight", 0.0))))
        if not values or sum(weights) <= 0:
            continue
        row["age"] = _deterministic_weighted_choice(
            values=[str(v) for v in values],
            weights=weights,
            seed_key=str(row.get("customer_id", "")),
        )


def _decide_table_record_count_rowwise(table_name: str, record_count: int, has_foreign_keys: bool) -> int:
    payload = load_cardinality_profiles_payload()
    profile_id = str(payload.get("default_rowwise_profile_id", "rowwise_default"))
    return decide_rowwise_table_count(
        profile_payload=payload,
        profile_id=profile_id,
        table_name=table_name,
        record_count=record_count,
        has_foreign_keys=has_foreign_keys,
    )


def _generate_unmatched_table(
    *,
    table_map: dict[str, TableDefinition],
    table_name: str,
    record_count: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
) -> list[dict[str, object]]:
    table_definition = table_map[table_name]
    table_record_count = _decide_table_record_count_rowwise(
        table_name=table_name,
        record_count=record_count,
        has_foreign_keys=bool(table_definition.foreign_key_columns()),
    )
    rows = _generate_table_records(
        table=table_definition,
        table_record_count=table_record_count,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )
    pk_col = table_definition.primary_key_column()
    primary_key_cache[table_name] = [str(row[pk_col]) for row in rows]
    return rows


def _generate_table_records(
    table: TableDefinition,
    table_record_count: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    *,
    semantics_profile_id: str,
    table_to_concept: dict[str, str] | None,
) -> list[dict[str, object]]:
    table_records: list[dict[str, object]] = []
    timestamp_anchor = datetime(2026, 1, 1, 9, 0, 0, tzinfo=SHANGHAI_TIMEZONE)
    ordered_columns = order_columns_for_generation(table=table, profile_id=semantics_profile_id)

    for row_index in range(1, table_record_count + 1):
        row_partial: dict[str, object] = {}
        row: dict[str, object] = {}
        for column in ordered_columns:
            value = sample_column_value(
                table_name=table.name,
                column=column,
                row_index=row_index,
                rng=rng,
                primary_key_cache=primary_key_cache,
                timestamp_anchor=timestamp_anchor,
                table_to_concept=table_to_concept,
                row_partial=row_partial,
                semantics_profile_id=semantics_profile_id,
            )
            row[column.name] = value
            row_partial[column.name] = value
        table_records.append(row)

    apply_fsm_row_overrides(table=table, rows=table_records, rng=rng)
    return table_records


def _enforce_transaction_temporal_consistency(
    records_by_table: dict[str, list[dict[str, object]]],
    *,
    transactions_table: str = "transactions",
    accounts_table: str = "accounts",
    account_id_key: str = "account_id",
    transaction_time_key: str = "transaction_time",
) -> None:
    transaction_rows = records_by_table.get(transactions_table, [])
    if not transaction_rows:
        return

    account_rows = records_by_table.get(accounts_table, [])
    account_time_anchor: dict[str, datetime] = {}
    for row_index, account_row in enumerate(account_rows, start=1):
        account_id = str(account_row.get(account_id_key, ""))
        account_time_anchor[account_id] = datetime(
            2026,
            1,
            1,
            9,
            0,
            0,
            tzinfo=SHANGHAI_TIMEZONE,
        ) + timedelta(minutes=row_index)

    transaction_counter_by_account: dict[str, int] = {}
    for transaction_row in transaction_rows:
        account_id = str(transaction_row.get(account_id_key, ""))
        base_time = account_time_anchor.get(
            account_id,
            datetime(2026, 1, 1, 9, 0, 0, tzinfo=SHANGHAI_TIMEZONE),
        )
        next_counter = transaction_counter_by_account.get(account_id, 0) + 1
        transaction_counter_by_account[account_id] = next_counter
        transaction_time = base_time + timedelta(minutes=next_counter * 3)
        transaction_row[transaction_time_key] = transaction_time.isoformat()


def _align_account_balance_with_transaction_conservation(
    records_by_table: dict[str, list[dict[str, object]]],
    *,
    accounts_table: str = "accounts",
    transactions_table: str = "transactions",
    account_id_key: str = "account_id",
    amount_key: str = "amount",
    balance_key: str = "balance",
) -> None:
    account_rows = records_by_table.get(accounts_table, [])
    transaction_rows = records_by_table.get(transactions_table, [])
    if not account_rows or not transaction_rows:
        return

    transaction_sum_by_account: dict[str, Decimal] = {}
    for transaction_row in transaction_rows:
        account_id = str(transaction_row.get(account_id_key, ""))
        amount_yuan = Decimal(str(transaction_row.get(amount_key, "0")))
        current_sum = transaction_sum_by_account.get(account_id, Decimal("0.00"))
        transaction_sum_by_account[account_id] = current_sum + amount_yuan

    for account_row in account_rows:
        account_id = str(account_row.get(account_id_key, ""))
        account_balance_yuan = transaction_sum_by_account.get(account_id, Decimal("0.00")).quantize(
            MONEY_PRECISION_STANDARD,
            rounding=ROUND_HALF_UP,
        )
        account_row[balance_key] = str(account_balance_yuan)


def _align_transaction_currency_with_customer_country(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    customers = records_by_table.get("customers", [])
    accounts = records_by_table.get("accounts", [])
    transactions = records_by_table.get("transactions", [])
    if not customers or not accounts or not transactions:
        return None
    customer_country = {
        str(row.get("customer_id", "")): str(row.get("country", ""))
        for row in customers
    }
    account_customer = {
        str(row.get("account_id", "")): str(row.get("customer_id", ""))
        for row in accounts
    }
    account_type_by_id = {
        str(row.get("account_id", "")): str(row.get("account_type", "default"))
        for row in accounts
    }
    currency_map = _merged_currency_overlay(
        overlays=overlays,
        scenario_ids=[scenario_id],
        section_key=section_key,
    )
    for row in transactions:
        account_id = str(row.get("account_id", ""))
        customer_id = account_customer.get(account_id, "")
        country = customer_country.get(customer_id, "").upper()
        account_type = account_type_by_id.get(account_id, "default")
        weights = _currency_weights_for(country=country, account_type=account_type, currency_map=currency_map)
        if not weights:
            continue
        values = list(weights.keys())
        w = [float(weights[v]) for v in values]
        row["currency"] = _deterministic_weighted_choice(
            values=values,
            weights=w,
            seed_key=str(row.get("transaction_id", account_id)),
        )
    return scenario_id


def _align_transaction_currency_with_profile_baseline(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    inferred_primary_domain_id: str | None,
) -> None:
    transactions = records_by_table.get("transactions", [])
    if not transactions:
        return
    probs = _load_currency_probs_from_semantics_profile(
        inferred_primary_domain_id=inferred_primary_domain_id
    )
    if not probs:
        return
    currencies = list(probs.keys())
    target_counts = _target_counts_from_probs(total=len(transactions), probs=probs)
    if not target_counts:
        return

    ranked_rows = sorted(
        transactions,
        key=lambda row: _deterministic_unit_interval(
            seed_key=f"{row.get('transaction_id', '')}|currency_profile_rank"
        ),
    )
    cursor = 0
    for currency in currencies:
        count = target_counts.get(currency, 0)
        for _ in range(count):
            if cursor >= len(ranked_rows):
                break
            ranked_rows[cursor]["currency"] = currency
            cursor += 1


def _load_currency_probs_from_semantics_profile(
    *,
    inferred_primary_domain_id: str | None,
) -> dict[str, float]:
    manifest_payload = load_schema_config(SYNTH_MANIFEST)
    profile_id = "main_profile"
    if isinstance(manifest_payload, dict):
        default_profile_id = manifest_payload.get("column_semantics_profile_id_default")
        if isinstance(default_profile_id, str) and default_profile_id.strip():
            profile_id = default_profile_id.strip()
        mapping = manifest_payload.get("column_semantics_profile_by_domain", {})
        if isinstance(mapping, dict) and isinstance(inferred_primary_domain_id, str):
            mapped = mapping.get(inferred_primary_domain_id)
            if isinstance(mapped, str) and mapped.strip():
                profile_id = mapped.strip()

    profiles_payload = load_schema_config(SYNTH_COLUMN_PROFILES)
    if not isinstance(profiles_payload, dict):
        return {}
    profiles = profiles_payload.get("profiles", {})
    if not isinstance(profiles, dict):
        return {}
    selected = profiles.get(profile_id)
    if not isinstance(selected, dict):
        return {}
    columns = selected.get("columns", {})
    if not isinstance(columns, dict):
        return {}
    txn_currency = columns.get("transactions.currency", {})
    if not isinstance(txn_currency, dict):
        return {}
    distribution = txn_currency.get("distribution", {})
    if not isinstance(distribution, dict):
        return {}
    if str(distribution.get("type", "")) != "weighted_enum":
        return {}
    weights = distribution.get("weights", {})
    if not isinstance(weights, dict) or not weights:
        return {}
    cleaned = {str(k).upper(): max(0.0, float(v)) for k, v in weights.items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in cleaned.items()}


def _target_counts_from_probs(*, total: int, probs: dict[str, float]) -> dict[str, int]:
    if total <= 0 or not probs:
        return {}
    raw_counts = {key: probs[key] * total for key in probs}
    floor_counts = {key: int(math.floor(value)) for key, value in raw_counts.items()}
    remainder = total - sum(floor_counts.values())
    if remainder <= 0:
        return floor_counts
    order = sorted(
        probs.keys(),
        key=lambda key: raw_counts[key] - floor_counts[key],
        reverse=True,
    )
    for idx in range(remainder):
        floor_counts[order[idx % len(order)]] += 1
    return floor_counts


def _align_account_status_with_scenario(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    accounts = records_by_table.get("accounts", [])
    if not accounts:
        return None
    status_maps: list[dict[str, object]] = []
    mapping = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if isinstance(mapping, dict):
        status_maps.append(mapping)
    for row in accounts:
        account_type = str(row.get("account_type", "default"))
        picked: dict[str, float] | None = None
        for mapping in status_maps:
            candidate = mapping.get(account_type) or mapping.get("default")
            if isinstance(candidate, dict):
                picked = {str(k): float(v) for k, v in candidate.items()}
                break
        if not picked:
            continue
        values = list(picked.keys())
        weights = [picked[v] for v in values]
        sampled_status = _deterministic_weighted_choice(
            values=values,
            weights=weights,
            seed_key=str(row.get("account_id", "")),
        )
        row["status"] = _normalize_status_value_for_generation(
            table_name="accounts",
            column_name="status",
            value=sampled_status,
        )
    return scenario_id


def _align_account_status_with_journey_stage(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    accounts = records_by_table.get("accounts", [])
    customers = records_by_table.get("customers", [])
    if not accounts or not customers:
        return None
    journey_map = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if not isinstance(journey_map, dict) or not journey_map:
        return None
    customer_by_id = {
        str(row.get("customer_id", "")): row for row in customers
    }
    for row in accounts:
        customer_id = str(row.get("customer_id", ""))
        customer_row = customer_by_id.get(customer_id, {})
        journey_stage = str(customer_row.get("journey_stage", "")).strip().lower()
        if not journey_stage:
            continue
        candidate = journey_map.get(journey_stage) or journey_map.get("default")
        if not isinstance(candidate, dict) or not candidate:
            continue
        picked = {str(key): float(value) for key, value in candidate.items()}
        values = list(picked.keys())
        weights = [picked[value] for value in values]
        sampled_status = _deterministic_weighted_choice(
            values=values,
            weights=weights,
            seed_key=f"{row.get('account_id', '')}|{journey_stage}|status_journey_stage",
        )
        row["status"] = _normalize_status_value_for_generation(
            table_name="accounts",
            column_name="status",
            value=sampled_status,
        )
    return scenario_id


def _align_account_status_with_customer_kyc(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    accounts = records_by_table.get("accounts", [])
    customers = records_by_table.get("customers", [])
    if not accounts or not customers:
        return None
    kyc_map = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if not isinstance(kyc_map, dict) or not kyc_map:
        return None
    customer_by_id = {
        str(row.get("customer_id", "")): row for row in customers
    }
    for row in accounts:
        customer_id = str(row.get("customer_id", ""))
        customer_row = customer_by_id.get(customer_id, {})
        kyc_status = _extract_customer_kyc_status(customer_row).lower()
        candidate = kyc_map.get(kyc_status) or kyc_map.get("default")
        if not isinstance(candidate, dict) or not candidate:
            continue
        picked = {str(key): float(value) for key, value in candidate.items()}
        values = list(picked.keys())
        weights = [picked[value] for value in values]
        sampled_status = _deterministic_weighted_choice(
            values=values,
            weights=weights,
            seed_key=f"{row.get('account_id', '')}|{kyc_status}|status_customer_kyc",
        )
        row["status"] = _normalize_status_value_for_generation(
            table_name="accounts",
            column_name="status",
            value=sampled_status,
        )  # AUDIT: account status aligned with customer KYC profile
    return scenario_id


def _extract_customer_kyc_status(customer_row: dict[str, object]) -> str:
    raw_profile = customer_row.get("profile_json")
    if raw_profile is None:
        return "missing"
    try:
        profile = json.loads(str(raw_profile))
    except Exception:
        return "missing"
    if not isinstance(profile, dict):
        return "missing"
    kyc_status = str(profile.get("kyc_status", "")).strip().upper()
    return kyc_status or "missing"


def _synchronize_account_status_history_fact(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> None:
    accounts = records_by_table.get("accounts", [])
    if not accounts:
        return
    history_table_name = _pick_account_status_history_table_name(records_by_table=records_by_table)
    if history_table_name is None:
        return

    template_row = records_by_table.get(history_table_name, [])
    template_keys = set(template_row[0].keys()) if template_row else set()
    synchronized_rows: list[dict[str, object]] = []
    history_render_config = _load_account_status_history_render_config()
    source_system_distribution = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )

    for account in accounts:
        account_id = str(account.get("account_id", "")).strip()
        if not account_id:
            continue
        final_status = _normalize_status_value_for_generation(
            table_name="accounts",
            column_name="status",
            value=str(account.get("status", "open")),
        )
        status_timeline = _account_status_timeline_from_lifecycle_config(final_status=final_status)
        opened_time = _parse_iso_time_for_generation(str(account.get("opened_time", "")))
        base_time = opened_time or datetime(2026, 1, 1, 9, 0, 0, tzinfo=SHANGHAI_TIMEZONE)
        source_system_value = _sample_account_status_history_source_system(
            account_id=account_id,
            distribution=source_system_distribution,
            fallback=history_render_config["source_system"],
        )
        for index, status in enumerate(status_timeline, start=1):
            status_time = base_time + timedelta(minutes=index - 1)
            is_current = "Y" if index == len(status_timeline) else "N"
            row = {key: "" for key in template_keys}
            row["status_event_id"] = _render_history_template(
                template=history_render_config["status_event_id_template"],
                account_id=account_id,
                sequence=index,
                status=status,
            )
            row["account_id"] = account_id
            row["status"] = status
            row["status_time"] = status_time.isoformat()
            row["is_current"] = is_current
            row["source_system"] = source_system_value
            row["trace_id"] = _render_history_template(
                template=history_render_config["trace_id_template"],
                account_id=account_id,
                sequence=index,
                status=status,
            )
            synchronized_rows.append(row)

    records_by_table[history_table_name] = synchronized_rows


def _load_account_status_history_render_config() -> dict[str, str]:
    lifecycle = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    rules = lifecycle.get("state_machine_rules", []) if isinstance(lifecycle, dict) else []
    default_config = {
        "status_event_id_template": "evt_{account_id}_{sequence:03d}",
        "trace_id_template": "trace_{account_id}_{sequence:03d}",
        "source_system": "core_banking",
    }
    if not isinstance(rules, list):
        return default_config
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if str(rule.get("rule_id", "")) != "HARD_ACCOUNT_STATUS_SCD_LIFECYCLE":
            continue
        status_event_id_template = str(
            rule.get(
                "history_status_event_id_template",
                default_config["status_event_id_template"],
            )
        ).strip()
        trace_id_template = str(
            rule.get(
                "history_trace_id_template",
                default_config["trace_id_template"],
            )
        ).strip()
        source_system = str(
            rule.get(
                "history_source_system_value",
                default_config["source_system"],
            )
        ).strip()
        return {
            "status_event_id_template": status_event_id_template
            or default_config["status_event_id_template"],
            "trace_id_template": trace_id_template or default_config["trace_id_template"],
            "source_system": source_system or default_config["source_system"],
        }
    return default_config


def _render_history_template(*, template: str, account_id: str, sequence: int, status: str) -> str:
    safe_template = str(template).strip()
    if not safe_template:
        safe_template = "{account_id}_{sequence:03d}"
    try:
        return safe_template.format(account_id=account_id, sequence=sequence, status=status)
    except Exception:
        return f"{account_id}_{sequence:03d}"


def _sample_account_status_history_source_system(
    *,
    account_id: str,
    distribution: dict[str, Any],
    fallback: str,
) -> str:
    if not isinstance(distribution, dict) or not distribution:
        return fallback
    values = [str(key).strip() for key in distribution.keys() if str(key).strip()]
    if not values:
        return fallback
    weights = [max(0.0, float(distribution.get(value, 0.0))) for value in values]
    if sum(weights) <= 0:
        return fallback
    return _deterministic_weighted_choice(
        values=values,
        weights=weights,
        seed_key=f"{account_id}|scd_source_system",
    )


def _pick_account_status_history_table_name(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> str | None:
    preferred_names = (
        "account_status_scd",
        "account_status_history",
        "account_status_events",
        "account_status_event",
    )
    for name in preferred_names:
        if name in records_by_table:
            return name
    for table_name in records_by_table:
        lowered = table_name.lower()
        if "account_status" in lowered and ("history" in lowered or "event" in lowered or "scd" in lowered):
            return table_name
    return None


def _account_status_timeline_by_final_status(*, final_status: str) -> list[str]:
    return _account_status_timeline_from_lifecycle_config(final_status=final_status)


def _account_status_timeline_from_lifecycle_config(*, final_status: str) -> list[str]:
    normalized_final = final_status.strip().lower()
    if not normalized_final:
        return ["open"]

    lifecycle = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    rules = lifecycle.get("state_machine_rules", []) if isinstance(lifecycle, dict) else []
    if not isinstance(rules, list):
        return ["open", "pending", "active"]

    target_rule: dict[str, object] | None = None
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if str(rule.get("rule_id", "")) == "HARD_ACCOUNT_STATUS_SCD_LIFECYCLE":
            target_rule = rule
            break
    if target_rule is None:
        return ["open", "pending", "active"]

    initial_states_raw = target_rule.get("initial_states", [])
    initial_states = [
        _normalize_status_value_for_generation(
            table_name="account_status_scd",
            column_name="status",
            value=str(state),
        )
        for state in (initial_states_raw if isinstance(initial_states_raw, list) else [])
        if str(state).strip()
    ]
    transitions_raw = target_rule.get("allowed_transitions", {})
    if not isinstance(transitions_raw, dict):
        return initial_states or ["open"]

    transitions: dict[str, list[str]] = {}
    for raw_from_state, raw_to_list in transitions_raw.items():
        from_state = _normalize_status_value_for_generation(
            table_name="account_status_scd",
            column_name="status",
            value=str(raw_from_state),
        )
        if not from_state:
            continue
        to_list: list[str] = []
        if isinstance(raw_to_list, list):
            for raw_to_state in raw_to_list:
                to_state = _normalize_status_value_for_generation(
                    table_name="account_status_scd",
                    column_name="status",
                    value=str(raw_to_state),
                )
                if to_state:
                    to_list.append(to_state)
        transitions[from_state] = to_list

    path = _shortest_status_path(
        initial_states=initial_states or ["open"],
        transitions=transitions,
        final_status=normalized_final,
    )
    if path:
        return path
    # Fallback should still be deterministic and readable.
    return [normalized_final]


def _shortest_status_path(
    *,
    initial_states: list[str],
    transitions: dict[str, list[str]],
    final_status: str,
) -> list[str]:
    if final_status in initial_states:
        return [final_status]
    queue: list[list[str]] = [[state] for state in initial_states if state]
    visited: set[str] = set(initial_states)
    max_depth = 16
    while queue:
        path = queue.pop(0)
        current = path[-1]
        if len(path) > max_depth:
            continue
        for next_state in transitions.get(current, []):
            if not next_state:
                continue
            candidate = [*path, next_state]
            if next_state == final_status:
                return candidate
            if next_state in visited:
                continue
            visited.add(next_state)
            queue.append(candidate)
    return []


def _align_credit_exposure_with_facility_limit(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    facilities = records_by_table.get("crd_facilities", []) or records_by_table.get("facilities", [])
    exposures = records_by_table.get("crd_exposures", []) or records_by_table.get("exposures", [])
    if not facilities or not exposures:
        return None
    limit_by_facility: dict[str, Decimal] = {}
    for row in facilities:
        fid = str(row.get("facility_id", ""))
        try:
            limit_by_facility[fid] = Decimal(str(row.get("limit_amount", "0")))
        except Exception:
            continue
    ratio_map = _merged_exposure_ratio_overlay(
        overlays=overlays,
        scenario_ids=[scenario_id],
        section_key=section_key,
    )
    for row in exposures:
        fid = str(row.get("facility_id", ""))
        limit = limit_by_facility.get(fid)
        if limit is None:
            continue
        segment = _segment_for_exposure(row=row)
        ratio_cfg = ratio_map.get(segment) or ratio_map.get("default") or {"min_ratio": 0.6, "max_ratio": 0.85}
        minimum = Decimal(str(ratio_cfg.get("min_ratio", 0.6)))
        maximum = Decimal(str(ratio_cfg.get("max_ratio", 0.85)))
        if maximum < minimum:
            minimum, maximum = maximum, minimum
        seed = (sum(ord(ch) for ch in str(row.get("exposure_id", fid))) % 10_000) / Decimal("10000")
        ratio = minimum + (maximum - minimum) * seed
        target = (limit * ratio).quantize(MONEY_PRECISION_STANDARD, rounding=ROUND_HALF_UP)
        row["ead_amount"] = str(max(target, Decimal("0.01")))
    return scenario_id


def _segment_for_exposure(*, row: dict[str, object]) -> str:
    try:
        overdue_days = int(str(row.get("overdue_days", "0")))
    except ValueError:
        overdue_days = 0
    if overdue_days >= 90:
        return "senior"
    if overdue_days <= 7:
        return "young"
    return "default"


def _merged_currency_overlay(
    *,
    overlays: dict[str, dict[str, object]],
    scenario_ids: list[str],
    section_key: str,
) -> dict[str, Any]:
    return _merge_overlay_section_dict(
        overlays=overlays,
        scenario_ids=scenario_ids,
        section_key=section_key,
    )


def _currency_weights_for(*, country: str, account_type: str, currency_map: dict[str, Any]) -> dict[str, float]:
    country_obj = currency_map.get(country) or currency_map.get("default")
    if not isinstance(country_obj, dict):
        return {}
    rule = country_obj.get(account_type) or country_obj.get("default")
    if not isinstance(rule, dict):
        return {}
    return {str(k): max(0.0, float(v)) for k, v in rule.items()}


def _merged_exposure_ratio_overlay(
    *,
    overlays: dict[str, dict[str, object]],
    scenario_ids: list[str],
    section_key: str,
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    merged = _merge_overlay_section_dict(
        overlays=overlays,
        scenario_ids=scenario_ids,
        section_key=section_key,
    )
    for key, value in merged.items():
        if isinstance(value, dict):
            result[str(key)] = {
                "min_ratio": float(value.get("min_ratio", 0.6)),
                "max_ratio": float(value.get("max_ratio", 0.85)),
            }
    return result


def _deterministic_weighted_choice(*, values: list[str], weights: list[float], seed_key: str) -> str:
    if not values:
        return ""
    total = sum(max(0.0, w) for w in weights)
    if total <= 0:
        return values[0]
    score = _deterministic_unit_interval(seed_key=seed_key)
    cumulative = 0.0
    for idx, value in enumerate(values):
        cumulative += max(0.0, weights[idx]) / total
        if score <= cumulative:
            return value
    return values[-1]


def _align_collateral_haircut_with_asset_class(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    rows = records_by_table.get("collateral_items", [])
    if not rows:
        return
    haircut_by_asset_class = {
        "cash": Decimal("0.02"),
        "bond": Decimal("0.08"),
        "equity": Decimal("0.18"),
        "real_estate": Decimal("0.25"),
        "other": Decimal("0.30"),
    }
    for row in rows:
        asset_class = str(row.get("asset_class", "other")).lower()
        haircut = haircut_by_asset_class.get(asset_class, haircut_by_asset_class["other"])
        row["haircut_percent"] = str(haircut)


def _enrich_transaction_details_with_related_context(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    customers = records_by_table.get("customers", [])
    accounts = records_by_table.get("accounts", [])
    transactions = records_by_table.get("transactions", [])
    if not customers or not accounts or not transactions:
        return
    customer_by_id = {str(row.get("customer_id", "")): row for row in customers}
    account_by_id = {str(row.get("account_id", "")): row for row in accounts}
    for txn in transactions:
        details_raw = txn.get("details_json")
        if details_raw is None:
            continue
        try:
            details = json.loads(str(details_raw))
        except Exception:
            continue
        if not isinstance(details, dict):
            continue
        account_id = str(txn.get("account_id", ""))
        account = account_by_id.get(account_id, {})
        customer_id = str(account.get("customer_id", ""))
        customer = customer_by_id.get(customer_id, {})
        country = str(customer.get("country", "CN")).upper()
        age = customer.get("age")
        account_type = str(account.get("account_type", ""))
        details.setdefault("counterparty", {})
        if isinstance(details["counterparty"], dict):
            details["counterparty"]["country"] = country
        details.setdefault("settlement", {})
        if isinstance(details["settlement"], dict):
            details["settlement"]["currency"] = str(txn.get("currency", "CNY")).upper()
        details["customer_context"] = {
            "country": country,
            "age": age,
            "account_type": account_type,
        }
        txn["details_json"] = json.dumps(details, ensure_ascii=True)


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


def _align_transaction_amount_with_scenario(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    transactions = records_by_table.get("transactions", [])
    accounts = records_by_table.get("accounts", [])
    if not transactions or not accounts:
        return None
    amount_cfg = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if not amount_cfg:
        return None
    account_type_by_id = {
        str(row.get("account_id", "")): str(row.get("account_type", "default"))
        for row in accounts
    }
    currency_scale_map, default_scale = _load_currency_decimal_scale_config()
    base_currency, rates = _load_fx_rate_profile()
    for row in transactions:
        account_id = str(row.get("account_id", ""))
        account_type = account_type_by_id.get(account_id, "default")
        spec = amount_cfg.get(account_type) or amount_cfg.get("default")
        if not isinstance(spec, dict):
            continue
        components = spec.get("components", [])
        if not isinstance(components, list) or not components:
            continue
        options: list[tuple[Decimal, Decimal, float]] = []
        for comp in components:
            if not isinstance(comp, dict):
                continue
            minimum = Decimal(str(comp.get("min_base", "100.00")))
            maximum = Decimal(str(comp.get("max_base", "1000000.00")))
            if maximum < minimum:
                minimum, maximum = maximum, minimum
            minimum = max(minimum, Decimal("0.01"))
            maximum = max(maximum, minimum)
            weight = max(0.0, float(comp.get("weight", 0.0)))
            options.append((minimum, maximum, weight))
        if not options:
            continue
        picked_idx = _deterministic_weighted_choice(
            values=[str(i) for i in range(len(options))],
            weights=[item[2] for item in options],
            seed_key=f"{row.get('transaction_id', account_id)}|amount_component",
        )
        index = int(picked_idx) if picked_idx.isdigit() else 0
        index = max(0, min(index, len(options) - 1))
        minimum, maximum, _ = options[index]
        log_min = math.log(float(minimum))
        log_max = math.log(float(maximum))
        unit_score = _deterministic_unit_interval(
            seed_key=f"{row.get('transaction_id', account_id)}|amount_value|{minimum}|{maximum}"
        )
        sampled_base = Decimal(str(math.exp(log_min + (log_max - log_min) * unit_score)))
        sampled_base = max(minimum, min(sampled_base, maximum))
        currency_code = str(row.get("currency", "")).upper().strip()
        amount = _convert_base_to_target_amount(
            amount_in_base_currency=sampled_base,
            target_currency=currency_code or base_currency,
            base_currency=base_currency,
            rates=rates,
        )
        scale = currency_scale_map.get(currency_code, default_scale)
        row["amount"] = _amount_string_from_decimal(amount=amount, scale=scale)
    return scenario_id


def _align_customer_total_aum_with_segment_tier(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    overlays: dict[str, dict[str, object]],
    scenario_id: str,
    section_key: str,
) -> str | None:
    customers = records_by_table.get("customers", [])
    customer_segments = records_by_table.get("customer_segments", [])
    if not customers or not customer_segments:
        return None
    aum_cfg = _overlay_section_dict(
        overlays=overlays,
        scenario_id=scenario_id,
        section_key=section_key,
    )
    if not aum_cfg:
        return None

    segment_by_id = {
        str(row.get("customer_segment_id", "")): row for row in customer_segments
    }
    for customer in customers:
        segment_id = str(customer.get("customer_segment_id", ""))
        segment_row = segment_by_id.get(segment_id, {})
        segment_tier = str(segment_row.get("segment_tier", "default")).upper().strip()
        tier_rule = aum_cfg.get(segment_tier) or aum_cfg.get("default")
        if not isinstance(tier_rule, dict):
            continue

        components = tier_rule.get("components", [])
        if not isinstance(components, list) or not components:
            continue
        options: list[tuple[Decimal, Decimal, float]] = []
        for comp in components:
            if not isinstance(comp, dict):
                continue
            minimum = Decimal(str(comp.get("min_base", "100.00")))
            maximum = Decimal(str(comp.get("max_base", "1000000.00")))
            if maximum < minimum:
                minimum, maximum = maximum, minimum
            minimum = max(minimum, Decimal("0.01"))
            maximum = max(maximum, minimum)
            weight = max(0.0, float(comp.get("weight", 0.0)))
            options.append((minimum, maximum, weight))
        if not options:
            continue

        customer_id = str(customer.get("customer_id", segment_id))
        picked_idx = _deterministic_weighted_choice(
            values=[str(i) for i in range(len(options))],
            weights=[item[2] for item in options],
            seed_key=f"{customer_id}|total_aum_component|{segment_tier}",
        )
        index = int(picked_idx) if picked_idx.isdigit() else 0
        index = max(0, min(index, len(options) - 1))
        minimum, maximum, _ = options[index]
        log_min = math.log(float(minimum))
        log_max = math.log(float(maximum))
        unit_score = _deterministic_unit_interval(
            seed_key=f"{customer_id}|total_aum_value|{segment_tier}|{minimum}|{maximum}"
        )
        sampled = Decimal(str(math.exp(log_min + (log_max - log_min) * unit_score)))
        sampled = max(minimum, min(sampled, maximum))
        customer["total_aum"] = _amount_string_from_decimal(amount=sampled, scale=2)
    return scenario_id


def _enrich_customer_profile_with_segment_context(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    customers = records_by_table.get("customers", [])
    customer_segments = records_by_table.get("customer_segments", [])
    if not customers or not customer_segments:
        return

    segment_by_id = {
        str(row.get("customer_segment_id", "")): row for row in customer_segments
    }
    risk_profile_by_band = {
        "LOW": "LOW",
        "MEDIUM": "MODERATE",
        "HIGH": "HIGH",
        "VERY_HIGH": "HIGH",
    }

    for customer in customers:
        raw_profile = customer.get("profile_json")
        if raw_profile is None:
            continue
        try:
            profile = json.loads(str(raw_profile))
        except Exception:
            continue
        if not isinstance(profile, dict):
            continue

        segment_id = str(customer.get("customer_segment_id", ""))
        segment_row = segment_by_id.get(segment_id, {})
        segment_name = str(segment_row.get("segment_name", "")).strip()
        segment_tier = str(segment_row.get("segment_tier", "")).upper().strip()
        risk_band = str(segment_row.get("risk_band", "")).upper().strip()

        if risk_band in risk_profile_by_band:
            profile["risk_profile"] = risk_profile_by_band[risk_band]
        profile["segment_context"] = {
            "segment_name": segment_name,
            "segment_tier": segment_tier,
            "risk_band": risk_band,
        }
        customer["profile_json"] = json.dumps(profile, ensure_ascii=True)


def _deterministic_unit_interval(*, seed_key: str) -> float:
    digest = hashlib.sha256(seed_key.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return value / float(2**64 - 1)


def _apply_lifecycle_constraints_for_generation(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    payload = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    if not isinstance(payload, dict):
        return
    _apply_temporal_order_rules_for_generation(
        rules=payload.get("temporal_order_rules", []),
        records_by_table=records_by_table,
    )
    _apply_cross_table_temporal_rules_for_generation(
        rules=payload.get("cross_table_temporal_rules", []),
        records_by_table=records_by_table,
    )
    _apply_state_machine_rules_for_generation(
        rules=payload.get("state_machine_rules", []),
        records_by_table=records_by_table,
    )
    _apply_business_conservation_rules_for_generation(
        rules=payload.get("business_conservation_rules", []),
        records_by_table=records_by_table,
    )


def _apply_temporal_order_rules_for_generation(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    if not isinstance(rules, list):
        return
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        table_name = str(raw_rule.get("table_name", ""))
        constraints = raw_rule.get("constraints", [])
        if not table_name or not isinstance(constraints, list):
            continue
        rows = records_by_table.get(table_name, [])
        if not rows:
            continue
        for row in rows:
            for constraint in constraints:
                if not isinstance(constraint, dict):
                    continue
                left_column = str(constraint.get("left_column", ""))
                right_column = str(constraint.get("right_column", ""))
                if left_column not in row or right_column not in row:
                    continue
                left_time = _parse_iso_time_for_generation(str(row.get(left_column, "")))
                right_time = _parse_iso_time_for_generation(str(row.get(right_column, "")))
                if left_time is None or right_time is None:
                    continue
                if left_time > right_time:
                    row[right_column] = left_time.isoformat()


def _apply_cross_table_temporal_rules_for_generation(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    if not isinstance(rules, list):
        return
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        left_table = str(raw_rule.get("left_table_name", ""))
        right_table = str(raw_rule.get("right_table_name", ""))
        left_keys = [str(item) for item in raw_rule.get("left_key_columns", []) or [] if str(item)]
        right_keys = [str(item) for item in raw_rule.get("right_foreign_key_columns", []) or [] if str(item)]
        left_time_column = str(raw_rule.get("left_time_column", ""))
        right_time_column = str(raw_rule.get("right_time_column", ""))
        if (
            not left_table
            or not right_table
            or not left_keys
            or not right_keys
            or len(left_keys) != len(right_keys)
            or not left_time_column
            or not right_time_column
        ):
            continue
        left_rows = records_by_table.get(left_table, [])
        right_rows = records_by_table.get(right_table, [])
        if not left_rows or not right_rows:
            continue
        left_time_by_key: dict[tuple[str, ...], datetime] = {}
        for row in left_rows:
            key = tuple(str(row.get(column, "")) for column in left_keys)
            parsed_time = _parse_iso_time_for_generation(str(row.get(left_time_column, "")))
            if parsed_time is None:
                continue
            existing = left_time_by_key.get(key)
            if existing is None or parsed_time < existing:
                left_time_by_key[key] = parsed_time
        for row in right_rows:
            key = tuple(str(row.get(column, "")) for column in right_keys)
            left_time = left_time_by_key.get(key)
            if left_time is None:
                continue
            right_time = _parse_iso_time_for_generation(str(row.get(right_time_column, "")))
            if right_time is None:
                continue
            if right_time < left_time:
                row[right_time_column] = left_time.isoformat()


def _apply_state_machine_rules_for_generation(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    if not isinstance(rules, list):
        return
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        table_name = str(raw_rule.get("table_name", ""))
        status_column = str(raw_rule.get("status_column", "status"))
        time_column = str(raw_rule.get("sequence_time_column", ""))
        entity_keys = [str(item) for item in raw_rule.get("entity_key_columns", []) or [] if str(item)]
        initial_states = [
            _normalize_status_value_for_generation(
                table_name=table_name,
                column_name=status_column,
                value=str(item),
            )
            for item in raw_rule.get("initial_states", []) or []
            if str(item)
        ]
        singleton_allowed_states = [
            _normalize_status_value_for_generation(
                table_name=table_name,
                column_name=status_column,
                value=str(item),
            )
            for item in raw_rule.get("singleton_allowed_states", []) or []
            if str(item)
        ]
        transition_raw = raw_rule.get("allowed_transitions", {})
        if not table_name or not status_column or not time_column or not entity_keys:
            continue
        if not isinstance(transition_raw, dict):
            continue
        rows = records_by_table.get(table_name, [])
        if not rows or not _rows_have_columns(rows=rows, columns=[status_column, time_column, *entity_keys]):
            continue
        allowed: dict[str, list[str]] = {}
        for key, value in transition_raw.items():
            if isinstance(value, list):
                normalized_key = _normalize_status_value_for_generation(
                    table_name=table_name,
                    column_name=status_column,
                    value=str(key),
                )
                allowed[normalized_key] = [
                    _normalize_status_value_for_generation(
                        table_name=table_name,
                        column_name=status_column,
                        value=str(item),
                    )
                    for item in value
                    if str(item)
                ]
        if not allowed:
            continue
        grouped_rows: dict[tuple[str, ...], list[dict[str, object]]] = {}
        for row in rows:
            key = tuple(str(row.get(column, "")) for column in entity_keys)
            grouped_rows.setdefault(key, []).append(row)
        for key_tuple, group in grouped_rows.items():
            sorted_rows = sorted(group, key=lambda item: str(item.get(time_column, "")))
            if not sorted_rows:
                continue
            if len(sorted_rows) == 1 and singleton_allowed_states:
                singleton_row = sorted_rows[0]
                chosen_singleton_state = _deterministic_weighted_choice(
                    values=singleton_allowed_states,
                    weights=[1.0] * len(singleton_allowed_states),
                    seed_key=f"{'|'.join(key_tuple)}|singleton_state",
                )
                singleton_row[status_column] = chosen_singleton_state
                continue
            first_row = sorted_rows[0]
            first_status = _normalize_status_value_for_generation(
                table_name=table_name,
                column_name=status_column,
                value=str(first_row.get(status_column, "")),
            )
            if initial_states and first_status not in initial_states:
                first_status = initial_states[0]
                first_row[status_column] = first_status
            if not first_status:
                first_status = initial_states[0] if initial_states else list(allowed.keys())[0]
                first_row[status_column] = first_status
            previous_status = first_status
            for row in sorted_rows[1:]:
                current_status = _normalize_status_value_for_generation(
                    table_name=table_name,
                    column_name=status_column,
                    value=str(row.get(status_column, "")),
                )
                allowed_next = allowed.get(previous_status, [previous_status])
                if current_status not in allowed_next:
                    picked = _deterministic_weighted_choice(
                        values=allowed_next,
                        weights=[1.0] * len(allowed_next),
                        seed_key="|".join(key_tuple),
                    )
                    row[status_column] = picked
                    previous_status = picked
                    continue
                row[status_column] = current_status
                previous_status = current_status


def _rows_have_columns(*, rows: list[dict[str, object]], columns: list[str]) -> bool:
    if not rows:
        return False
    first_row = rows[0]
    return all(column in first_row for column in columns)


def _parse_iso_time_for_generation(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _apply_business_conservation_rules_for_generation(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    if not isinstance(rules, list):
        return
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        rule_type = str(raw_rule.get("type", "")).strip()
        if rule_type == "intra_row_numeric_range":
            table_name = str(raw_rule.get("table_name", "")).strip()
            column_name = str(raw_rule.get("column_name", "")).strip()
            if not table_name or not column_name:
                continue
            rows = records_by_table.get(table_name, [])
            if not rows:
                continue
            min_value = raw_rule.get("min_value")
            max_value = raw_rule.get("max_value")
            min_decimal = _safe_decimal(min_value)
            max_decimal = _safe_decimal(max_value)
            for row in rows:
                value = _safe_decimal(row.get(column_name))
                if value is None:
                    continue
                if min_decimal is not None and value < min_decimal:
                    value = min_decimal
                if max_decimal is not None and value > max_decimal:
                    value = max_decimal
                row[column_name] = str(value)
            continue

        if rule_type == "state_requires_non_null_time":
            table_name = str(raw_rule.get("table_name", "")).strip()
            state_column = str(raw_rule.get("state_column", "")).strip()
            time_column = str(raw_rule.get("time_column", "")).strip()
            required_states = {
                str(item).strip().lower()
                for item in (raw_rule.get("required_states", []) or [])
                if str(item).strip()
            }
            if not table_name or not state_column or not time_column or not required_states:
                continue
            rows = records_by_table.get(table_name, [])
            if not rows:
                continue
            for row in rows:
                state_value = str(row.get(state_column, "")).strip().lower()
                if state_value not in required_states:
                    continue
                time_value = row.get(time_column)
                if time_value is None or str(time_value).strip() == "":
                    fallback = row.get("expected_close_date") or row.get("opened_time") or row.get("created_time")
                    if fallback is not None and str(fallback).strip():
                        row[time_column] = str(fallback)
            continue



def _safe_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _enforce_sales_opportunity_close_time_order(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
) -> None:
    rows = records_by_table.get("sales_opportunities", [])
    if not rows:
        return
    for row in rows:
        if "opportunity_json" in row:
            row["opportunity_json"] = ""

        stage = str(row.get("opportunity_stage", "")).strip().lower()
        is_closed_stage = stage in {"closed_won", "closed_lost"}
        if not is_closed_stage:
            row["actual_close_time"] = ""
            continue

        expected_time = _parse_iso_time_for_generation(str(row.get("expected_close_date", "")).strip())
        actual_time = _parse_iso_time_for_generation(str(row.get("actual_close_time", "")).strip())
        if expected_time is None and actual_time is not None:
            row["expected_close_date"] = actual_time.isoformat()
            continue
        if expected_time is None:
            continue
        if actual_time is None or actual_time < expected_time:
            row["actual_close_time"] = expected_time.isoformat()


def _normalize_status_value_for_generation(*, table_name: str, column_name: str, value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        return normalized
    for rule in _load_status_normalization_rules_for_generation():
        if rule["table_name"] != table_name or rule["column_name"] != column_name:
            continue
        alias_to_canonical = rule["alias_to_canonical"]
        mapped = alias_to_canonical.get(normalized)
        if mapped:
            return mapped
    return normalized


def _load_status_normalization_rules_for_generation() -> tuple[dict[str, object], ...]:
    payload = load_schema_config(SYNTH_STATUS_NORMALIZATION)
    if not isinstance(payload, dict):
        return ()
    rules = payload.get("rules", [])
    if not isinstance(rules, list):
        return ()
    parsed: list[dict[str, object]] = []
    for raw in rules:
        if not isinstance(raw, dict):
            continue
        table_name = str(raw.get("table_name", ""))
        column_name = str(raw.get("column_name", ""))
        canonical_values = raw.get("canonical_values", {})
        if not table_name or not column_name or not isinstance(canonical_values, dict):
            continue
        alias_to_canonical: dict[str, str] = {}
        for canonical, aliases in canonical_values.items():
            canonical_text = str(canonical).strip().lower()
            if not canonical_text or not isinstance(aliases, list):
                continue
            alias_to_canonical[canonical_text] = canonical_text
            for alias in aliases:
                alias_text = str(alias).strip().lower()
                if alias_text:
                    alias_to_canonical[alias_text] = canonical_text
        parsed.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "alias_to_canonical": alias_to_canonical,
            }
        )
    return tuple(parsed)
