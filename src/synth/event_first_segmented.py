from __future__ import annotations

import random
from typing import Any

from src.schema.models import SchemaDefinition, TableDefinition
from src.synth.cardinality_eval import load_cardinality_profiles_payload
from src.synth.generation_chain_resolver import ResolvedEventChain
from src.synth.event_first_generator import (
    _first_decimal_column_name,
    _first_timestamp_column_name,
)
from src.synth.generation_stages import (
    PIPELINE_STAGE_EXPAND_CHILDREN,
    PIPELINE_STAGE_POST_ADJUST,
    PIPELINE_STAGE_SAMPLE_ROOTS,
    stage_record,
)
from src.synth.generator import (
    GeneratedDataset,
    _generate_table_records,
    _generate_unmatched_table,
    apply_post_generation_constraints,
)


def generate_event_first_segmented_three(
    *,
    schema: SchemaDefinition,
    ordered_tables: list[str],
    record_count: int,
    seed: int,
    chain: ResolvedEventChain,
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
    inferred_primary_domain_id: str | None = None,
) -> GeneratedDataset:
    """
    Three-tier chain with per-customer segment: drives accounts/customer and txns/account ranges.

    Segment labels and weights come from ``synth/topology/cardinality_profiles.json`` (type ``segmented_three_tier``).
    """
    telemetry: list[dict[str, object]] = []

    if len(chain.concept_path) != 3:
        raise ValueError("segmented_three_tier expects exactly three concepts in concept_path")

    rng = random.Random(seed)
    table_map = schema.table_map()
    segments_cfg, segment_ids, weights = _load_segment_config_for_chain(
        profile_id=chain.cardinality_profile_id,
    )

    c0, c1, c2 = chain.concept_path
    t0 = chain.table_by_concept[c0]
    t1 = chain.table_by_concept[c1]
    t2 = chain.table_by_concept[c2]

    n0 = int(record_count)
    if n0 <= 0:
        raise ValueError("record_count must be positive")

    table_def0 = table_map[t0]
    table_def1 = table_map[t1]
    table_def2 = table_map[t2]

    primary_key_cache: dict[str, list[str]] = {}

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_SAMPLE_ROOTS,
            pipeline_mode="event_first_segmented",
            tables=t0,
            detail="segmented_tier0",
        )
    )

    rows0 = _generate_tier0_rows(
        table_def=table_def0,
        n0=n0,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
        table_name=t0,
        pk_column=chain.pk_column_by_concept[c0],
    )
    pk0 = chain.pk_column_by_concept[c0]
    fk1 = chain.fk_child_to_parent_column[(c1, c0)]

    segment_per_customer = _sample_segment_for_each_customer(
        n0=n0,
        rng=rng,
        segment_ids=segment_ids,
        weights=weights,
    )
    customer_index_for_account = _build_customer_index_for_account(
        n0=n0,
        segment_per_customer=segment_per_customer,
        segments_cfg=segments_cfg,
        segment_ids=segment_ids,
        rng=rng,
    )
    n1 = len(customer_index_for_account)

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_EXPAND_CHILDREN,
            pipeline_mode="event_first_segmented",
            tables=f"{t1},{t2}",
            detail="segmented_tier1_tier2",
        )
    )

    rows1 = _generate_tier1_rows_with_customer_fk(
        table_def=table_def1,
        n1=n1,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
        customer_index_for_account=customer_index_for_account,
        rows0=rows0,
        fk1=fk1,
        pk0=pk0,
        table_name=t1,
        pk1=chain.pk_column_by_concept[c1],
    )

    pk1 = chain.pk_column_by_concept[c1]
    fk2 = chain.fk_child_to_parent_column[(c2, c1)]
    transactions_per_account = _sample_transactions_per_account(
        n1=n1,
        customer_index_for_account=customer_index_for_account,
        segment_per_customer=segment_per_customer,
        segments_cfg=segments_cfg,
        segment_ids=segment_ids,
        rng=rng,
    )
    rows2 = _generate_tier2_rows_with_account_fk(
        table_def=table_def2,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
        transactions_per_account=transactions_per_account,
        rows1=rows1,
        fk2=fk2,
        pk1=pk1,
        table_name=t2,
        pk2=chain.pk_column_by_concept[c2],
    )

    records_by_table = _build_records_by_table_with_unmatched(
        t0=t0,
        t1=t1,
        t2=t2,
        rows0=rows0,
        rows1=rows1,
        rows2=rows2,
        ordered_tables=ordered_tables,
        table_map=table_map,
        record_count=record_count,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )

    transaction_table, accounts_table, account_id_key, time_key, amount_key, balance_key = (
        _resolve_post_adjust_columns_for_segmented(
            chain=chain,
            table_map=table_map,
            c0=c0,
            c1=c1,
            c2=c2,
        )
    )

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_POST_ADJUST,
            pipeline_mode="event_first_segmented",
            detail="apply_post_generation_constraints",
        )
    )

    post_adjust_result = apply_post_generation_constraints(
        records_by_table=records_by_table,
        transactions_table=transaction_table,
        accounts_table=accounts_table,
        account_id_key=account_id_key,
        transaction_time_key=time_key,
        amount_key=amount_key,
        balance_key=balance_key,
        inferred_primary_domain_id=inferred_primary_domain_id,
        graph_path_id=chain.chain_id,
    )
    return GeneratedDataset(
        records_by_table=records_by_table,
        pipeline_telemetry=tuple(telemetry),
        scenario_matches=post_adjust_result.scenario_matches,
        post_adjust_warnings=post_adjust_result.warnings,
    )


def _load_segment_config_for_chain(*, profile_id: str) -> tuple[dict[str, Any], list[str], list[float]]:
    profile_payload = load_cardinality_profiles_payload()
    profiles = profile_payload.get("profiles", {})
    profile = profiles.get(profile_id)
    if not isinstance(profile, dict) or str(profile.get("type", "")) != "segmented_three_tier":
        raise ValueError(f"profile {profile_id!r} is not segmented_three_tier")

    segments_cfg = profile.get("segments", {})
    if not isinstance(segments_cfg, dict) or not segments_cfg:
        raise ValueError("segmented_three_tier: missing segments")

    segment_ids: list[str] = []
    weights: list[float] = []
    for segment_id, segment_payload in segments_cfg.items():
        if not isinstance(segment_payload, dict):
            continue
        weight = float(segment_payload.get("weight", 0.0))
        segment_ids.append(str(segment_id))
        weights.append(max(0.0, weight))

    weight_sum = sum(weights) or 1.0
    normalized_weights = [weight / weight_sum for weight in weights]
    return segments_cfg, segment_ids, normalized_weights


def _generate_tier0_rows(
    *,
    table_def: TableDefinition,
    n0: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
    table_name: str,
    pk_column: str,
) -> list[dict[str, object]]:
    rows0 = _generate_table_records(
        table=table_def,
        table_record_count=n0,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )
    primary_key_cache[table_name] = [str(row[pk_column]) for row in rows0]
    return rows0


def _sample_segment_for_each_customer(
    *,
    n0: int,
    rng: random.Random,
    segment_ids: list[str],
    weights: list[float],
) -> list[str]:
    segment_per_customer: list[str] = []
    for _ in range(n0):
        segment_per_customer.append(str(rng.choices(segment_ids, weights=weights, k=1)[0]))
    return segment_per_customer


def _build_customer_index_for_account(
    *,
    n0: int,
    segment_per_customer: list[str],
    segments_cfg: dict[str, Any],
    segment_ids: list[str],
    rng: random.Random,
) -> list[int]:
    accounts_per_customer: list[int] = []
    for customer_index in range(n0):
        segment_id = segment_per_customer[customer_index]
        segment_payload = segments_cfg.get(segment_id)
        if not isinstance(segment_payload, dict):
            segment_payload = segments_cfg.get(segment_ids[0], {})
        acc_spec = segment_payload.get("accounts_per_customer", {})
        accounts_per_customer.append(_sample_int_from_spec(rng=rng, spec=acc_spec))

    customer_index_for_account: list[int] = []
    for customer_index in range(n0):
        for _repeat in range(accounts_per_customer[customer_index]):
            customer_index_for_account.append(customer_index)
    return customer_index_for_account


def _generate_tier1_rows_with_customer_fk(
    *,
    table_def: TableDefinition,
    n1: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
    customer_index_for_account: list[int],
    rows0: list[dict[str, object]],
    fk1: str,
    pk0: str,
    table_name: str,
    pk1: str,
) -> list[dict[str, object]]:
    rows1 = _generate_table_records(
        table=table_def,
        table_record_count=n1,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )
    for account_index, row1 in enumerate(rows1):
        cust_idx = customer_index_for_account[account_index]
        row1[fk1] = rows0[cust_idx][pk0]
    primary_key_cache[table_name] = [str(row[pk1]) for row in rows1]
    return rows1


def _sample_transactions_per_account(
    *,
    n1: int,
    customer_index_for_account: list[int],
    segment_per_customer: list[str],
    segments_cfg: dict[str, Any],
    segment_ids: list[str],
    rng: random.Random,
) -> list[int]:
    transactions_per_account: list[int] = []
    for account_index in range(n1):
        cust_idx = customer_index_for_account[account_index]
        segment_id = segment_per_customer[cust_idx]
        segment_payload = segments_cfg.get(segment_id)
        if not isinstance(segment_payload, dict):
            segment_payload = segments_cfg.get(segment_ids[0], {})
        txn_spec = segment_payload.get("transactions_per_account", {})
        transactions_per_account.append(_sample_int_from_spec(rng=rng, spec=txn_spec))
    return transactions_per_account


def _generate_tier2_rows_with_account_fk(
    *,
    table_def: TableDefinition,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
    transactions_per_account: list[int],
    rows1: list[dict[str, object]],
    fk2: str,
    pk1: str,
    table_name: str,
    pk2: str,
) -> list[dict[str, object]]:
    n2 = sum(transactions_per_account)
    rows2 = _generate_table_records(
        table=table_def,
        table_record_count=n2,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )

    transaction_cursor = 0
    for account_index, txn_n in enumerate(transactions_per_account):
        parent_account_id = rows1[account_index][pk1]
        for _ in range(txn_n):
            if transaction_cursor >= len(rows2):
                break
            rows2[transaction_cursor][fk2] = parent_account_id
            transaction_cursor += 1
    primary_key_cache[table_name] = [str(row[pk2]) for row in rows2]
    return rows2


def _build_records_by_table_with_unmatched(
    *,
    t0: str,
    t1: str,
    t2: str,
    rows0: list[dict[str, object]],
    rows1: list[dict[str, object]],
    rows2: list[dict[str, object]],
    ordered_tables: list[str],
    table_map: dict[str, TableDefinition],
    record_count: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
) -> dict[str, list[dict[str, object]]]:
    records_by_table: dict[str, list[dict[str, object]]] = {
        t0: rows0,
        t1: rows1,
        t2: rows2,
    }
    chain_tables = {t0, t1, t2}
    for table_name in ordered_tables:
        if table_name in chain_tables:
            continue
        records_by_table[table_name] = _generate_unmatched_table(
            table_map=table_map,
            table_name=table_name,
            record_count=record_count,
            rng=rng,
            primary_key_cache=primary_key_cache,
            semantics_profile_id=semantics_profile_id,
            table_to_concept=table_to_concept,
        )
    return records_by_table


def _resolve_post_adjust_columns_for_segmented(
    *,
    chain: ResolvedEventChain,
    table_map: dict[str, TableDefinition],
    c0: str,
    c1: str,
    c2: str,
) -> tuple[str, str, str, str, str, str]:
    transaction_table = chain.table_by_concept[c2]
    accounts_table = chain.table_by_concept[c1]
    account_id_key = chain.fk_child_to_parent_column[(c2, c1)]

    transaction_table_def = table_map[transaction_table]
    account_table_def = table_map[accounts_table]

    amount_key = _first_decimal_column_name(
        table=transaction_table_def,
        skip_columns={
            chain.pk_column_by_concept[c2],
            chain.fk_child_to_parent_column[(c2, c1)],
        },
    )
    balance_key = _first_decimal_column_name(
        table=account_table_def,
        skip_columns={
            chain.pk_column_by_concept[c1],
            chain.fk_child_to_parent_column[(c1, c0)],
        },
    )
    time_key = _first_timestamp_column_name(table=transaction_table_def)
    return transaction_table, accounts_table, account_id_key, time_key, amount_key, balance_key


def _sample_int_from_spec(*, rng: random.Random, spec: Any) -> int:
    if not isinstance(spec, dict):
        return 1
    kind = str(spec.get("type", "uniform_int"))
    if kind == "uniform_int":
        minimum = int(spec.get("min", 1))
        maximum = int(spec.get("max", 1))
        if maximum < minimum:
            minimum, maximum = maximum, minimum
        return rng.randint(minimum, maximum)
    return 1
