from __future__ import annotations

import random

from src.schema.models import SchemaDefinition, TableDefinition
from src.synth.cardinality_eval import (
    decide_per_concept_count,
    get_cardinality_profile_type,
    load_cardinality_profiles_payload,
)
from src.synth.generation_chain_resolver import ResolvedEventChain
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
    _enforce_multi_fk_parent_coherence,
    apply_post_generation_constraints,
)


def generate_event_first_linear_three(
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
    Linear N-concept chain for event-first generation (N>=3 recommended).

    Row counts come from ``synth/topology/cardinality_profiles.json`` (``chain.cardinality_profile_id``).
    Pipeline phases: sample_roots → expand_children → post_adjust (same stage names as rowwise).
    """
    telemetry: list[dict[str, object]] = []

    concept_path = chain.concept_path
    if len(concept_path) < 2:
        raise ValueError("event_first expects at least two concepts in concept_path")
    if (
        get_cardinality_profile_type(profile_id=chain.cardinality_profile_id) == "segmented_three_tier"
        and len(concept_path) == 3
    ):
        from src.synth.event_first_segmented import generate_event_first_segmented_three

        return generate_event_first_segmented_three(
            schema=schema,
            ordered_tables=ordered_tables,
            record_count=record_count,
            seed=seed,
            chain=chain,
            semantics_profile_id=semantics_profile_id,
            table_to_concept=table_to_concept,
            inferred_primary_domain_id=inferred_primary_domain_id,
        )

    rng = random.Random(seed)
    table_map = schema.table_map()
    chain_tables = [chain.table_by_concept[concept_id] for concept_id in concept_path]
    per_concept_counts = _build_per_concept_counts(
        concept_path=concept_path,
        profile_id=chain.cardinality_profile_id,
        record_count=record_count,
    )
    primary_key_cache: dict[str, list[str]] = {}

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_SAMPLE_ROOTS,
            pipeline_mode="event_first",
            tables=chain_tables[0],
            detail="tier0_root_concept_or_chain_head",
        )
    )

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_EXPAND_CHILDREN,
            pipeline_mode="event_first",
            tables=",".join(chain_tables[1:]),
            detail="tier1_plus_chain_tables",
        )
    )

    chain_records_by_concept = _generate_chain_records(
        chain=chain,
        concept_path=concept_path,
        table_map=table_map,
        per_concept_counts=per_concept_counts,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )
    records_by_table = _build_records_by_table_with_unmatched(
        chain=chain,
        concept_path=concept_path,
        chain_tables=chain_tables,
        chain_records_by_concept=chain_records_by_concept,
        ordered_tables=ordered_tables,
        table_map=table_map,
        record_count=record_count,
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )

    _enforce_multi_fk_parent_coherence(schema=schema, records_by_table=records_by_table)

    transaction_table, accounts_table, account_id_key, time_key, amount_key, balance_key = (
        _resolve_post_adjust_columns(
            chain=chain,
            table_map=table_map,
        )
    )

    telemetry.append(
        stage_record(
            stage=PIPELINE_STAGE_POST_ADJUST,
            pipeline_mode="event_first",
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


def _build_per_concept_counts(
    *,
    concept_path: list[str],
    profile_id: str,
    record_count: int,
) -> dict[str, int]:
    cardinality_payload = load_cardinality_profiles_payload()
    per_concept_counts: dict[str, int] = {}
    for concept_id in concept_path:
        per_concept_counts[concept_id] = decide_per_concept_count(
            profile_payload=cardinality_payload,
            profile_id=profile_id,
            concept_id=concept_id,
            record_count=record_count,
        )
    return per_concept_counts


def _generate_chain_records(
    *,
    chain: ResolvedEventChain,
    concept_path: list[str],
    table_map: dict[str, TableDefinition],
    per_concept_counts: dict[str, int],
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
) -> dict[str, list[dict[str, object]]]:
    chain_records_by_concept: dict[str, list[dict[str, object]]] = {}

    first_concept = concept_path[0]
    first_table = chain.table_by_concept[first_concept]
    first_table_def = table_map[first_table]
    rows_head = _generate_table_records(
        table=first_table_def,
        table_record_count=per_concept_counts[first_concept],
        rng=rng,
        primary_key_cache=primary_key_cache,
        semantics_profile_id=semantics_profile_id,
        table_to_concept=table_to_concept,
    )
    head_pk = chain.pk_column_by_concept[first_concept]
    primary_key_cache[first_table] = [str(row[head_pk]) for row in rows_head]
    chain_records_by_concept[first_concept] = rows_head

    for child_concept, parent_concept in zip(concept_path[1:], concept_path[:-1]):
        child_table = chain.table_by_concept[child_concept]
        child_table_def = table_map[child_table]
        child_rows = _generate_table_records(
            table=child_table_def,
            table_record_count=per_concept_counts[child_concept],
            rng=rng,
            primary_key_cache=primary_key_cache,
            semantics_profile_id=semantics_profile_id,
            table_to_concept=table_to_concept,
        )
        parent_rows = chain_records_by_concept[parent_concept]
        parent_pk = chain.pk_column_by_concept[parent_concept]
        child_fk = chain.fk_child_to_parent_column[(child_concept, parent_concept)]
        if parent_rows:
            for child_index, child_row in enumerate(child_rows):
                parent_idx = _parent_index_for_middle_tier(
                    child_index=child_index,
                    n_parents=len(parent_rows),
                    n_children=len(child_rows),
                )
                child_row[child_fk] = parent_rows[parent_idx][parent_pk]
        child_pk = chain.pk_column_by_concept[child_concept]
        primary_key_cache[child_table] = [str(row[child_pk]) for row in child_rows]
        chain_records_by_concept[child_concept] = child_rows
    return chain_records_by_concept


def _build_records_by_table_with_unmatched(
    *,
    chain: ResolvedEventChain,
    concept_path: list[str],
    chain_tables: list[str],
    chain_records_by_concept: dict[str, list[dict[str, object]]],
    ordered_tables: list[str],
    table_map: dict[str, TableDefinition],
    record_count: int,
    rng: random.Random,
    primary_key_cache: dict[str, list[str]],
    semantics_profile_id: str,
    table_to_concept: dict[str, str],
) -> dict[str, list[dict[str, object]]]:
    records_by_table: dict[str, list[dict[str, object]]] = {}
    for concept_id in concept_path:
        records_by_table[chain.table_by_concept[concept_id]] = chain_records_by_concept[concept_id]

    chain_tables_set = set(chain_tables)
    for table_name in ordered_tables:
        if table_name in chain_tables_set:
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


def _resolve_post_adjust_columns(
    *,
    chain: ResolvedEventChain,
    table_map: dict[str, TableDefinition],
) -> tuple[str, str, str, str, str, str]:
    transaction_table = chain.table_by_concept.get("transaction", "transactions")
    accounts_table = chain.table_by_concept.get("account", "accounts")

    account_id_key = "account_id"
    if "transaction" in chain.concept_path and "account" in chain.concept_path:
        account_id_key = chain.fk_child_to_parent_column.get(("transaction", "account"), "account_id")

    amount_key = "amount"
    balance_key = "balance"
    time_key = "transaction_time"

    transaction_table_def = table_map.get(transaction_table)
    if transaction_table_def is not None:
        amount_key = _first_decimal_column_name(
            table=transaction_table_def,
            skip_columns={chain.pk_column_by_concept.get("transaction", "")},
        )
        time_key = _first_timestamp_column_name(table=transaction_table_def)

    account_table_def = table_map.get(accounts_table)
    if account_table_def is not None:
        balance_key = _first_decimal_column_name(
            table=account_table_def,
            skip_columns={chain.pk_column_by_concept.get("account", "")},
        )

    return transaction_table, accounts_table, account_id_key, time_key, amount_key, balance_key


def _parent_index_for_middle_tier(*, child_index: int, n_parents: int, n_children: int) -> int:
    if n_parents <= 0 or n_children <= 0:
        return 0
    if n_children % n_parents == 0:
        group_size = n_children // n_parents
        return min(child_index // group_size, n_parents - 1)
    return min(child_index * n_parents // n_children, n_parents - 1)


def _first_decimal_column_name(
    *,
    table: TableDefinition,
    skip_columns: set[str],
) -> str:
    for column in table.columns:
        if column.name in skip_columns:
            continue
        if column.normalized_type == "decimal" and not column.is_primary_key:
            return column.name
    return "amount"


def _first_timestamp_column_name(*, table: TableDefinition) -> str:
    for column in table.columns:
        if column.normalized_type == "timestamp":
            return column.name
    return "transaction_time"
