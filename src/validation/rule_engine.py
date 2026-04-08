from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from typing import Any

from src.schema.config_store import (
    SYNTH_LIFECYCLE_CONSTRAINTS,
    SYNTH_STATUS_NORMALIZATION,
    load_schema_config,
)
from src.synth.declarative_fsm import (
    load_column_fsm_binding,
    load_state_machine_from_lifecycle_rule,
)
from src.validation.rule_catalog import (
    HARD_TRANSACTION_CURRENCY_ENUM_VALUES,
    SOFT_ACCOUNT_TYPE_ENUM_VALUES,
    SOFT_COUNTRY_ENUM_VALUES,
    SOFT_CUSTOMER_AGE_RANGE_MAX,
    SOFT_CUSTOMER_AGE_RANGE_MIN,
)

ALLOWED_ACCOUNT_TYPES = SOFT_ACCOUNT_TYPE_ENUM_VALUES
ALLOWED_COUNTRY_CODES = SOFT_COUNTRY_ENUM_VALUES
ALLOWED_CURRENCY_CODES = HARD_TRANSACTION_CURRENCY_ENUM_VALUES


@dataclass(frozen=True)
class RuleEvaluationResult:
    """Capture hard/soft rule pass rates and detailed rule violations."""

    violations: list[dict[str, object]]
    hard_rule_pass_rate: float
    soft_rule_pass_rate: float
    hard_rule_failed_count: int
    soft_rule_failed_count: int
    hard_rule_checked_count: int
    soft_rule_checked_count: int
    lifecycle_rule_metrics: tuple[dict[str, object], ...] = ()


def evaluate_rule_violations(
    records_by_table: dict[str, list[dict[str, object]]]
) -> RuleEvaluationResult:
    violations: list[dict[str, object]] = []
    hard_checks_total = 0
    hard_checks_failed = 0
    soft_checks_total = 0
    soft_checks_failed = 0

    account_rows = records_by_table.get("accounts", [])
    for row_index, row in enumerate(account_rows, start=1):
        soft_checks_total += 1
        if str(row.get("account_type", "")) not in ALLOWED_ACCOUNT_TYPES:
            soft_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="soft",
                    rule_id="SOFT_ACCOUNT_TYPE_ENUM",
                    table_name="accounts",
                    row_index=row_index,
                    column_name="account_type",
                    column_value=row.get("account_type", ""),
                    violation_message="account_type is outside allowed enum",
                )
            )

    customer_rows = records_by_table.get("customers", [])
    for row_index, row in enumerate(customer_rows, start=1):
        soft_checks_total += 1
        age_value = int(row.get("age", 0))
        if age_value < SOFT_CUSTOMER_AGE_RANGE_MIN or age_value > SOFT_CUSTOMER_AGE_RANGE_MAX:
            soft_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="soft",
                    rule_id="SOFT_CUSTOMER_AGE_RANGE",
                    table_name="customers",
                    row_index=row_index,
                    column_name="age",
                    column_value=row.get("age", ""),
                    violation_message="customer age should be in [18, 85]",
                )
            )

        soft_checks_total += 1
        if str(row.get("country", "")) not in ALLOWED_COUNTRY_CODES:
            soft_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="soft",
                    rule_id="SOFT_COUNTRY_ENUM",
                    table_name="customers",
                    row_index=row_index,
                    column_name="country",
                    column_value=row.get("country", ""),
                    violation_message="country code is outside allowed enum",
                )
            )

    transaction_rows = records_by_table.get("transactions", [])
    for row_index, row in enumerate(transaction_rows, start=1):
        hard_checks_total += 1
        if _decimal_value_is_negative_or_zero(row.get("amount", "0")):
            hard_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="hard",
                    rule_id="HARD_TRANSACTION_AMOUNT_POSITIVE",
                    table_name="transactions",
                    row_index=row_index,
                    column_name="amount",
                    column_value=row.get("amount", ""),
                    violation_message="transaction amount must be positive",
                )
            )

        hard_checks_total += 1
        if str(row.get("currency", "")) not in ALLOWED_CURRENCY_CODES:
            hard_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="hard",
                    rule_id="HARD_TRANSACTION_CURRENCY_ENUM",
                    table_name="transactions",
                    row_index=row_index,
                    column_name="currency",
                    column_value=row.get("currency", ""),
                    violation_message="transaction currency is outside allowed enum",
                )
            )

    conservation_check = _evaluate_balance_conservation(
        account_rows=account_rows,
        transaction_rows=transaction_rows,
        violations=violations,
    )
    hard_checks_total += conservation_check["hard_checks_total"]
    hard_checks_failed += conservation_check["hard_checks_failed"]

    temporal_check = _evaluate_transaction_temporal_consistency(
        transaction_rows=transaction_rows,
        violations=violations,
    )
    hard_checks_total += temporal_check["hard_checks_total"]
    hard_checks_failed += temporal_check["hard_checks_failed"]

    # Legacy state-machine check is table-level and duplicates lifecycle checks.
    # Keep lifecycle_constraints as the single source of truth to avoid false positives.

    lifecycle_check = _evaluate_lifecycle_constraints(records_by_table=records_by_table, violations=violations)
    hard_checks_total += lifecycle_check["hard_checks_total"]
    hard_checks_failed += lifecycle_check["hard_checks_failed"]

    hard_pass_rate = _calculate_pass_rate(total_checks=hard_checks_total, failed_checks=hard_checks_failed)
    soft_pass_rate = _calculate_pass_rate(total_checks=soft_checks_total, failed_checks=soft_checks_failed)
    return RuleEvaluationResult(
        violations=violations,
        hard_rule_pass_rate=hard_pass_rate,
        soft_rule_pass_rate=soft_pass_rate,
        hard_rule_failed_count=hard_checks_failed,
        soft_rule_failed_count=soft_checks_failed,
        hard_rule_checked_count=hard_checks_total,
        soft_rule_checked_count=soft_checks_total,
        lifecycle_rule_metrics=tuple(lifecycle_check["rule_metrics"]),
    )


def _build_violation(
    severity: str,
    rule_id: str,
    table_name: str,
    row_index: int,
    column_name: str,
    column_value: object,
    violation_message: str,
) -> dict[str, object]:
    return {
        "severity": severity,
        "rule_id": rule_id,
        "table_name": table_name,
        "row_index": row_index,
        "column_name": column_name,
        "column_value": str(column_value),
        "violation_message": violation_message,
    }


def _decimal_value_is_negative(value: object) -> bool:
    try:
        value_decimal = Decimal(str(value))
    except InvalidOperation:
        return True
    return value_decimal < Decimal("0")


def _decimal_value_is_negative_or_zero(value: object) -> bool:
    try:
        value_decimal = Decimal(str(value))
    except InvalidOperation:
        return True
    return value_decimal <= Decimal("0")


def _calculate_pass_rate(total_checks: int, failed_checks: int) -> float:
    if total_checks == 0:
        return 1.0
    return round((total_checks - failed_checks) / total_checks, 6)


def _evaluate_balance_conservation(
    account_rows: list[dict[str, object]],
    transaction_rows: list[dict[str, object]],
    violations: list[dict[str, object]],
) -> dict[str, int]:
    if not account_rows or not transaction_rows:
        return {"hard_checks_total": 0, "hard_checks_failed": 0}

    transaction_sum_by_account: dict[str, Decimal] = {}
    for transaction_row in transaction_rows:
        account_id = str(transaction_row.get("account_id", ""))
        amount_yuan = Decimal(str(transaction_row.get("amount", "0")))
        transaction_sum_by_account[account_id] = transaction_sum_by_account.get(account_id, Decimal("0.00")) + amount_yuan

    hard_checks_total = 0
    hard_checks_failed = 0
    for row_index, account_row in enumerate(account_rows, start=1):
        hard_checks_total += 1
        account_id = str(account_row.get("account_id", ""))
        account_balance_yuan = Decimal(str(account_row.get("balance", "0")))
        expected_balance_yuan = transaction_sum_by_account.get(account_id, Decimal("0.00"))
        if (account_balance_yuan - expected_balance_yuan).copy_abs() <= Decimal("0.01"):
            continue

        hard_checks_failed += 1
        violations.append(
            _build_violation(
                severity="hard",
                rule_id="HARD_BALANCE_CONSERVATION",
                table_name="accounts",
                row_index=row_index,
                column_name="balance",
                column_value=account_row.get("balance", ""),
                violation_message=(
                    "account balance must equal transaction replay sum "
                    f"(expected={expected_balance_yuan})"
                ),
            )
        )
    return {"hard_checks_total": hard_checks_total, "hard_checks_failed": hard_checks_failed}


def _evaluate_transaction_temporal_consistency(
    transaction_rows: list[dict[str, object]],
    violations: list[dict[str, object]],
) -> dict[str, int]:
    if not transaction_rows:
        return {"hard_checks_total": 0, "hard_checks_failed": 0}

    hard_checks_total = 0
    hard_checks_failed = 0
    previous_timestamp_by_account: dict[str, datetime] = {}
    for row_index, transaction_row in enumerate(transaction_rows, start=1):
        account_id = str(transaction_row.get("account_id", ""))
        time_text = str(transaction_row.get("transaction_time", ""))

        hard_checks_total += 1
        parsed_time = _parse_iso_timestamp(time_text=time_text)
        if parsed_time is None:
            hard_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="hard",
                    rule_id="HARD_TRANSACTION_TIME_PARSE",
                    table_name="transactions",
                    row_index=row_index,
                    column_name="transaction_time",
                    column_value=time_text,
                    violation_message="transaction_time must be a valid ISO-8601 timestamp",
                )
            )
            continue

        previous_time = previous_timestamp_by_account.get(account_id)
        hard_checks_total += 1
        if previous_time is not None and parsed_time < previous_time:
            hard_checks_failed += 1
            violations.append(
                _build_violation(
                    severity="hard",
                    rule_id="HARD_TRANSACTION_TIME_ORDER",
                    table_name="transactions",
                    row_index=row_index,
                    column_name="transaction_time",
                    column_value=time_text,
                    violation_message="transaction_time must be non-decreasing within each account",
                )
            )
        previous_timestamp_by_account[account_id] = parsed_time

    return {"hard_checks_total": hard_checks_total, "hard_checks_failed": hard_checks_failed}


def _evaluate_status_state_machine_if_present(
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, int]:
    hard_checks_total = 0
    hard_checks_failed = 0

    for table_name, rows in records_by_table.items():
        if not rows:
            continue
        binding = load_column_fsm_binding(table_name=table_name, column_name="status")
        if binding is None:
            continue
        state_machine = load_state_machine_from_lifecycle_rule(binding.lifecycle_rule_id)
        if state_machine is None:
            continue
        if "status" not in rows[0] or binding.requires_ordered_rows_by not in rows[0]:
            continue
        allowed_transitions = state_machine.allowed_transitions

        rows_sorted = sorted(rows, key=lambda row: str(row.get(binding.requires_ordered_rows_by, "")))
        previous_status: str | None = None
        for row_index, row in enumerate(rows_sorted, start=1):
            current_status = str(row.get("status", "")).lower()
            if previous_status is None:
                previous_status = current_status
                continue
            hard_checks_total += 1
            allowed_next = allowed_transitions.get(previous_status, set())
            if current_status not in allowed_next:
                hard_checks_failed += 1
                violations.append(
                    _build_violation(
                        severity="hard",
                        rule_id="HARD_STATUS_STATE_MACHINE",
                        table_name=table_name,
                        row_index=row_index,
                        column_name="status",
                        column_value=row.get("status", ""),
                        violation_message=(
                            "status transition is invalid "
                            f"({previous_status} -> {current_status})"
                        ),
                    )
                )
            previous_status = current_status

    return {"hard_checks_total": hard_checks_total, "hard_checks_failed": hard_checks_failed}


def _parse_iso_timestamp(time_text: str) -> datetime | None:
    try:
        return datetime.fromisoformat(time_text)
    except ValueError:
        return None


def _evaluate_lifecycle_constraints(
    *,
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    payload = _load_lifecycle_constraints_payload()
    hard_checks_total = 0
    hard_checks_failed = 0
    rule_metrics: list[dict[str, object]] = []

    state_machine_result = _evaluate_state_machine_rules_from_config(
        rules=payload.get("state_machine_rules", []),
        records_by_table=records_by_table,
        violations=violations,
    )
    hard_checks_total += state_machine_result["hard_checks_total"]
    hard_checks_failed += state_machine_result["hard_checks_failed"]
    rule_metrics.extend(state_machine_result["rule_metrics"])

    temporal_result = _evaluate_temporal_order_rules_from_config(
        rules=payload.get("temporal_order_rules", []),
        records_by_table=records_by_table,
        violations=violations,
    )
    hard_checks_total += temporal_result["hard_checks_total"]
    hard_checks_failed += temporal_result["hard_checks_failed"]
    rule_metrics.extend(temporal_result["rule_metrics"])

    cross_table_result = _evaluate_cross_table_temporal_rules_from_config(
        rules=payload.get("cross_table_temporal_rules", []),
        records_by_table=records_by_table,
        violations=violations,
    )
    hard_checks_total += cross_table_result["hard_checks_total"]
    hard_checks_failed += cross_table_result["hard_checks_failed"]
    rule_metrics.extend(cross_table_result["rule_metrics"])
    business_result = _evaluate_business_conservation_rules_from_config(
        rules=payload.get("business_conservation_rules", []),
        records_by_table=records_by_table,
        violations=violations,
    )
    hard_checks_total += business_result["hard_checks_total"]
    hard_checks_failed += business_result["hard_checks_failed"]
    rule_metrics.extend(business_result["rule_metrics"])
    return {
        "hard_checks_total": hard_checks_total,
        "hard_checks_failed": hard_checks_failed,
        "rule_metrics": rule_metrics,
    }


def _evaluate_state_machine_rules_from_config(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    hard_checks_total = 0
    hard_checks_failed = 0
    rule_metrics: list[dict[str, object]] = []
    if not isinstance(rules, list):
        return {"hard_checks_total": 0, "hard_checks_failed": 0, "rule_metrics": []}

    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        rule_id = str(raw_rule.get("rule_id", "HARD_STATUS_STATE_MACHINE"))
        table_name = str(raw_rule.get("table_name", ""))
        status_column = str(raw_rule.get("status_column", "status"))
        time_column = str(raw_rule.get("sequence_time_column", ""))
        entity_keys = [str(item) for item in raw_rule.get("entity_key_columns", []) or [] if str(item)]
        initial_states = {
            _normalize_status_value(table_name=table_name, column_name=status_column, value=str(item))
            for item in raw_rule.get("initial_states", []) or []
            if str(item)
        }
        singleton_allowed_states = {
            _normalize_status_value(table_name=table_name, column_name=status_column, value=str(item))
            for item in raw_rule.get("singleton_allowed_states", []) or []
            if str(item)
        }
        allowed_transitions_raw = raw_rule.get("allowed_transitions", {})
        if not table_name or not time_column or not entity_keys or not isinstance(allowed_transitions_raw, dict):
            continue
        rows = records_by_table.get(table_name, [])
        if not rows:
            continue
        if not _table_has_columns(rows=rows, columns=[status_column, time_column, *entity_keys]):
            continue

        allowed_transitions: dict[str, set[str]] = {}
        for from_state, to_states in allowed_transitions_raw.items():
            if isinstance(to_states, list):
                normalized_from = _normalize_status_value(
                    table_name=table_name,
                    column_name=status_column,
                    value=str(from_state),
                )
                allowed_transitions[normalized_from] = {
                    _normalize_status_value(table_name=table_name, column_name=status_column, value=str(item))
                    for item in to_states
                }

        checks_for_rule = 0
        failed_for_rule = 0
        grouped_rows = _group_rows_by_entity(rows=rows, entity_key_columns=entity_keys)
        for grouped in grouped_rows.values():
            sorted_rows = sorted(grouped, key=lambda item: str(item["row"].get(time_column, "")))
            previous_status: str | None = None
            for item in sorted_rows:
                row_index = item["row_index"]
                row = item["row"]
                time_text = str(row.get(time_column, ""))
                checks_for_rule += 1
                parsed_time = _parse_iso_timestamp(time_text=time_text)
                if parsed_time is None:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=f"{rule_id}_TIME_PARSE",
                            table_name=table_name,
                            row_index=row_index,
                            column_name=time_column,
                            column_value=time_text,
                            violation_message=f"{time_column} must be a valid ISO-8601 timestamp",
                        )
                    )
                    continue
                current_status = _normalize_status_value(
                    table_name=table_name,
                    column_name=status_column,
                    value=str(row.get(status_column, "")),
                )
                if previous_status is None:
                    if len(sorted_rows) == 1 and singleton_allowed_states:
                        checks_for_rule += 1
                        if current_status not in singleton_allowed_states:
                            failed_for_rule += 1
                            violations.append(
                                _build_violation(
                                    severity="hard",
                                    rule_id=f"{rule_id}_SINGLETON_STATE",
                                    table_name=table_name,
                                    row_index=row_index,
                                    column_name=status_column,
                                    column_value=row.get(status_column, ""),
                                    violation_message=(
                                        "single-row entity state must be one of "
                                        f"{sorted(singleton_allowed_states)}"
                                    ),
                                )
                            )
                    elif initial_states:
                        checks_for_rule += 1
                        if current_status not in initial_states:
                            failed_for_rule += 1
                            violations.append(
                                _build_violation(
                                    severity="hard",
                                    rule_id=f"{rule_id}_INITIAL_STATE",
                                    table_name=table_name,
                                    row_index=row_index,
                                    column_name=status_column,
                                    column_value=row.get(status_column, ""),
                                    violation_message=(
                                        f"initial state must be one of {sorted(initial_states)}"
                                    ),
                                )
                            )
                    previous_status = current_status
                    continue
                checks_for_rule += 1
                allowed_next = allowed_transitions.get(previous_status, set())
                if current_status not in allowed_next:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=status_column,
                            column_value=row.get(status_column, ""),
                            violation_message=(
                                "status transition is invalid "
                                f"({previous_status} -> {current_status})"
                            ),
                        )
                    )
                previous_status = current_status

        hard_checks_total += checks_for_rule
        hard_checks_failed += failed_for_rule
        rule_metrics.append(
            {
                "rule_id": rule_id,
                "category": "state_machine",
                "table_name": table_name,
                "checked_count": checks_for_rule,
                "failed_count": failed_for_rule,
                "pass_rate": _calculate_pass_rate(checks_for_rule, failed_for_rule),
            }
        )

    return {
        "hard_checks_total": hard_checks_total,
        "hard_checks_failed": hard_checks_failed,
        "rule_metrics": rule_metrics,
    }


def _evaluate_temporal_order_rules_from_config(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    hard_checks_total = 0
    hard_checks_failed = 0
    rule_metrics: list[dict[str, object]] = []
    if not isinstance(rules, list):
        return {"hard_checks_total": 0, "hard_checks_failed": 0, "rule_metrics": []}

    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        rule_id = str(raw_rule.get("rule_id", "HARD_TEMPORAL_ORDER"))
        table_name = str(raw_rule.get("table_name", ""))
        constraints = raw_rule.get("constraints", [])
        if not table_name or not isinstance(constraints, list):
            continue
        rows = records_by_table.get(table_name, [])
        if not rows:
            continue
        checks_for_rule = 0
        failed_for_rule = 0
        for row_index, row in enumerate(rows, start=1):
            for raw_constraint in constraints:
                if not isinstance(raw_constraint, dict):
                    continue
                if not _temporal_constraint_applies(row=row, constraint=raw_constraint):
                    continue
                left_column = str(raw_constraint.get("left_column", ""))
                right_column = str(raw_constraint.get("right_column", ""))
                operator = str(raw_constraint.get("operator", "<="))
                if left_column not in row or right_column not in row:
                    continue

                left_time_text = str(row.get(left_column, ""))
                right_time_text = str(row.get(right_column, ""))
                left_time = _parse_iso_timestamp(time_text=left_time_text)
                right_time = _parse_iso_timestamp(time_text=right_time_text)
                checks_for_rule += 2
                parse_failed = False
                if left_time is None:
                    failed_for_rule += 1
                    parse_failed = True
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=f"{rule_id}_TIME_PARSE",
                            table_name=table_name,
                            row_index=row_index,
                            column_name=left_column,
                            column_value=left_time_text,
                            violation_message=f"{left_column} must be a valid ISO-8601 timestamp",
                        )
                    )
                if right_time is None:
                    failed_for_rule += 1
                    parse_failed = True
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=f"{rule_id}_TIME_PARSE",
                            table_name=table_name,
                            row_index=row_index,
                            column_name=right_column,
                            column_value=right_time_text,
                            violation_message=f"{right_column} must be a valid ISO-8601 timestamp",
                        )
                    )
                if parse_failed:
                    continue
                checks_for_rule += 1
                if operator == "<=" and left_time > right_time:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=right_column,
                            column_value=right_time_text,
                            violation_message=f"{left_column} must be <= {right_column}",
                        )
                    )
                if operator == "<" and left_time >= right_time:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=right_column,
                            column_value=right_time_text,
                            violation_message=f"{left_column} must be < {right_column}",
                        )
                    )
        hard_checks_total += checks_for_rule
        hard_checks_failed += failed_for_rule
        rule_metrics.append(
            {
                "rule_id": rule_id,
                "category": "temporal_order",
                "table_name": table_name,
                "checked_count": checks_for_rule,
                "failed_count": failed_for_rule,
                "pass_rate": _calculate_pass_rate(checks_for_rule, failed_for_rule),
            }
        )

    return {
        "hard_checks_total": hard_checks_total,
        "hard_checks_failed": hard_checks_failed,
        "rule_metrics": rule_metrics,
    }


def _temporal_constraint_applies(*, row: dict[str, object], constraint: dict[str, object]) -> bool:
    apply_when_column = str(constraint.get("apply_when_column", "")).strip()
    apply_when_in = constraint.get("apply_when_in", [])
    if not apply_when_column:
        return True
    if apply_when_column not in row:
        return False
    if not isinstance(apply_when_in, list) or not apply_when_in:
        return False
    current_value = str(row.get(apply_when_column, "")).strip().lower()
    expected_values = {
        str(item).strip().lower() for item in apply_when_in if str(item).strip()
    }
    if not expected_values:
        return False
    return current_value in expected_values


def _evaluate_cross_table_temporal_rules_from_config(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    hard_checks_total = 0
    hard_checks_failed = 0
    rule_metrics: list[dict[str, object]] = []
    if not isinstance(rules, list):
        return {"hard_checks_total": 0, "hard_checks_failed": 0, "rule_metrics": []}

    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        rule_id = str(raw_rule.get("rule_id", "HARD_CROSS_TABLE_TEMPORAL"))
        left_table = str(raw_rule.get("left_table_name", ""))
        right_table = str(raw_rule.get("right_table_name", ""))
        left_keys = [str(item) for item in raw_rule.get("left_key_columns", []) or [] if str(item)]
        right_keys = [str(item) for item in raw_rule.get("right_foreign_key_columns", []) or [] if str(item)]
        left_time_column = str(raw_rule.get("left_time_column", ""))
        right_time_column = str(raw_rule.get("right_time_column", ""))
        operator = str(raw_rule.get("operator", "<="))
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
        if not _table_has_columns(rows=left_rows, columns=[left_time_column, *left_keys]):
            continue
        if not _table_has_columns(rows=right_rows, columns=[right_time_column, *right_keys]):
            continue

        left_time_by_key: dict[tuple[str, ...], datetime] = {}
        for left_row in left_rows:
            key = tuple(str(left_row.get(col, "")) for col in left_keys)
            parsed = _parse_iso_timestamp(time_text=str(left_row.get(left_time_column, "")))
            if parsed is None:
                continue
            existing = left_time_by_key.get(key)
            if existing is None or parsed < existing:
                left_time_by_key[key] = parsed

        checks_for_rule = 0
        failed_for_rule = 0
        for row_index, right_row in enumerate(right_rows, start=1):
            key = tuple(str(right_row.get(col, "")) for col in right_keys)
            left_time = left_time_by_key.get(key)
            if left_time is None:
                continue
            right_time_text = str(right_row.get(right_time_column, ""))
            checks_for_rule += 1
            right_time = _parse_iso_timestamp(time_text=right_time_text)
            if right_time is None:
                failed_for_rule += 1
                violations.append(
                    _build_violation(
                        severity="hard",
                        rule_id=f"{rule_id}_TIME_PARSE",
                        table_name=right_table,
                        row_index=row_index,
                        column_name=right_time_column,
                        column_value=right_time_text,
                        violation_message=f"{right_time_column} must be a valid ISO-8601 timestamp",
                    )
                )
                continue
            checks_for_rule += 1
            if operator == "<=" and left_time > right_time:
                failed_for_rule += 1
                violations.append(
                    _build_violation(
                        severity="hard",
                        rule_id=rule_id,
                        table_name=right_table,
                        row_index=row_index,
                        column_name=right_time_column,
                        column_value=right_time_text,
                        violation_message=(
                            f"{left_table}.{left_time_column} must be <= "
                            f"{right_table}.{right_time_column}"
                        ),
                    )
                )
            if operator == "<" and left_time >= right_time:
                failed_for_rule += 1
                violations.append(
                    _build_violation(
                        severity="hard",
                        rule_id=rule_id,
                        table_name=right_table,
                        row_index=row_index,
                        column_name=right_time_column,
                        column_value=right_time_text,
                        violation_message=(
                            f"{left_table}.{left_time_column} must be < "
                            f"{right_table}.{right_time_column}"
                        ),
                    )
                )

        hard_checks_total += checks_for_rule
        hard_checks_failed += failed_for_rule
        rule_metrics.append(
            {
                "rule_id": rule_id,
                "category": "cross_table_temporal",
                "table_name": f"{left_table}->{right_table}",
                "checked_count": checks_for_rule,
                "failed_count": failed_for_rule,
                "pass_rate": _calculate_pass_rate(checks_for_rule, failed_for_rule),
            }
        )
    return {
        "hard_checks_total": hard_checks_total,
        "hard_checks_failed": hard_checks_failed,
        "rule_metrics": rule_metrics,
    }


def _load_lifecycle_constraints_payload() -> dict[str, object]:
    payload = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    return payload if isinstance(payload, dict) else {}


def _table_has_columns(*, rows: list[dict[str, object]], columns: list[str]) -> bool:
    if not rows:
        return False
    first_row = rows[0]
    return all(column in first_row for column in columns)


def _group_rows_by_entity(
    *,
    rows: list[dict[str, object]],
    entity_key_columns: list[str],
) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row_index, row in enumerate(rows, start=1):
        entity_key = tuple(str(row.get(column, "")) for column in entity_key_columns)
        grouped.setdefault(entity_key, []).append({"row_index": row_index, "row": row})
    return grouped


def _normalize_status_value(*, table_name: str, column_name: str, value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        return normalized
    for rule in _load_status_normalization_rules():
        if rule["table_name"] != table_name or rule["column_name"] != column_name:
            continue
        alias_to_canonical = rule["alias_to_canonical"]
        mapped = alias_to_canonical.get(normalized)
        if mapped:
            return mapped
    return normalized


def _load_status_normalization_rules() -> tuple[dict[str, object], ...]:
    payload = load_schema_config(SYNTH_STATUS_NORMALIZATION)
    if not isinstance(payload, dict):
        return ()
    rules = payload.get("rules", [])
    if not isinstance(rules, list):
        return ()
    normalized_rules: list[dict[str, object]] = []
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
        normalized_rules.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "alias_to_canonical": alias_to_canonical,
            }
        )
    return tuple(normalized_rules)


def _evaluate_business_conservation_rules_from_config(
    *,
    rules: object,
    records_by_table: dict[str, list[dict[str, object]]],
    violations: list[dict[str, object]],
) -> dict[str, object]:
    hard_checks_total = 0
    hard_checks_failed = 0
    rule_metrics: list[dict[str, object]] = []
    if not isinstance(rules, list):
        return {"hard_checks_total": 0, "hard_checks_failed": 0, "rule_metrics": []}

    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        rule_id = str(raw_rule.get("rule_id", "HARD_BUSINESS_CONSERVATION"))
        rule_type = str(raw_rule.get("type", ""))
        checks_for_rule = 0
        failed_for_rule = 0
        table_scope = ""

        if rule_type == "intra_row_numeric_compare":
            table_name = str(raw_rule.get("table_name", ""))
            left_column = str(raw_rule.get("left_column", ""))
            right_column = str(raw_rule.get("right_column", ""))
            operator = str(raw_rule.get("operator", "<="))
            table_scope = table_name
            rows = records_by_table.get(table_name, [])
            for row_index, row in enumerate(rows, start=1):
                if left_column not in row or right_column not in row:
                    continue
                checks_for_rule += 1
                left_value = _parse_decimal(str(row.get(left_column, "")))
                right_value = _parse_decimal(str(row.get(right_column, "")))
                if left_value is None or right_value is None:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=f"{rule_id}_PARSE",
                            table_name=table_name,
                            row_index=row_index,
                            column_name=left_column if left_value is None else right_column,
                            column_value=row.get(left_column if left_value is None else right_column, ""),
                            violation_message="numeric parse failed in business conservation check",
                        )
                    )
                    continue
                if not _compare_decimal_values(left=left_value, right=right_value, operator=operator):
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=left_column,
                            column_value=row.get(left_column, ""),
                            violation_message=f"{left_column} must be {operator} {right_column}",
                        )
                    )

        elif rule_type == "intra_row_numeric_range":
            table_name = str(raw_rule.get("table_name", ""))
            column_name = str(raw_rule.get("column_name", ""))
            table_scope = table_name
            min_raw = raw_rule.get("min_value")
            max_raw = raw_rule.get("max_value")
            minimum = _parse_decimal(str(min_raw)) if min_raw is not None else None
            maximum = _parse_decimal(str(max_raw)) if max_raw is not None else None
            rows = records_by_table.get(table_name, [])
            for row_index, row in enumerate(rows, start=1):
                if column_name not in row:
                    continue
                checks_for_rule += 1
                value = _parse_decimal(str(row.get(column_name, "")))
                if value is None:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=f"{rule_id}_PARSE",
                            table_name=table_name,
                            row_index=row_index,
                            column_name=column_name,
                            column_value=row.get(column_name, ""),
                            violation_message=f"{column_name} must be numeric",
                        )
                    )
                    continue
                if minimum is not None and value < minimum:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=column_name,
                            column_value=row.get(column_name, ""),
                            violation_message=f"{column_name} must be >= {minimum}",
                        )
                    )
                if maximum is not None and value > maximum:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=column_name,
                            column_value=row.get(column_name, ""),
                            violation_message=f"{column_name} must be <= {maximum}",
                        )
                    )

        elif rule_type == "state_requires_non_null_time":
            table_name = str(raw_rule.get("table_name", ""))
            state_column = str(raw_rule.get("state_column", ""))
            time_column = str(raw_rule.get("time_column", ""))
            required_states = {str(item).strip().lower() for item in raw_rule.get("required_states", []) or []}
            table_scope = table_name
            rows = records_by_table.get(table_name, [])
            for row_index, row in enumerate(rows, start=1):
                if state_column not in row:
                    continue
                state = str(row.get(state_column, "")).strip().lower()
                if state not in required_states:
                    continue
                checks_for_rule += 1
                time_value = str(row.get(time_column, "")).strip()
                if not time_value:
                    failed_for_rule += 1
                    violations.append(
                        _build_violation(
                            severity="hard",
                            rule_id=rule_id,
                            table_name=table_name,
                            row_index=row_index,
                            column_name=time_column,
                            column_value=row.get(time_column, ""),
                            violation_message=f"{time_column} must be non-null for {state_column}={state}",
                        )
                    )

        elif rule_type == "aggregate_child_amount_le_parent_limit":
            parent_table = str(raw_rule.get("parent_table_name", ""))
            parent_key = str(raw_rule.get("parent_key_column", ""))
            parent_limit_col = str(raw_rule.get("parent_limit_column", ""))
            child_table = str(raw_rule.get("child_table_name", ""))
            child_fk = str(raw_rule.get("child_fk_column", ""))
            child_amount_col = str(raw_rule.get("child_amount_column", ""))
            table_scope = f"{parent_table}->{child_table}"
            parent_rows = records_by_table.get(parent_table, [])
            child_rows = records_by_table.get(child_table, [])
            if parent_rows and child_rows:
                sum_by_parent: dict[str, Decimal] = {}
                for child_row in child_rows:
                    parent_id = str(child_row.get(child_fk, ""))
                    amount_value = _parse_decimal(str(child_row.get(child_amount_col, "")))
                    if not parent_id or amount_value is None:
                        continue
                    sum_by_parent[parent_id] = sum_by_parent.get(parent_id, Decimal("0")) + amount_value
                for row_index, parent_row in enumerate(parent_rows, start=1):
                    parent_id = str(parent_row.get(parent_key, ""))
                    limit_value = _parse_decimal(str(parent_row.get(parent_limit_col, "")))
                    if not parent_id or limit_value is None:
                        continue
                    checks_for_rule += 1
                    total_amount = sum_by_parent.get(parent_id, Decimal("0"))
                    if total_amount > limit_value:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=parent_table,
                                row_index=row_index,
                                column_name=parent_limit_col,
                                column_value=parent_row.get(parent_limit_col, ""),
                                violation_message=(
                                    f"aggregated {child_table}.{child_amount_col} exceeds "
                                    f"{parent_table}.{parent_limit_col} (sum={total_amount})"
                                ),
                            )
                        )

        elif rule_type == "current_state_matches_parent_status":
            parent_table = str(raw_rule.get("parent_table_name", ""))
            parent_key_column = str(raw_rule.get("parent_key_column", ""))
            parent_status_column = str(raw_rule.get("parent_status_column", ""))
            history_table = str(raw_rule.get("history_table_name", ""))
            history_fk_column = str(raw_rule.get("history_fk_column", ""))
            history_status_column = str(raw_rule.get("history_status_column", ""))
            current_flag_column = str(raw_rule.get("history_current_flag_column", ""))
            current_flag_value = str(raw_rule.get("history_current_flag_value", "Y"))
            table_scope = f"{parent_table}<->{history_table}"

            parent_rows = records_by_table.get(parent_table, [])
            history_rows = records_by_table.get(history_table, [])
            if parent_rows and history_rows:
                parent_status_by_id: dict[str, str] = {}
                for parent_row in parent_rows:
                    parent_id = str(parent_row.get(parent_key_column, "")).strip()
                    if not parent_id:
                        continue
                    parent_status_by_id[parent_id] = _normalize_status_value(
                        table_name=parent_table,
                        column_name=parent_status_column,
                        value=str(parent_row.get(parent_status_column, "")),
                    )

                current_rows_by_parent: dict[str, list[tuple[int, dict[str, object]]]] = {}
                for row_index, history_row in enumerate(history_rows, start=1):
                    parent_id = str(history_row.get(history_fk_column, "")).strip()
                    if not parent_id:
                        continue
                    if str(history_row.get(current_flag_column, "")).strip() != current_flag_value:
                        continue
                    current_rows_by_parent.setdefault(parent_id, []).append((row_index, history_row))

                for parent_row_index, parent_row in enumerate(parent_rows, start=1):
                    parent_id = str(parent_row.get(parent_key_column, "")).strip()
                    if not parent_id:
                        continue
                    current_rows = current_rows_by_parent.get(parent_id, [])
                    checks_for_rule += 1
                    if len(current_rows) != 1:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=history_table,
                                row_index=current_rows[0][0] if current_rows else 0,
                                column_name=current_flag_column,
                                column_value=current_flag_value,
                                violation_message=(
                                    f"account_id={parent_id} must have exactly one current history row "
                                    f"({current_flag_column}={current_flag_value})"
                                ),
                            )
                        )
                        continue

                    checks_for_rule += 1
                    current_row_index, current_row = current_rows[0]
                    current_status = _normalize_status_value(
                        table_name=history_table,
                        column_name=history_status_column,
                        value=str(current_row.get(history_status_column, "")),
                    )
                    parent_status = parent_status_by_id.get(parent_id, "")
                    if current_status != parent_status:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=history_table,
                                row_index=current_row_index,
                                column_name=history_status_column,
                                column_value=current_row.get(history_status_column, ""),
                                violation_message=(
                                    f"current history status must match parent status "
                                    f"(account_id={parent_id}, parent={parent_status}, current={current_status})"
                                ),
                            )
                        )

        elif rule_type == "current_flag_must_be_latest_time":
            table_name = str(raw_rule.get("table_name", ""))
            entity_key_column = str(raw_rule.get("entity_key_column", ""))
            time_column = str(raw_rule.get("time_column", ""))
            current_flag_column = str(raw_rule.get("current_flag_column", ""))
            current_flag_value = str(raw_rule.get("current_flag_value", "Y"))
            table_scope = table_name
            rows = records_by_table.get(table_name, [])
            if rows:
                grouped: dict[str, list[tuple[int, dict[str, object]]]] = {}
                for row_index, row in enumerate(rows, start=1):
                    entity_id = str(row.get(entity_key_column, "")).strip()
                    if entity_id:
                        grouped.setdefault(entity_id, []).append((row_index, row))

                for entity_id, entity_rows in grouped.items():
                    current_rows = [
                        (row_index, row)
                        for row_index, row in entity_rows
                        if str(row.get(current_flag_column, "")).strip() == current_flag_value
                    ]
                    checks_for_rule += 1
                    if len(current_rows) != 1:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=table_name,
                                row_index=current_rows[0][0] if current_rows else 0,
                                column_name=current_flag_column,
                                column_value=current_flag_value,
                                violation_message=(
                                    f"{entity_key_column}={entity_id} must have exactly one row with "
                                    f"{current_flag_column}={current_flag_value}"
                                ),
                            )
                        )
                        continue

                    latest_time: datetime | None = None
                    latest_row_index = 0
                    latest_row: dict[str, object] | None = None
                    for row_index, row in entity_rows:
                        parsed = _parse_iso_timestamp(str(row.get(time_column, "")))
                        checks_for_rule += 1
                        if parsed is None:
                            failed_for_rule += 1
                            violations.append(
                                _build_violation(
                                    severity="hard",
                                    rule_id=f"{rule_id}_TIME_PARSE",
                                    table_name=table_name,
                                    row_index=row_index,
                                    column_name=time_column,
                                    column_value=row.get(time_column, ""),
                                    violation_message=f"{time_column} must be a valid ISO-8601 timestamp",
                                )
                            )
                            continue
                        if latest_time is None or parsed > latest_time:
                            latest_time = parsed
                            latest_row_index = row_index
                            latest_row = row

                    if latest_row is None:
                        continue
                    checks_for_rule += 1
                    current_row_index, current_row = current_rows[0]
                    if current_row_index != latest_row_index:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=table_name,
                                row_index=current_row_index,
                                column_name=current_flag_column,
                                column_value=current_row.get(current_flag_column, ""),
                                violation_message=(
                                    f"{current_flag_column}={current_flag_value} must be on latest {time_column} "
                                    f"row for {entity_key_column}={entity_id}"
                                ),
                            )
                        )

        elif rule_type == "child_state_requires_parent_json_value_in":
            parent_table = str(raw_rule.get("parent_table_name", ""))
            parent_key_column = str(raw_rule.get("parent_key_column", ""))
            parent_json_column = str(raw_rule.get("parent_json_column", ""))
            parent_json_path = str(raw_rule.get("parent_json_path", ""))
            child_table = str(raw_rule.get("child_table_name", ""))
            child_fk_column = str(raw_rule.get("child_fk_column", ""))
            child_state_column = str(raw_rule.get("child_state_column", ""))
            restricted_child_states = {
                str(item).strip().lower() for item in raw_rule.get("restricted_child_states", []) or []
            }
            allowed_parent_values = {
                str(item).strip().lower() for item in raw_rule.get("allowed_parent_values", []) or []
            }
            table_scope = f"{parent_table}->{child_table}"
            parent_rows = records_by_table.get(parent_table, [])
            child_rows = records_by_table.get(child_table, [])
            if parent_rows and child_rows:
                parent_value_by_id: dict[str, str] = {}
                for parent_row in parent_rows:
                    parent_id = str(parent_row.get(parent_key_column, "")).strip()
                    if not parent_id:
                        continue
                    parsed_value = _extract_json_path_value(
                        raw_json=parent_row.get(parent_json_column),
                        dot_path=parent_json_path,
                    )
                    normalized_value = str(parsed_value).strip().lower() if parsed_value is not None else ""
                    parent_value_by_id[parent_id] = normalized_value

                for row_index, child_row in enumerate(child_rows, start=1):
                    child_state = str(child_row.get(child_state_column, "")).strip().lower()
                    if child_state not in restricted_child_states:
                        continue
                    checks_for_rule += 1
                    parent_id = str(child_row.get(child_fk_column, "")).strip()
                    parent_value = parent_value_by_id.get(parent_id, "")
                    if parent_value not in allowed_parent_values:
                        failed_for_rule += 1
                        violations.append(
                            _build_violation(
                                severity="hard",
                                rule_id=rule_id,
                                table_name=child_table,
                                row_index=row_index,
                                column_name=child_state_column,
                                column_value=child_row.get(child_state_column, ""),
                                violation_message=(
                                    f"{child_state_column}={child_state} requires "
                                    f"{parent_table}.{parent_json_column}.{parent_json_path} in "
                                    f"{sorted(allowed_parent_values)}"
                                ),
                            )
                        )


        hard_checks_total += checks_for_rule
        hard_checks_failed += failed_for_rule
        rule_metrics.append(
            {
                "rule_id": rule_id,
                "category": "business_conservation",
                "table_name": table_scope,
                "checked_count": checks_for_rule,
                "failed_count": failed_for_rule,
                "pass_rate": _calculate_pass_rate(checks_for_rule, failed_for_rule),
            }
        )

    return {
        "hard_checks_total": hard_checks_total,
        "hard_checks_failed": hard_checks_failed,
        "rule_metrics": rule_metrics,
    }


def _parse_decimal(text: str) -> Decimal | None:
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _compare_decimal_values(*, left: Decimal, right: Decimal, operator: str) -> bool:
    if operator == "<=":
        return left <= right
    if operator == "<":
        return left < right
    if operator == ">=":
        return left >= right
    if operator == ">":
        return left > right
    return False


def _extract_json_path_value(*, raw_json: object, dot_path: str) -> object | None:
    path_parts = [part.strip() for part in dot_path.split(".") if part.strip()]
    if not path_parts:
        return None
    try:
        payload = json.loads(str(raw_json))
    except Exception:
        return None
    current: object = payload
    for part in path_parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
