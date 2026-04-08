from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import copy
import random


@dataclass(frozen=True)
class InjectionResult:
    """Return mutated records and detailed anomaly injection logs."""

    records_by_table: dict[str, list[dict[str, object]]]
    injection_logs: list[dict[str, object]]


def inject_bad_cases(
    records_by_table: dict[str, list[dict[str, object]]],
    seed: int,
    anomaly_ratio: float,
) -> InjectionResult:
    rng = random.Random(seed + 17)
    mutated_records = copy.deepcopy(records_by_table)
    injection_logs: list[dict[str, object]] = []

    _inject_transaction_anomalies(
        records_by_table=mutated_records,
        anomaly_ratio=anomaly_ratio,
        rng=rng,
        injection_logs=injection_logs,
    )
    _inject_account_anomalies(
        records_by_table=mutated_records,
        anomaly_ratio=anomaly_ratio,
        rng=rng,
        injection_logs=injection_logs,
    )
    return InjectionResult(records_by_table=mutated_records, injection_logs=injection_logs)


def _inject_transaction_anomalies(
    records_by_table: dict[str, list[dict[str, object]]],
    anomaly_ratio: float,
    rng: random.Random,
    injection_logs: list[dict[str, object]],
) -> None:
    transaction_rows = records_by_table.get("transactions", [])
    if not transaction_rows:
        return

    target_count = max(1, int(len(transaction_rows) * anomaly_ratio))
    selected_indexes = rng.sample(range(len(transaction_rows)), k=min(target_count, len(transaction_rows)))
    for row_position in selected_indexes:
        row = transaction_rows[row_position]

        original_currency = row.get("currency", "")
        row["currency"] = "ZZZ"
        injection_logs.append(
            {
                "table_name": "transactions",
                "row_index": row_position + 1,
                "column_name": "currency",
                "old_value": original_currency,
                "new_value": "ZZZ",
                "injection_type": "invalid_currency_code",
            }
        )

        original_amount = str(row.get("amount", "0"))
        positive_amount_yuan = Decimal(original_amount)
        row["amount"] = str(-positive_amount_yuan)
        injection_logs.append(
            {
                "table_name": "transactions",
                "row_index": row_position + 1,
                "column_name": "amount",
                "old_value": original_amount,
                "new_value": row["amount"],
                "injection_type": "negative_transaction_amount",
            }
        )


def _inject_account_anomalies(
    records_by_table: dict[str, list[dict[str, object]]],
    anomaly_ratio: float,
    rng: random.Random,
    injection_logs: list[dict[str, object]],
) -> None:
    account_rows = records_by_table.get("accounts", [])
    if not account_rows:
        return

    target_count = max(1, int(len(account_rows) * anomaly_ratio))
    selected_indexes = rng.sample(range(len(account_rows)), k=min(target_count, len(account_rows)))
    for row_position in selected_indexes:
        row = account_rows[row_position]

        original_account_type = row.get("account_type", "")
        row["account_type"] = "invalid_type"
        injection_logs.append(
            {
                "table_name": "accounts",
                "row_index": row_position + 1,
                "column_name": "account_type",
                "old_value": original_account_type,
                "new_value": "invalid_type",
                "injection_type": "invalid_account_type",
            }
        )
