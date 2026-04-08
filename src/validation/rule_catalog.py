"""
Canonical rule identifiers and allowed domains shared by:
- validation/rule_engine.py (post-generation checks)
- synth/column_semantics_sampler.py (generation aligned to the same rule_id)

Single source of truth avoids SOFT/HARD enum drift between generate and validate.
"""

from __future__ import annotations

from typing import FrozenSet

# --- Enum domains keyed by rule_id (must stay aligned with rule_engine checks) ---

SOFT_ACCOUNT_TYPE_ENUM_VALUES: FrozenSet[str] = frozenset(
    {"savings", "checking", "margin", "investment", "other"}
)
SOFT_CUSTOMER_AGE_RANGE_MIN = 18
SOFT_CUSTOMER_AGE_RANGE_MAX = 85
SOFT_COUNTRY_ENUM_VALUES: FrozenSet[str] = frozenset({"CN", "US", "SG", "HK", "GB"})
HARD_TRANSACTION_CURRENCY_ENUM_VALUES: FrozenSet[str] = frozenset({"CNY", "USD", "EUR", "SGD", "HKD"})

RULE_ID_TO_ENUM_VALUES: dict[str, FrozenSet[str]] = {
    "SOFT_ACCOUNT_TYPE_ENUM": SOFT_ACCOUNT_TYPE_ENUM_VALUES,
    "SOFT_COUNTRY_ENUM": SOFT_COUNTRY_ENUM_VALUES,
    "HARD_TRANSACTION_CURRENCY_ENUM": HARD_TRANSACTION_CURRENCY_ENUM_VALUES,
}


def allowed_enum_for_rule_id(rule_id: str) -> FrozenSet[str] | None:
    """Return allowed string values for a rule_id, or None if not an enum-style rule."""

    return RULE_ID_TO_ENUM_VALUES.get(rule_id)


def is_age_rule_id(rule_id: str) -> bool:
    return rule_id == "SOFT_CUSTOMER_AGE_RANGE"


def age_bounds_for_rule_id(rule_id: str) -> tuple[int, int] | None:
    if rule_id == "SOFT_CUSTOMER_AGE_RANGE":
        return (SOFT_CUSTOMER_AGE_RANGE_MIN, SOFT_CUSTOMER_AGE_RANGE_MAX)
    return None
