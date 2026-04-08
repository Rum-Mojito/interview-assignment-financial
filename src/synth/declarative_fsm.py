"""
Load declarative_generation_rules.json: state machines + column bindings for generation.

Aligned with HARD_STATUS_STATE_MACHINE: sampled sequences respect allowed_transitions.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.infra.config_store import SYNTH_GENERATION_RULES, SYNTH_LIFECYCLE_CONSTRAINTS, load_schema_config
from src.schema.models import TableDefinition


@dataclass(frozen=True)
class StateMachineSpec:
    machine_id: str
    initial_states: tuple[str, ...]
    allowed_transitions: dict[str, frozenset[str]]
    aligned_hard_rule_id: str


@dataclass(frozen=True)
class ColumnFsmBinding:
    table_name: str
    column_name: str
    lifecycle_rule_id: str
    requires_ordered_rows_by: str


def load_state_machine_from_lifecycle_rule(rule_id: str) -> StateMachineSpec | None:
    payload = _load_lifecycle_constraints_payload()
    machines = payload.get("state_machine_rules", [])
    if not isinstance(machines, list):
        return None
    for raw in machines:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("rule_id", "")) != rule_id:
            continue
        initial = tuple(str(x) for x in (raw.get("initial_states") or []) if x and str(x).strip())
        trans = raw.get("allowed_transitions", {})
        allowed: dict[str, frozenset[str]] = {}
        if isinstance(trans, dict):
            for k, v in trans.items():
                if isinstance(v, list):
                    allowed[str(k)] = frozenset(str(x) for x in v)
        return StateMachineSpec(
            machine_id=str(raw.get("rule_id", rule_id)),
            initial_states=initial,
            allowed_transitions=allowed,
            aligned_hard_rule_id=str(raw.get("rule_id", rule_id)),
        )
    return None


def load_column_fsm_binding(table_name: str, column_name: str) -> ColumnFsmBinding | None:
    payload = _load_generation_rules_payload()
    bindings = payload.get("column_to_state_machine", [])
    if not isinstance(bindings, list):
        return None
    column_lower = column_name.lower()
    for raw in bindings:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("table_name", "")) != table_name:
            continue
        if str(raw.get("column_name", "")).lower() != column_lower:
            continue
        return ColumnFsmBinding(
            table_name=str(raw.get("table_name", "")),
            column_name=str(raw.get("column_name", "")),
            lifecycle_rule_id=str(raw.get("lifecycle_rule_id", "")),
            requires_ordered_rows_by=str(raw.get("requires_ordered_rows_by", "")),
        )
    return None


def table_has_fsm_columns(table: TableDefinition) -> tuple[ColumnFsmBinding, StateMachineSpec] | None:
    for col in table.columns:
        binding = load_column_fsm_binding(table.name, col.name)
        if binding is None:
            continue
        fsm = load_state_machine_from_lifecycle_rule(binding.lifecycle_rule_id)
        if fsm is None:
            continue
        time_col = binding.requires_ordered_rows_by
        if time_col and any(c.name == time_col for c in table.columns):
            return (binding, fsm)
    return None


def sample_fsm_status_sequence(
    *,
    row_count: int,
    fsm: StateMachineSpec,
    rng: random.Random,
) -> list[str]:
    if row_count <= 0:
        return []
    initials = list(fsm.initial_states) or list(fsm.allowed_transitions.keys())
    if not initials:
        return ["active"] * row_count
    status = str(rng.choice(initials))
    out = [status]
    for _ in range(1, row_count):
        nxt_pool = list(fsm.allowed_transitions.get(status, frozenset()))
        if not nxt_pool:
            nxt_pool = [status]
        status = str(rng.choice(nxt_pool))
        out.append(status)
    return out


def apply_fsm_row_overrides(
    *,
    table: TableDefinition,
    rows: list[dict[str, object]],
    rng: random.Random,
) -> None:
    """Mutate rows so status follows FSM when declarative binding + ordering column exist."""

    meta = table_has_fsm_columns(table)
    if meta is None:
        return
    binding, fsm = meta
    time_key = binding.requires_ordered_rows_by
    if not rows or not time_key:
        return
    sorted_indices = sorted(range(len(rows)), key=lambda i: str(rows[i].get(time_key, "")))
    seq = sample_fsm_status_sequence(row_count=len(rows), fsm=fsm, rng=rng)
    for position, row_index in enumerate(sorted_indices):
        if position < len(seq):
            rows[row_index][binding.column_name] = seq[position]


def load_active_declarative_rule_pack_ids() -> tuple[str, ...]:
    payload = _load_generation_rules_payload()
    raw = payload.get("generation_behavior_pack_ids", [])
    if isinstance(raw, list):
        return tuple(str(x) for x in raw if x)
    return ()


def _load_generation_rules_payload() -> dict[str, object]:
    payload = load_schema_config(SYNTH_GENERATION_RULES)
    return payload if isinstance(payload, dict) else {}


def _load_lifecycle_constraints_payload() -> dict[str, object]:
    payload = load_schema_config(SYNTH_LIFECYCLE_CONSTRAINTS)
    return payload if isinstance(payload, dict) else {}
