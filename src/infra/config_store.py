from __future__ import annotations

import json
from pathlib import Path

# Canonical directory for project-wide financial knowledge JSON (Requirement 1 + Requirement 2).
PROJECT_CONFIG_DIR = Path(__file__).resolve().parent.parent / "project_config"
PROJECT_CONFIG_SEARCH_DIRS = (
    PROJECT_CONFIG_DIR / "common",
    PROJECT_CONFIG_DIR / "schema",
    PROJECT_CONFIG_DIR / "synth",
    PROJECT_CONFIG_DIR,
)

# R2 synthesis JSON under project_config/synth/{topology,column_semantics,behavior,compliance,gates}/.
SYNTH_MANIFEST = "synth/topology/generation_config_manifest.json"
SYNTH_TOPOLOGY = "synth/topology/generation_topology.json"
SYNTH_CARDINALITY_PROFILES = "synth/topology/cardinality_profiles.json"
SYNTH_COLUMN_PROFILES = "synth/column_semantics/column_profiles_unified.json"
SYNTH_JSON_OBJECT_PACKS = "synth/column_semantics/json_object_packs.json"
SYNTH_GENERATION_RULES = "synth/behavior/generation_rules.json"
SYNTH_SCENARIO_OVERLAYS = "synth/behavior/scenario_overlays.json"
SYNTH_LIFECYCLE_CONSTRAINTS = "synth/compliance/lifecycle_constraints.json"
SYNTH_STATUS_NORMALIZATION = "synth/compliance/status_value_normalization.json"


def load_schema_config(config_name: str) -> dict[str, object]:
    config_path = schema_config_path(config_name=config_name)
    return json.loads(config_path.read_text(encoding="utf-8"))


def write_schema_config(config_name: str, payload: dict[str, object]) -> None:
    config_path = schema_config_path(config_name=config_name)
    config_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def schema_config_path(config_name: str) -> Path:
    normalized = str(config_name).strip()
    if "/" in normalized:
        return PROJECT_CONFIG_DIR / normalized
    for base_dir in PROJECT_CONFIG_SEARCH_DIRS:
        candidate = base_dir / normalized
        if candidate.exists():
            return candidate
    return PROJECT_CONFIG_DIR / normalized
