"""
Backward-compatible entry point. Canonical implementation lives in ``src.infra``.
"""

from __future__ import annotations

from src.infra.config_store import (
    PROJECT_CONFIG_DIR,
    SYNTH_CARDINALITY_PROFILES,
    SYNTH_COLUMN_PROFILES,
    SYNTH_GENERATION_RULES,
    SYNTH_JSON_OBJECT_PACKS,
    SYNTH_LIFECYCLE_CONSTRAINTS,
    SYNTH_MANIFEST,
    SYNTH_SCENARIO_OVERLAYS,
    SYNTH_STATUS_NORMALIZATION,
    SYNTH_TOPOLOGY,
    load_schema_config,
    schema_config_path,
    write_schema_config,
)

# Legacy name: config directory for JSON shared across R1/R2.
SCHEMA_CONFIG_DIR = PROJECT_CONFIG_DIR

__all__ = [
    "PROJECT_CONFIG_DIR",
    "SCHEMA_CONFIG_DIR",
    "SYNTH_CARDINALITY_PROFILES",
    "SYNTH_COLUMN_PROFILES",
    "SYNTH_GENERATION_RULES",
    "SYNTH_JSON_OBJECT_PACKS",
    "SYNTH_LIFECYCLE_CONSTRAINTS",
    "SYNTH_MANIFEST",
    "SYNTH_SCENARIO_OVERLAYS",
    "SYNTH_STATUS_NORMALIZATION",
    "SYNTH_TOPOLOGY",
    "load_schema_config",
    "schema_config_path",
    "write_schema_config",
]
