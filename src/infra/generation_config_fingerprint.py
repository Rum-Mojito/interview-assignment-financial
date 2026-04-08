from __future__ import annotations

import hashlib

from src.infra.config_store import (
    PROJECT_CONFIG_DIR,
    SYNTH_CARDINALITY_PROFILES,
    SYNTH_COLUMN_PROFILES,
    SYNTH_GENERATION_RULES,
    SYNTH_JSON_OBJECT_PACKS,
    SYNTH_MANIFEST,
    SYNTH_SCENARIO_OVERLAYS,
    SYNTH_TOPOLOGY,
    load_schema_config,
)


_CONFIG_FILES_FOR_FINGERPRINT: tuple[str, ...] = (
    "common/concept_relation_graph.json",
    "common/field_packs.json",
    SYNTH_CARDINALITY_PROFILES,
    SYNTH_COLUMN_PROFILES,
    SYNTH_GENERATION_RULES,
    SYNTH_JSON_OBJECT_PACKS,
    SYNTH_MANIFEST,
    SYNTH_SCENARIO_OVERLAYS,
    SYNTH_TOPOLOGY,
)


def compute_generation_config_fingerprint() -> str:
    """Stable SHA-256 over canonical project_config files that affect R2 generation semantics."""

    hasher = hashlib.sha256()
    for rel_path in sorted(_CONFIG_FILES_FOR_FINGERPRINT):
        path = PROJECT_CONFIG_DIR / rel_path
        if not path.is_file():
            continue
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def load_generation_config_version() -> str:
    manifest = load_schema_config(SYNTH_MANIFEST)
    version = manifest.get("config_version", "")
    return str(version) if version else "unknown"
