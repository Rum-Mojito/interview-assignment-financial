"""
Project-wide financial knowledge configuration (domains, concepts, relation graph).

Used by Requirement 1 (scenario → schema) and shared by Requirement 2 (schema → data)
for domain/concept IDs, taxonomy, and rule-profile references.
"""

from src.infra.config_store import (
    PROJECT_CONFIG_DIR,
    load_schema_config,
    schema_config_path,
    write_schema_config,
)

__all__ = [
    "PROJECT_CONFIG_DIR",
    "load_schema_config",
    "schema_config_path",
    "write_schema_config",
]
