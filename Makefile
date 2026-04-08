# Project-wide financial JSON under src/project_config — run after any manual edit or before commit.
.PHONY: validate-config help

help:
	@echo "Targets:"
	@echo "  make validate-config   Cross-validate all schema config JSON (concepts, packs, relations, profiles, templates, rules)."

validate-config:
	python -m src.schema.financial_config_validate
