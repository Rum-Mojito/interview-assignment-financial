"""
Requirement 1 CLI: natural language -> schema artifacts only (no synthetic data).
"""

from __future__ import annotations

from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import sys

from src.schema.financial_config_validate import validate_financial_schema_configs
from src.schema.onboarding import (
    apply_onboarding_session,
    build_onboarding_session_payload,
    normalize_identifier,
    parse_columns_input,
    parse_relation_input,
    write_onboarding_session_file,
)
from src.schema.planner import plan_table_order
from src.schema.scenario_generator import (
    build_unknown_domain_draft_schema,
    generate_schema_from_scenario_with_report,
    generate_schema_from_system,
    schema_to_json_payload,
    schema_to_sql_ddl,
)


@dataclass(frozen=True)
class SchemaGenerationConfig:
    scenario_text: str
    output_dir: Path
    write_schema_report: bool
    unknown_domain_policy: str
    # When True: only generated_schema.sql/json (and draft_schema.* if unknown); no run_config,
    # run_metadata, entity_match_report, feedback log, or onboarding_session files.
    clean_mode: bool = True

    def validate(self) -> None:
        if not (self.scenario_text or "").strip():
            raise ValueError("scenario_text must be non-empty")
        if self.unknown_domain_policy not in {"interactive", "draft_only"}:
            raise ValueError("unknown_domain_policy must be one of: interactive, draft_only")

    def snapshot(self, run_output_dir: Path) -> None:
        snapshot_path = run_output_dir / "run_config.json"
        payload = {
            "pipeline": "requirement1",
            "scenario_text": self.scenario_text,
            "output_dir": str(self.output_dir),
            "write_schema_report": self.write_schema_report,
            "unknown_domain_policy": self.unknown_domain_policy,
            "clean_mode": self.clean_mode,
        }
        snapshot_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Requirement 1: generate schema from business scenario text (no data generation).",
    )
    parser.add_argument(
        "--scenario-text",
        required=True,
        type=str,
        help="Business scenario sentence used to auto-generate schema.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=str,
        help="Base output directory; each run creates a subdirectory with generated_schema.*",
    )
    parser.add_argument(
        "--unknown-domain-policy",
        default="interactive",
        choices=["interactive", "draft_only"],
        help="When scenario is unknown: interactive onboarding or draft_only.",
    )
    parser.add_argument(
        "--write-schema-report",
        action="store_true",
        help="Write entity_match_report.json and append schema_feedback_log.jsonl (full mode only).",
    )
    parser.add_argument(
        "--full-artifacts",
        action="store_true",
        help="Write run_config, run_metadata, onboarding session, and optional match reports (default is clean mode).",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    clean_mode = not bool(args.full_artifacts)
    config = SchemaGenerationConfig(
        scenario_text=str(args.scenario_text),
        output_dir=Path(args.output_dir),
        write_schema_report=bool(args.write_schema_report),
        unknown_domain_policy=str(args.unknown_domain_policy),
        clean_mode=clean_mode,
    )
    config.validate()
    run_requirement1(config=config)


def run_requirement1(config: SchemaGenerationConfig) -> Path:
    validate_financial_schema_configs()
    run_output_dir = _create_r1_run_dir(base_output_dir=config.output_dir)
    if not config.clean_mode:
        config.snapshot(run_output_dir=run_output_dir)

    schema_source = "scenario_text"
    onboarding_session_path: str | None = None
    entity_match_report: dict[str, object] = {}
    scenario_text = str(config.scenario_text).strip()

    schema, entity_match_report = generate_schema_from_scenario_with_report(scenario_text=scenario_text)
    if bool(entity_match_report.get("unknown_domain")):
        draft_schema = build_unknown_domain_draft_schema()
        _write_unknown_draft_schema_artifacts(draft_schema=draft_schema, run_output_dir=run_output_dir)
        if not config.clean_mode:
            session_payload = _build_default_onboarding_session(
                scenario_text=scenario_text,
                entity_match_report=entity_match_report,
            )
            if config.unknown_domain_policy == "interactive" and sys.stdin.isatty():
                interactive_session_payload, apply_now = _interactive_collect_unknown_domain_spec(
                    scenario_text=scenario_text,
                    inferred_system_name=entity_match_report.get("inferred_system_name"),
                )
                if interactive_session_payload is not None:
                    session_payload = interactive_session_payload
                if apply_now and session_payload is not None:
                    apply_result = apply_onboarding_session(session_payload=session_payload)
                    schema = generate_schema_from_system(
                        system_name=str(apply_result["system_name"]),
                        table_names=None,
                    )
                    schema_source = "scenario_text_interactive_onboarding"
                    entity_match_report = {
                        "strategy": "interactive_onboarding",
                        "scenario_text": scenario_text,
                        "inferred_system_name": apply_result["system_name"],
                        "selected_source": "interactive_onboarding",
                        "selected_table_names": [table.name for table in schema.tables],
                        "unknown_domain": False,
                        "needs_review": False,
                        "confidence_score": 0.95,
                        "score_margin": 1.0,
                        "interactive_onboarding": {
                            "system_name": session_payload["system_name"],
                            "entity_names": session_payload["entity_names"],
                            "relations": session_payload["relations"],
                            "entity_columns_by_entity": session_payload.get("entity_columns_by_entity", {}),
                            "config_persisted": True,
                            "concept_ids": apply_result["concept_ids"],
                        },
                        "candidates": [],
                    }
            elif config.unknown_domain_policy == "interactive":
                print(
                    "unknown domain detected but stdin is not interactive, "
                    "fallback to draft_only behavior."
                )
            onboarding_session_path = _write_onboarding_session_artifact(
                session_payload=session_payload,
                run_output_dir=run_output_dir,
            )
            entity_match_report["onboarding_session_path"] = onboarding_session_path

    _write_generated_schema_artifacts(schema=schema, run_output_dir=run_output_dir)
    if config.write_schema_report and not config.clean_mode:
        _write_entity_match_report(entity_match_report=entity_match_report, run_output_dir=run_output_dir)
        _append_schema_feedback_log(
            output_dir=config.output_dir,
            scenario_text=scenario_text,
            entity_match_report=entity_match_report,
        )

    if not config.clean_mode:
        ordered_tables = plan_table_order(schema=schema)
        raw_inferred = entity_match_report.get("inferred_system_name")
        inferred_system_name: str | None = None
        if isinstance(raw_inferred, str) and raw_inferred.strip():
            inferred_system_name = raw_inferred.strip()

        _write_run_metadata_requirement1(
            run_output_dir=run_output_dir,
            scenario_text=scenario_text,
            inferred_system_name=inferred_system_name,
            schema_source=schema_source,
            needs_review=bool(entity_match_report.get("needs_review", False)),
            unknown_domain=bool(entity_match_report.get("unknown_domain", False)),
            schema_confidence_score=float(entity_match_report.get("confidence_score", 1.0)),
            onboarding_session_path=onboarding_session_path,
            ordered_tables=ordered_tables,
        )
    print(f"requirement1 schema generation completed: {run_output_dir}")
    return run_output_dir


def _create_r1_run_dir(base_output_dir: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    random_suffix = random.Random().randint(1000, 9999)
    run_output_dir = base_output_dir / f"r1_schema_{timestamp}_{random_suffix}"
    run_output_dir.mkdir(parents=True, exist_ok=False)
    return run_output_dir


def _write_run_metadata_requirement1(
    run_output_dir: Path,
    scenario_text: str,
    inferred_system_name: str | None,
    schema_source: str,
    needs_review: bool,
    unknown_domain: bool,
    schema_confidence_score: float,
    onboarding_session_path: str | None,
    ordered_tables: list[str],
) -> None:
    metadata_payload = {
        "pipeline": "requirement1",
        "scenario_text": scenario_text,
        "inferred_system_name": inferred_system_name,
        "schema_source": schema_source,
        "needs_review": needs_review,
        "unknown_domain": unknown_domain,
        "schema_confidence_score": schema_confidence_score,
        "onboarding_session_path": onboarding_session_path,
        "ordered_tables": ordered_tables,
    }
    metadata_path = run_output_dir / "run_metadata.json"
    metadata_path.write_text(json.dumps(metadata_payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _write_generated_schema_artifacts(schema: object, run_output_dir: Path) -> None:
    sql_schema_path = run_output_dir / "generated_schema.sql"
    json_schema_path = run_output_dir / "generated_schema.json"
    sql_schema_path.write_text(schema_to_sql_ddl(schema=schema), encoding="utf-8")
    json_schema_path.write_text(
        json.dumps(schema_to_json_payload(schema=schema), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def _write_entity_match_report(entity_match_report: dict[str, object], run_output_dir: Path) -> None:
    report_path = run_output_dir / "entity_match_report.json"
    report_path.write_text(
        json.dumps(entity_match_report, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def _write_unknown_draft_schema_artifacts(draft_schema: object, run_output_dir: Path) -> None:
    sql_schema_path = run_output_dir / "draft_schema.sql"
    json_schema_path = run_output_dir / "draft_schema.json"
    sql_schema_path.write_text(schema_to_sql_ddl(schema=draft_schema), encoding="utf-8")
    json_schema_path.write_text(
        json.dumps(schema_to_json_payload(schema=draft_schema), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def _append_schema_feedback_log(
    output_dir: Path,
    scenario_text: str,
    entity_match_report: dict[str, object],
) -> None:
    log_path = output_dir / "schema_feedback_log.jsonl"
    scenario_hash = hashlib.sha256(scenario_text.encode("utf-8")).hexdigest()
    feedback_record = {
        "scenario_hash": scenario_hash,
        "scenario_text": scenario_text,
        "selected_source": entity_match_report.get("selected_source"),
        "selected_table_names": entity_match_report.get("selected_table_names", []),
        "unknown_domain": entity_match_report.get("unknown_domain", False),
        "needs_review": entity_match_report.get("needs_review", False),
        "confidence_score": entity_match_report.get("confidence_score"),
        "candidates": entity_match_report.get("candidates", []),
    }
    with log_path.open("a", encoding="utf-8") as output_file:
        output_file.write(json.dumps(feedback_record, ensure_ascii=True) + "\n")


def _interactive_collect_unknown_domain_spec(
    scenario_text: str,
    inferred_system_name: object,
) -> tuple[dict[str, object] | None, bool]:
    print("unknown domain detected, interactive onboarding is enabled.")
    print(f"scenario_text: {scenario_text}")
    system_default = str(inferred_system_name) if inferred_system_name else "new_financial_system"
    try:
        system_name_input = input(f"please enter system_name [{system_default}]: ").strip()
    except EOFError:
        return None, False
    system_name = normalize_identifier(system_name_input or system_default)
    if not system_name:
        return None, False

    try:
        entity_input = input(
            "please enter core entities (comma-separated, for example: collateral, repo_trade, margin_call): "
        ).strip()
    except EOFError:
        return None, False
    entity_names = [normalize_identifier(item) for item in entity_input.split(",") if item.strip()]
    entity_names = [item for item in entity_names if item]
    if not entity_names:
        return None, False

    try:
        relation_input = input(
            "please enter key dependencies child->parent (comma-separated, for example: margin_call->repo_trade): "
        ).strip()
    except EOFError:
        return None, False
    relation_pairs = parse_relation_input(relation_text=relation_input, allowed_entities=set(entity_names))
    entity_columns_by_entity = _collect_entity_columns_for_onboarding(entity_names=entity_names)

    session_payload = build_onboarding_session_payload(
        scenario_text=scenario_text,
        system_name=system_name,
        entity_names=entity_names,
        relations=relation_pairs,
        entity_columns_by_entity=entity_columns_by_entity,
        source="cli_interactive",
    )

    print("interactive onboarding summary:")
    print(f"system_name={system_name}")
    print(f"entity_names={entity_names}")
    print(f"relations={relation_pairs}")
    print(f"entity_columns_by_entity={entity_columns_by_entity}")
    try:
        confirm = input("confirm and persist to config now? [y/N]: ").strip().lower()
    except EOFError:
        return session_payload, False
    return session_payload, confirm in {"y", "yes"}


def _build_default_onboarding_session(
    scenario_text: str,
    entity_match_report: dict[str, object],
) -> dict[str, object]:
    inferred_system_name = str(entity_match_report.get("inferred_system_name") or "new_financial_system")
    draft_table_names = [str(item) for item in list(entity_match_report.get("draft_table_names", []))]
    proposed_entities = [_table_name_to_concept_name(table_name) for table_name in draft_table_names]
    proposed_entities = [entity_name for entity_name in proposed_entities if entity_name]
    if not proposed_entities:
        proposed_entities = ["business_party", "financial_contract", "financial_event"]
    return build_onboarding_session_payload(
        scenario_text=scenario_text,
        system_name=inferred_system_name,
        entity_names=proposed_entities,
        relations=[],
        entity_columns_by_entity={},
        source="cli_default_unknown",
    )


def _table_name_to_concept_name(table_name: str) -> str:
    normalized_name = normalize_identifier(table_name)
    if normalized_name.endswith("ies"):
        return normalized_name[:-3] + "y"
    if normalized_name.endswith("s"):
        return normalized_name[:-1]
    return normalized_name


def _write_onboarding_session_artifact(
    session_payload: dict[str, object] | None,
    run_output_dir: Path,
) -> str | None:
    if session_payload is None:
        return None
    session_path = run_output_dir / "onboarding_session.json"
    write_onboarding_session_file(session_payload=session_payload, output_path=session_path)
    return str(session_path)


def _collect_entity_columns_for_onboarding(entity_names: list[str]) -> dict[str, list[dict[str, str]]]:
    entity_columns_by_entity: dict[str, list[dict[str, str]]] = {}
    for entity_name in entity_names:
        try:
            columns_input = input(
                f"optional columns for {entity_name} in format name:type,name:type "
                f"(supported: string/integer/decimal/timestamp/json/xml/text/categorical): "
            ).strip()
        except EOFError:
            return entity_columns_by_entity
        parsed_columns = parse_columns_input(columns_text=columns_input)
        if parsed_columns:
            entity_columns_by_entity[entity_name] = parsed_columns
    return entity_columns_by_entity
if __name__ == "__main__":
    main()
