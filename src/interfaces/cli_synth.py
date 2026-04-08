"""
Requirement 2 CLI: load schema from SQL/JSON file -> synthetic CSV export (tier A: generation only).
"""

from __future__ import annotations

from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path

from src.infra.config_store import SYNTH_TOPOLOGY
from src.infra.exporters.csv_exporter import export_synthetic_data_csv
from src.schema.financial_config_validate import validate_financial_schema_configs
from src.schema.parser import parse_schema
from src.schema.planner import plan_table_order
from src.synth.concept_schema_mapping import map_schema_to_concepts
from src.synth.generator import generate_dataset


@dataclass(frozen=True)
class SyntheticDataGenerationConfig:
    """R2 slim entry: schema file -> `synthetic_data/*.csv` under a new run directory."""

    schema_path: Path
    output_dir: Path
    record_count: int
    seed: int
    graph_path_id: str | None = None

    def validate(self) -> None:
        if self.record_count <= 0:
            raise ValueError("record_count must be greater than 0")
        if self.seed < 0:
            raise ValueError("seed must be non-negative")
        if not self.schema_path.exists():
            raise FileNotFoundError(f"schema path does not exist: {self.schema_path}")


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Requirement 2: generate synthetic data from a SQL or JSON schema (CSV output only).",
    )
    parser.add_argument("--schema-path", required=True, type=Path, help="Path to SQL or JSON schema file.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output directory for artifacts.")
    parser.add_argument("--record-count", required=True, type=int, help="Base record count per parent table.")
    parser.add_argument("--seed", required=True, type=int, help="Deterministic seed for data generation.")
    parser.add_argument(
        "--graph-path-id",
        default=None,
        type=str,
        help=(
            f"Pin {SYNTH_TOPOLOGY} graph_event_paths.path_id "
            "(e.g. crm_deposit_graph). Default: first matching path."
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = SyntheticDataGenerationConfig(
        schema_path=args.schema_path,
        output_dir=args.output_dir,
        record_count=args.record_count,
        seed=args.seed,
        graph_path_id=args.graph_path_id,
    )
    config.validate()
    run_requirement2(config=config)


def run_requirement2(config: SyntheticDataGenerationConfig) -> Path:
    validate_financial_schema_configs()
    run_output_dir = _create_r2_run_dir(base_output_dir=config.output_dir, seed=config.seed)

    schema = parse_schema(schema_path=config.schema_path)
    ordered_tables = plan_table_order(schema=schema)
    concept_mapping = map_schema_to_concepts(schema=schema, graph_path_id=config.graph_path_id)

    generated_dataset = generate_dataset(
        schema=schema,
        ordered_tables=ordered_tables,
        record_count=config.record_count,
        seed=config.seed,
        concept_mapping=concept_mapping,
    )

    synthetic_data_dir = run_output_dir / "synthetic_data"
    export_synthetic_data_csv(
        records_by_table=generated_dataset.records_by_table,
        synthetic_data_dir=synthetic_data_dir,
    )

    print(f"requirement2 data generation completed: {run_output_dir}")
    return run_output_dir


def _create_r2_run_dir(base_output_dir: Path, seed: int) -> Path:
    from datetime import datetime, timezone
    import random

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    random_suffix = random.Random().randint(1000, 9999)
    run_output_dir = base_output_dir / f"r2_data_{timestamp}_seed_{seed}_{random_suffix}"
    run_output_dir.mkdir(parents=True, exist_ok=False)
    return run_output_dir


if __name__ == "__main__":
    main()
