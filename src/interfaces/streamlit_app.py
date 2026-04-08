from __future__ import annotations

from pathlib import Path
import sys
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from src.interfaces.cli_schema import SchemaGenerationConfig, run_requirement1
from src.interfaces.cli_synth import SyntheticDataGenerationConfig, run_requirement2


OUTPUT_DIR = PROJECT_ROOT / "outputs"
UPLOADED_SCHEMA_DIR = OUTPUT_DIR / "uploaded_schemas"

# Streamlit defaults when not using clean mode (see sidebar checkbox; R1 only)
R1_UNKNOWN_DOMAIN_POLICY_DEFAULT = "draft_only"

# Requirement 1: curated samples for demos (label, scenario_text). First row = user-edited / empty.
R1_SAMPLE_SCENARIOS: list[tuple[str, str]] = [
    ("Custom (type or paste below)", ""),
    (
        "Trading · best execution audit",
        "The brokerage system stores trading accounts, trade orders, executions, and market "
        "instruments for best execution audit.",
    ),
    (
        "Credit · limits and capital",
        "The wholesale credit data mart links each obligor to committed facilities and measured "
        "exposure; drawdown and repayment events are stored for limit and capital monitoring.",
    ),
    (
        "CRM · profiles, accounts, servicing",
        "The bank operates a customer relationship platform: each customer has a profile, "
        "one or more product accounts, posted transactions, and logged channel interactions; "
        "servicing teams open cases for complaints and track SLA until resolution.",
    ),
    (
        "Compliance · KYC and screening",
        "Before onboarding completes, compliance maintains KYC cases and runs sanctions screening "
        "hits against the customer master, recording outcomes for audit.",
    ),
    (
        "Universal bank · risk monitoring",
        "The universal bank runs retail deposits, card products, corporate lending, and treasury; "
        "risk and finance need obligor-level exposure alongside facility limits for monitoring.",
    ),
]


def main() -> None:
    st.set_page_config(page_title="Financial Data Synthesizer", layout="wide")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    UPLOADED_SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    st.title("Financial Data Synthesizer")
    st.caption(f"Artifacts: `{OUTPUT_DIR}`")

    page = st.sidebar.radio(
        "Page",
        [
            "Requirement 1: Natural language -> Schema",
            "Requirement 2: Schema -> Synthetic Data",
        ],
    )
    clean_mode = st.sidebar.checkbox(
        "Clean mode (R1: minimal artifacts)",
        value=True,
        help="Requirement 1 only: skip entity_match_report, run_metadata, etc. Requirement 2 always writes only synthetic_data/*.csv.",
    )

    if page == "Requirement 1: Natural language -> Schema":
        _render_requirement1_page(clean_mode=clean_mode)
    else:
        _render_requirement2_page()

    _render_recent_outputs()


def _render_requirement1_page(*, clean_mode: bool) -> None:
    st.subheader("Requirement 1: Natural language -> Schema")

    sample_labels = [pair[0] for pair in R1_SAMPLE_SCENARIOS]
    if "_r1_sample_choice_snapshot" not in st.session_state:
        st.session_state._r1_sample_choice_snapshot = sample_labels[0]
    if "r1_scenario_text" not in st.session_state:
        st.session_state.r1_scenario_text = ""

    sample_choice = st.selectbox(
        "Sample scenario (loads into the text area; you can edit after)",
        options=sample_labels,
        key="r1_sample_choice_widget",
    )
    if sample_choice != st.session_state._r1_sample_choice_snapshot:
        loaded = next(text for label, text in R1_SAMPLE_SCENARIOS if label == sample_choice)
        st.session_state.r1_scenario_text = loaded
        st.session_state._r1_sample_choice_snapshot = sample_choice

    scenario_text = st.text_area(
        "Business scenario text",
        height=160,
        key="r1_scenario_text",
        placeholder="Example: CRM customers have accounts and transactions with json xml text fields",
    )

    if st.button("Generate Schema", type="primary"):
        normalized = scenario_text.strip()
        if not normalized:
            st.error("Please input scenario text.")
            return
        config = SchemaGenerationConfig(
            scenario_text=normalized,
            output_dir=OUTPUT_DIR,
            write_schema_report=not clean_mode,
            unknown_domain_policy=R1_UNKNOWN_DOMAIN_POLICY_DEFAULT,
            clean_mode=clean_mode,
        )
        try:
            config.validate()
            run_dir = run_requirement1(config=config)
        except Exception as exc:
            st.exception(exc)
            return

        st.success(f"Requirement 1 completed. Run directory: `{run_dir}`")
        _show_requirement1_generated_schema(run_dir=run_dir)
        if not clean_mode:
            _show_common_run_files(run_dir=run_dir)


def _render_requirement2_page() -> None:
    st.subheader("Requirement 2: Schema -> Synthetic Data")
    st.caption("Schema: SQL or JSON. Outputs **`synthetic_data/*.csv`** only (tier A: no validation reports or bad_case).")

    source_type = st.radio("Schema source", ["Upload file", "Use existing path"], horizontal=True)
    schema_path: Path | None = None

    if source_type == "Upload file":
        uploaded = st.file_uploader("Upload schema (.sql or .json)", type=["sql", "json"])
        if uploaded is not None:
            suffix = Path(uploaded.name).suffix.lower()
            if suffix not in {".sql", ".json"}:
                st.error("Only .sql or .json schema files are supported.")
                return
            save_path = UPLOADED_SCHEMA_DIR / uploaded.name
            save_path.write_bytes(uploaded.getvalue())
            schema_path = save_path
            st.success(f"Schema uploaded to: {save_path}")
    else:
        raw_path = st.text_input(
            "Schema file path",
            value=str(PROJECT_ROOT / "examples/test_schemas/r2_customer_lead_opportunity_account_txn_scd.sql"),
        ).strip()
        if raw_path:
            schema_path = Path(raw_path)

    col1, col2 = st.columns(2)
    with col1:
        record_count = st.number_input("Record count", min_value=1, value=300, step=1)
    with col2:
        seed = st.number_input("Seed", min_value=0, value=20260409, step=1)

    if st.button("Generate Synthetic Data", type="primary"):
        if schema_path is None:
            st.error("Please provide a schema file first.")
            return

        config = SyntheticDataGenerationConfig(
            schema_path=schema_path,
            output_dir=OUTPUT_DIR,
            record_count=int(record_count),
            seed=int(seed),
        )
        try:
            config.validate()
            run_dir = run_requirement2(config=config)
        except Exception as exc:
            st.exception(exc)
            return

        st.success(f"Requirement 2 completed. Run directory: `{run_dir}`")
        _show_synthetic_data_folder_link(run_dir=run_dir)


def _show_requirement1_generated_schema(*, run_dir: Path) -> None:
    sql_path = run_dir / "generated_schema.sql"
    json_path = run_dir / "generated_schema.json"
    if not sql_path.exists() and not json_path.exists():
        draft_sql = run_dir / "draft_schema.sql"
        draft_json = run_dir / "draft_schema.json"
        if draft_sql.exists() or draft_json.exists():
            st.warning("Unknown domain: showing **draft** schema (if present) instead of final generated schema.")
            sql_path = draft_sql
            json_path = draft_json
        else:
            st.info("No `generated_schema.sql` / `generated_schema.json` found in this run directory.")
            return

    st.markdown("#### Generated schema (this run)")
    tab_sql, tab_json = st.tabs(["SQL DDL", "JSON"])
    with tab_sql:
        if sql_path.exists():
            st.code(sql_path.read_text(encoding="utf-8"), language="sql")
        else:
            st.caption("No SQL file for this run.")
    with tab_json:
        if json_path.exists():
            raw = json_path.read_text(encoding="utf-8")
            try:
                st.json(json.loads(raw))
            except json.JSONDecodeError:
                st.code(raw, language="json")
        else:
            st.caption("No JSON file for this run.")


def _show_synthetic_data_folder_link(*, run_dir: Path) -> None:
    data_dir = (run_dir / "synthetic_data").resolve()
    st.markdown("#### Synthetic data folder")
    if not data_dir.is_dir():
        st.warning(f"No `synthetic_data` directory at: `{data_dir}`")
        return
    folder_uri = data_dir.as_uri()
    st.markdown(
        f"**Open folder (local `file://` link):** "
        f"[{data_dir}]({folder_uri})"
    )
    st.caption(
        "If the link does not open in your browser, copy the path below and open it in Finder / Explorer / VS Code."
    )
    st.code(str(data_dir), language="text")

    csv_files = sorted(data_dir.glob("*.csv"))
    if csv_files:
        with st.expander(f"CSV files in this run ({len(csv_files)})", expanded=False):
            for csv_path in csv_files[:40]:
                st.text(csv_path.name)
            if len(csv_files) > 40:
                st.caption(f"… and {len(csv_files) - 40} more")


def _show_common_run_files(*, run_dir: Path) -> None:
    quality_report_path = run_dir / "quality_report.json"
    run_metadata_path = run_dir / "run_metadata.json"
    rule_violations_path = run_dir / "rule_violations.csv"

    if quality_report_path.exists():
        st.markdown("#### quality_report.json")
        st.json(json.loads(quality_report_path.read_text(encoding="utf-8")))
    if run_metadata_path.exists():
        st.markdown("#### run_metadata.json")
        st.json(json.loads(run_metadata_path.read_text(encoding="utf-8")))
    if rule_violations_path.exists():
        st.markdown("#### rule_violations.csv (preview)")
        preview = rule_violations_path.read_text(encoding="utf-8").splitlines()[:20]
        st.code("\n".join(preview))


def _render_recent_outputs() -> None:
    st.divider()
    st.subheader("Recent Runs in outputs/")
    r1_runs = sorted(OUTPUT_DIR.glob("r1_schema_*"), reverse=True)[:5]
    r2_runs = sorted(OUTPUT_DIR.glob("r2_data_*"), reverse=True)[:5]

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("**Requirement 1**")
        if not r1_runs:
            st.write("-")
        for run in r1_runs:
            st.code(str(run))
    with col_right:
        st.markdown("**Requirement 2**")
        if not r2_runs:
            st.write("-")
        for run in r2_runs:
            st.code(str(run))


if __name__ == "__main__":
    main()
