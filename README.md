# Financial Data Synthesizer

Rule-first synthetic data generation for financial scenarios.

This repository supports two workflows:

- **Requirement 1**: scenario text -> generated schema artifacts.
- **Requirement 2**: input schema -> **`synthetic_data/*.csv`** (tier A: generation only; no post-hoc validation export).


## Quick Start

### Environment setup

1. **Python**: `3.11+` (this repo is exercised on **3.11.15**).
2. **Virtual environment** (choose one):
   - **conda** (recommended):  
     `conda create -n financial_ass python=3.11.15 -y`  
     `conda activate financial_ass`
   - **venv**:  
     `python3.11 -m venv .venv`  
     `source .venv/bin/activate`  
     On Windows: `.venv\Scripts\activate`
3. **Install dependencies** from the **repository root**:  
   `pip install -r requirements.txt`
4. **Working directory**: run all `python -m src.interfaces...` commands from the repo root so `src` imports resolve.

### Requirement 1: text -> schema

```bash
python -m src.interfaces.cli_schema \
  --scenario-text "CRM customers have accounts and transactions with json xml text fields" \
  --output-dir outputs/runs/manual
```

Requirement 1 default sample scenarios in the app:

- **Trading · best execution audit**  
  The brokerage system stores trading accounts, trade orders, executions, and market instruments for best execution audit.
- **Credit · limits and capital**  
  The wholesale credit data mart links each obligor to committed facilities and measured exposure; drawdown and repayment events are stored for limit and capital monitoring.
- **CRM · profiles, accounts, servicing**  
  The bank operates a customer relationship platform: each customer has a profile, one or more product accounts, posted transactions, and logged channel interactions; servicing teams open cases for complaints and track SLA until resolution.
- **Compliance · KYC and screening**  
  Before onboarding completes, compliance maintains KYC cases and runs sanctions screening hits against the customer master, recording outcomes for audit.
- **Universal bank · risk monitoring**  
  The universal bank runs retail deposits, card products, corporate lending, and treasury; risk and finance need obligor-level exposure alongside facility limits for monitoring.

### Requirement 2: schema -> synthetic CSV

```bash
python -m src.interfaces.cli_synth \
  --schema-path requirement/sample_schema.sql \
  --output-dir outputs/runs/manual \
  --record-count 300 \
  --seed 20260409
```

Recommended schema paths to try for Requirement 2:

- `requirement/sample_schema.sql` (repo built-in sample)
- `examples/test_schemas/r2_customer_lead_opportunity_account_txn_scd.sql` (schema generated from Requirement 1 runs)

### Streamlit (built-in defaults)

Run:

```bash
streamlit run src/interfaces/streamlit_app.py
```

## More about this project

Project docs:

- Requirement 1 quickstart: `docs/requirement1_quickstart.md`
- Requirement 2 quickstart: `docs/requirement2_quickstart.md`
- Requirement 2 targeted checklist (CRM lead-opportunity-account-transaction-SCD): `docs/requirement2_r2_customer_lead_opportunity_account_txn_scd_checklist.md`

---

## Config Validation (Always Run After Config Changes)

```bash
make validate-config
```

Equivalent command:

```bash
python -m src.schema.financial_config_validate
```

Expected output:

```text
financial schema configs OK
```

---
