CREATE TABLE obligors (
    obligor_id TEXT PRIMARY KEY,
    obligor_name TEXT,
    risk_grade TEXT,
    country TEXT,
    profile_json JSON
);

CREATE TABLE facilities (
    facility_id TEXT PRIMARY KEY,
    obligor_id TEXT,
    approved_amount NUMERIC,
    available_amount NUMERIC,
    facility_status TEXT,
    effective_time TIMESTAMP,
    FOREIGN KEY(obligor_id) REFERENCES obligors(obligor_id)
);

CREATE TABLE exposures (
    exposure_id TEXT PRIMARY KEY,
    facility_id TEXT,
    ead_amount NUMERIC,
    currency TEXT,
    as_of_time TIMESTAMP,
    exposure_note_text TEXT,
    FOREIGN KEY(facility_id) REFERENCES facilities(facility_id)
);

CREATE TABLE drawdown_events (
    drawdown_event_id TEXT PRIMARY KEY,
    facility_id TEXT,
    drawdown_amount NUMERIC,
    drawdown_currency TEXT,
    drawdown_time TIMESTAMP,
    drawdown_json JSON,
    FOREIGN KEY(facility_id) REFERENCES facilities(facility_id)
);

CREATE TABLE repayment_events (
    repayment_event_id TEXT PRIMARY KEY,
    facility_id TEXT,
    principal_amount NUMERIC,
    interest_amount NUMERIC,
    repayment_time TIMESTAMP,
    repayment_json JSON,
    FOREIGN KEY(facility_id) REFERENCES facilities(facility_id)
);

CREATE TABLE collateral_links (
    collateral_link_id TEXT PRIMARY KEY,
    facility_id TEXT,
    collateral_type TEXT,
    collateral_value NUMERIC,
    haircut_percent NUMERIC,
    valuation_time TIMESTAMP,
    FOREIGN KEY(facility_id) REFERENCES facilities(facility_id)
);
