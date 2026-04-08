CREATE TABLE customer_segments (
    customer_segment_id TEXT PRIMARY KEY,
    segment_name TEXT,
    segment_tier TEXT,
    risk_band TEXT,
    segment_payload_json JSON
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_segment_id TEXT,
    full_name TEXT,
    age INTEGER,
    country TEXT,
    journey_stage TEXT,
    total_aum NUMERIC,
    profile_json JSON,
    created_time TIMESTAMP,
    FOREIGN KEY(customer_segment_id) REFERENCES customer_segments(customer_segment_id)
);

CREATE TABLE marketing_campaigns (
    marketing_campaign_id TEXT PRIMARY KEY,
    campaign_name TEXT,
    channel TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

CREATE TABLE leads (
    lead_id TEXT PRIMARY KEY,
    customer_id TEXT,
    marketing_campaign_id TEXT,
    campaign_source TEXT,
    product_interest TEXT,
    lead_score NUMERIC,
    lead_status TEXT,
    captured_time TIMESTAMP,
    assigned_time TIMESTAMP,
    contact_pref_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(marketing_campaign_id) REFERENCES marketing_campaigns(marketing_campaign_id)
);

CREATE TABLE sales_opportunities (
    sales_opportunity_id TEXT PRIMARY KEY,
    lead_id TEXT,
    customer_id TEXT,
    opportunity_stage TEXT,
    expected_value NUMERIC,
    win_probability NUMERIC,
    expected_close_date TIMESTAMP,
    actual_close_time TIMESTAMP,
    opportunity_json JSON,
    FOREIGN KEY(lead_id) REFERENCES leads(lead_id),
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE bank_products (
    bank_product_id TEXT PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    risk_tier TEXT
);

CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    sales_opportunity_id TEXT,
    customer_id TEXT,
    bank_product_id TEXT,
    account_type TEXT,
    status TEXT,
    opened_time TIMESTAMP,
    balance NUMERIC,
    metadata_json JSON,
    FOREIGN KEY(sales_opportunity_id) REFERENCES sales_opportunities(sales_opportunity_id),
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(bank_product_id) REFERENCES bank_products(bank_product_id)
);

CREATE TABLE account_status_scd (
    status_event_id TEXT PRIMARY KEY,
    account_id TEXT,
    status TEXT,
    status_time TIMESTAMP,
    is_current TEXT,
    source_system TEXT,
    trace_id TEXT,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    amount NUMERIC,
    currency TEXT,
    transaction_time TIMESTAMP,
    channel TEXT,
    details_json JSON,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);
