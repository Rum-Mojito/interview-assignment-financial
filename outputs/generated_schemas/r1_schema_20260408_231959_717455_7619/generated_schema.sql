CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_segment_id TEXT,
    full_name TEXT,
    age INTEGER,
    country TEXT,
    profile_json JSON,
    kyc_xml XML,
    risk_note_text TEXT,
    journey_stage TEXT CHECK (journey_stage IN ('onboarding', 'first_purchase', 'repeat', 'loyal', 'advocate')),
    total_aum NUMERIC,
    customer_360_json JSON,
    FOREIGN KEY(customer_segment_id) REFERENCES customer_segments(customer_segment_id)
);

CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT,
    bank_product_id TEXT,
    account_type TEXT CHECK (account_type IN ('checking', 'savings', 'credit_line', 'investment', 'other')),
    balance NUMERIC,
    status TEXT CHECK (status IN ('open', 'active', 'closed', 'frozen', 'dormant', 'pending')),
    opened_time TIMESTAMP,
    metadata_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(bank_product_id) REFERENCES bank_products(bank_product_id)
);

CREATE TABLE leads (
    lead_id TEXT PRIMARY KEY,
    customer_id TEXT,
    marketing_campaign_id TEXT,
    lead_source TEXT CHECK (lead_source IN ('web', 'mobile_app', 'branch', 'referral', 'campaign', 'api', 'other')),
    lead_score INTEGER,
    lead_status TEXT CHECK (lead_status IN ('new', 'contacted', 'qualified', 'converted', 'lost', 'recycled')),
    created_time TIMESTAMP,
    assigned_time TIMESTAMP,
    lead_payload_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(marketing_campaign_id) REFERENCES marketing_campaigns(marketing_campaign_id)
);

CREATE TABLE sales_opportunities (
    sales_opportunity_id TEXT PRIMARY KEY,
    customer_id TEXT,
    pipeline_stage TEXT CHECK (pipeline_stage IN ('prospecting', 'needs_analysis', 'solution_presentation', 'negotiation', 'closed_won', 'closed_lost')),
    expected_amount NUMERIC,
    expected_close_time TIMESTAMP,
    actual_close_time TIMESTAMP,
    opportunity_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE bank_products (
    bank_product_id TEXT PRIMARY KEY,
    product_name TEXT CHECK (product_name IN ('Classic Demand Deposit', 'Step-Up Time Deposit 12M', 'Wealth Dual-Currency Note', 'Retail Mortgage Prime', 'Revolving Working Capital Line', 'Platinum Rewards Card', 'Money Market Liquidity Fund', 'FX Forward Contract Pack', 'Structured Deposit Series A', 'Junior Savings Starter', 'Private Banking Cash Sweep', 'Trade Finance Import LC', 'Green Loan Facility', 'Custody Omnibus Service', 'Bancassurance Term Life')),
    product_category TEXT CHECK (product_category IN ('deposit', 'wealth', 'lending', 'insurance', 'cards', 'other')),
    risk_tier TEXT CHECK (risk_tier IN ('r1', 'r2', 'r3', 'r4', 'r5')),
    product_json JSON
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    amount NUMERIC,
    currency TEXT CHECK (currency IN ('CNY', 'USD', 'EUR', 'HKD', 'SGD')),
    transaction_time TIMESTAMP,
    details_json JSON,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE customer_interactions (
    interaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    channel TEXT CHECK (channel IN ('branch', 'mobile_app', 'web', 'call_center', 'atm', 'other')),
    sentiment_score NUMERIC,
    interaction_time TIMESTAMP,
    interaction_json JSON,
    interaction_note_text TEXT,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE servicing_cases (
    servicing_case_id TEXT PRIMARY KEY,
    customer_id TEXT,
    case_type TEXT CHECK (case_type IN ('complaint', 'inquiry', 'request', 'dispute', 'other')),
    status TEXT CHECK (status IN ('open', 'in_progress', 'resolved', 'closed')),
    priority TEXT CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    opened_time TIMESTAMP,
    closed_time TIMESTAMP,
    case_summary_text TEXT,
    case_payload_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE customer_segments (
    customer_segment_id TEXT PRIMARY KEY,
    segment_code TEXT,
    segment_name TEXT CHECK (segment_name IN ('emerging_affluent', 'mass_affluent', 'high_net_worth', 'private_banking', 'small_business_owner')),
    segment_tier TEXT CHECK (segment_tier IN ('TIER_1', 'TIER_2', 'TIER_3', 'TIER_4')),
    risk_band TEXT CHECK (risk_band IN ('LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH')),
    tier_rank INTEGER,
    segment_json JSON
);

CREATE TABLE employees (
    employee_id TEXT PRIMARY KEY,
    employee_code TEXT,
    job_title TEXT CHECK (job_title IN ('relationship_manager', 'team_head', 'support', 'other')),
    branch_code TEXT,
    hire_time TIMESTAMP,
    employee_json JSON
);

CREATE TABLE relationship_assignments (
    relationship_assignment_id TEXT PRIMARY KEY,
    customer_id TEXT,
    employee_id TEXT,
    assignment_role TEXT CHECK (assignment_role IN ('primary_rm', 'secondary_rm', 'support')),
    effective_from TIMESTAMP,
    effective_until TIMESTAMP,
    assignment_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE investment_holdings (
    investment_holding_id TEXT PRIMARY KEY,
    account_id TEXT,
    quantity NUMERIC,
    market_value NUMERIC,
    currency TEXT CHECK (currency IN ('CNY', 'USD', 'EUR', 'HKD', 'SGD')),
    as_of_time TIMESTAMP,
    holding_json JSON,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE marketing_campaigns (
    marketing_campaign_id TEXT PRIMARY KEY,
    campaign_name TEXT CHECK (campaign_name IN ('new_to_bank_onboarding_drive', 'salary_account_activation_wave', 'wealth_upgrade_advisory_month', 'mortgage_cross_sell_sprint', 'credit_card_reactivation_nudge', 'retention_winback_program')),
    campaign_channel TEXT CHECK (campaign_channel IN ('email', 'sms', 'push', 'branch', 'multi', 'other')),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    campaign_json JSON
);

CREATE TABLE client_tasks (
    client_task_id TEXT PRIMARY KEY,
    customer_id TEXT,
    task_type TEXT CHECK (task_type IN ('follow_up', 'review', 'reminder', 'onboarding', 'other')),
    due_time TIMESTAMP,
    task_status TEXT CHECK (task_status IN ('open', 'done', 'cancelled')),
    task_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE appointments (
    appointment_id TEXT PRIMARY KEY,
    customer_id TEXT,
    employee_id TEXT,
    appointment_channel TEXT CHECK (appointment_channel IN ('branch', 'video', 'phone', 'other')),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    appointment_status TEXT CHECK (appointment_status IN ('scheduled', 'completed', 'no_show', 'cancelled')),
    appointment_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE customer_entitlements (
    customer_entitlement_id TEXT PRIMARY KEY,
    customer_id TEXT,
    entitlement_code TEXT,
    tier_level TEXT,
    valid_from TIMESTAMP,
    valid_until TIMESTAMP,
    entitlement_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);
