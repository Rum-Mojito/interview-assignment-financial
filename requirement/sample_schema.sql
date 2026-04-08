
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    age INTEGER,
    country TEXT,
    profile_json JSON
);

CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT,
    account_type TEXT,
    balance NUMERIC,
    metadata_json JSON,
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    amount NUMERIC,
    currency TEXT,
    transaction_time TIMESTAMP,
    details_json JSON,
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);
