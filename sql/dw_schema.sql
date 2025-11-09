
DROP TABLE IF EXISTS dim_stock CASCADE;
DROP TABLE IF EXISTS dim_datetime CASCADE;
DROP TABLE IF EXISTS fact_stock_indicators CASCADE;

CREATE TABLE dim_stock (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255)
);

CREATE TABLE dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_dim_datetime_date ON dim_datetime (date);

CREATE TABLE fact_stock_indicators (
    record_id SERIAL PRIMARY KEY,
    stock_id INT NOT NULL REFERENCES dim_stock(stock_id) ON DELETE CASCADE,
    datetime_id INT NOT NULL REFERENCES dim_datetime(datetime_id) ON DELETE CASCADE,
    close NUMERIC(12,4),
    volume BIGINT,
    diff NUMERIC(12,4),
    percent_change_close NUMERIC(12,6),
    rsi NUMERIC(8,4),
    roc NUMERIC(8,4),
    bb_upper NUMERIC(12,4),
    bb_lower NUMERIC(12,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_stock_id ON fact_stock_indicators(stock_id);
CREATE INDEX IF NOT EXISTS idx_fact_datetime_id ON fact_stock_indicators(datetime_id);


