DROP TABLE IF EXISTS stg_market_prices CASCADE;

DROP TABLE IF EXISTS stg_transform_market_prices CASCADE;

DROP TABLE IF EXISTS dim_stock CASCADE;

DROP TABLE IF EXISTS dim_datetime CASCADE;

DROP TABLE IF EXISTS dim_date CASCADE;

DROP TABLE IF EXISTS fact_stock_indicators CASCADE;

CREATE TABLE
    stg_market_prices (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(20) NOT NULL,
        datetime_utc TIMESTAMPTZ NOT NULL,
        close NUMERIC(18, 4),
        volume NUMERIC(18, 4),
        diff NUMERIC(18, 4),
        percent_change_close NUMERIC(10, 4),
        extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE
    dim_stock (
        stock_sk SERIAL PRIMARY KEY,
        ticker VARCHAR(20) NOT NULL UNIQUE
    );

CREATE TABLE
    fact_stock_indicators (
        record_sk SERIAL PRIMARY KEY,
        stock_sk INT NOT NULL REFERENCES dim_stock (stock_sk) ON DELETE CASCADE,
        datetime_utc TIMESTAMPTZ NOT NULL,
        close NUMERIC(12, 4),
        volume BIGINT,
        diff NUMERIC(12, 4),
        percent_change_close NUMERIC(12, 6),
        rsi NUMERIC(8, 4),
        roc NUMERIC(8, 4),
        bb_upper NUMERIC(12, 4),
        bb_lower NUMERIC(12, 4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );