-- ===================================
-- DROP TABLES (nếu đã tồn tại)
-- ===================================
DROP TABLE IF EXISTS fact_stock_indicators CASCADE;
DROP TABLE IF EXISTS dim_stock CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_datetime CASCADE;

-- ===================================
-- DIMENSION: STOCK
-- ===================================
CREATE TABLE dim_stock (
    stock_sk SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE
);

-- ===================================
-- DIMENSION: DATE
-- ===================================
CREATE TABLE dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    weekday INT,
    weekday_name VARCHAR(10),
    is_weekend BOOLEAN
);

-- Populate dim_date từ 2010 đến 2035
DO $$
DECLARE
    d DATE := '2010-01-01';
BEGIN
    WHILE d <= '2035-12-31' LOOP
        INSERT INTO dim_date (
            full_date, year, quarter, month, month_name, day,
            weekday, weekday_name, is_weekend
        )
        VALUES (
            d,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(QUARTER FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            TRIM(TO_CHAR(d, 'Month')),
            EXTRACT(DAY FROM d)::INT,
            EXTRACT(ISODOW FROM d)::INT,
            TRIM(TO_CHAR(d, 'Day')),
            (EXTRACT(ISODOW FROM d)::INT IN (6,7))
        );
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;

-- ===================================
-- FACT: STOCK INDICATORS
-- ===================================
CREATE TABLE fact_stock_indicators (
    record_sk SERIAL PRIMARY KEY,
    stock_sk INT NOT NULL REFERENCES dim_stock(stock_sk) ON DELETE CASCADE,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk) ON DELETE CASCADE,
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

-- ===================================
-- INDEXES
-- ===================================
CREATE INDEX IF NOT EXISTS idx_fact_stock_sk ON fact_stock_indicators(stock_sk);
CREATE INDEX IF NOT EXISTS idx_fact_date_sk ON fact_stock_indicators(date_sk);
-- ===================================
-- DROP TABLES (nếu đã tồn tại)
-- ===================================
DROP TABLE IF EXISTS fact_stock_indicators CASCADE;
DROP TABLE IF EXISTS dim_stock CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_datetime CASCADE;

-- ===================================
-- DIMENSION: STOCK
-- ===================================
CREATE TABLE dim_stock (
    stock_sk SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE
);

-- ===================================
-- DIMENSION: DATE
-- ===================================
CREATE TABLE dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    weekday INT,
    weekday_name VARCHAR(10),
    is_weekend BOOLEAN
);

-- Populate dim_date từ 2010 đến 2035
DO $$
DECLARE
    d DATE := '2010-01-01';
BEGIN
    WHILE d <= '2035-12-31' LOOP
        INSERT INTO dim_date (
            full_date, year, quarter, month, month_name, day,
            weekday, weekday_name, is_weekend
        )
        VALUES (
            d,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(QUARTER FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            TRIM(TO_CHAR(d, 'Month')),
            EXTRACT(DAY FROM d)::INT,
            EXTRACT(ISODOW FROM d)::INT,
            TRIM(TO_CHAR(d, 'Day')),
            (EXTRACT(ISODOW FROM d)::INT IN (6,7))
        );
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;

-- ===================================
-- FACT: STOCK INDICATORS
-- ===================================
CREATE TABLE fact_stock_indicators (
    record_sk SERIAL PRIMARY KEY,
    stock_sk INT NOT NULL REFERENCES dim_stock(stock_sk) ON DELETE CASCADE,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk) ON DELETE CASCADE,
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

-- ===================================
-- INDEXES
-- ===================================
CREATE INDEX IF NOT EXISTS idx_fact_stock_sk ON fact_stock_indicators(stock_sk);
CREATE INDEX IF NOT EXISTS idx_fact_date_sk ON fact_stock_indicators(date_sk);

CREATE OR REPLACE PROCEDURE sp_load_stock_files(
    dim_path VARCHAR,
    fact_path VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_dim INT := 0;
    inserted_fact INT := 0;
    skipped_fact INT := 0;
BEGIN
    -- ============================
    -- 1. Load dim_stock từ CSV
    -- ============================
    CREATE TEMP TABLE tmp_dim_stock (
        id INT,
        ticker VARCHAR(20)
    );

    EXECUTE format(
        'COPY tmp_dim_stock(id, ticker) FROM %L DELIMITER '','' CSV HEADER;',
        dim_path
    );

    -- Insert ticker mới, tránh duplicate
INSERT INTO dim_stock(ticker)
SELECT DISTINCT t.ticker
FROM tmp_dim_stock t
LEFT JOIN dim_stock d ON t.ticker = d.ticker
WHERE d.ticker IS NULL;

GET DIAGNOSTICS inserted_dim = ROW_COUNT;
RAISE NOTICE 'Dim_stock: % bản ghi mới insert', inserted_dim;

    DROP TABLE tmp_dim_stock;

    -- ============================
    -- 2. Load fact_stock_indicators từ CSV
    -- ============================
    CREATE TEMP TABLE tmp_fact_stock (
        record_sk INT,
        stock_sk INT,
        datetime_utc TIMESTAMPTZ,
        close NUMERIC(12,4),
        volume BIGINT,
        diff NUMERIC(12,4),
        percent_change_close NUMERIC(12,6),
        rsi NUMERIC(8,4),
        roc NUMERIC(8,4),
        bb_upper NUMERIC(12,4),
        bb_lower NUMERIC(12,4),
        created_at TIMESTAMP
    );

    EXECUTE format(
        'COPY tmp_fact_stock(
            record_sk, stock_sk, datetime_utc, close, volume, diff,
            percent_change_close, rsi, roc, bb_upper, bb_lower, created_at
        ) FROM %L DELIMITER '','' CSV HEADER;',
        fact_path
    );

    -- Insert vào fact_stock_indicators, chuyển datetime_utc -> date_sk
INSERT INTO fact_stock_indicators(
    stock_sk,
    date_sk,
    close,
    volume,
    diff,
    percent_change_close,
    rsi,
    roc,
    bb_upper,
    bb_lower,
    created_at
)
SELECT 
    f.stock_sk,
    d.date_sk,
    f.close,
    f.volume,
    f.diff,
    f.percent_change_close,
    f.rsi,
    f.roc,
    f.bb_upper,
    f.bb_lower,
    f.created_at
FROM tmp_fact_stock f
JOIN dim_date d ON d.full_date = f.datetime_utc::DATE
LEFT JOIN fact_stock_indicators fi
    ON fi.stock_sk = f.stock_sk AND fi.date_sk = d.date_sk
WHERE fi.record_sk IS NULL;

GET DIAGNOSTICS inserted_fact = ROW_COUNT;
RAISE NOTICE 'Fact_stock_indicators: % bản ghi mới insert', inserted_fact;


    DROP TABLE tmp_fact_stock;

END;
$$;
