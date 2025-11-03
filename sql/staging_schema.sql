

DROP TABLE IF EXISTS stg_market_prices CASCADE;
DROP TABLE IF EXISTS stg_transform_market_prices CASCADE;


--Extract
CREATE TABLE stg_market_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    datetime_utc TIMESTAMPTZ NOT NULL,
    close NUMERIC(18,4),
    volume NUMERIC(18,4),
    diff NUMERIC(18,4),
    percent_change_close NUMERIC(10,4),
    source_type VARCHAR(50),
    extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

SELECT 'Schema ETL được khởi tạo lại thành công!' AS status;
--TranSform

CREATE TABLE stg_transform_market_prices  (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    close NUMERIC(18,4),
    volume NUMERIC(18,4),
    diff NUMERIC(18,4),
    percent_change_close NUMERIC(10,4),
    rsi NUMERIC(10,4),
    roc NUMERIC(10,4),
    bb_upper NUMERIC(18,4),
    bb_lower NUMERIC(18,4),
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT 'Transform dữ liệu thành công và lưu vào staging.transform_stock!' AS status;
