-- ===================================
-- DROP TABLES (nếu đã tồn tại)
-- ===================================
DROP TABLE IF EXISTS fact_price_daily CASCADE;
-- DROP TABLE IF EXISTS fact_order_book CASCADE;
-- DROP TABLE IF EXISTS fact_trading_liquidity CASCADE;

-- DROP TABLE IF EXISTS dim_symbol CASCADE;
-- DROP TABLE IF EXISTS dim_company CASCADE;
-- DROP TABLE IF EXISTS dim_exchange CASCADE;
-- DROP TABLE IF EXISTS dim_calendar CASCADE;

-- ===================================
-- FACT: PRICE DAILY
-- ===================================
CREATE TABLE fact_price_daily (
    date_key INT NOT NULL,
    stock_key INT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    change NUMERIC,
    change_percent NUMERIC,
    PRIMARY KEY (date_key, stock_key)
);