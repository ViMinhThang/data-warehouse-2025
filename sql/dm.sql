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

CREATE OR REPLACE PROCEDURE sp_fact_price_daily_from_dw()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ===============================
    -- Insert dữ liệu vào fact_price_daily từ DW
    -- ===============================
    INSERT INTO fact_price_daily (
        date_key, stock_key, open, high, low, close, volume, change, change_percent
    )
    SELECT 
        dd.date_key,
        f.stock_sk AS stock_key,
        f.open_price AS open,
        f.high_price AS high,
        f.low_price AS low,
        f.close AS close,
        f.volume,
        f.close - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY dd.date) AS change,
        CASE 
            WHEN LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY dd.date) IS NULL THEN NULL
            ELSE ((f.close - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY dd.date)) / 
                  LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY dd.date)) * 100
        END AS change_percent
    FROM fact_stock_indicators f
    JOIN dim_date dd ON DATE(f.datetime_utc) = dd.date
    -- Nếu muốn join dim_datetime để lấy chi tiết thời gian:
    -- JOIN dim_datetime dt ON f.datetime_utc = dt.datetime_utc
    ;

END;
$$;