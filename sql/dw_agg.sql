\c dw;

-- ============================================
-- AGGREGATE MATERIALIZED VIEWS FOR STOCK DW
-- ============================================

-- ===========================
-- 1. Daily Stock Summary
-- ===========================
DROP MATERIALIZED VIEW IF EXISTS agg_daily_stock_summary;

CREATE MATERIALIZED VIEW agg_daily_stock_summary AS
SELECT
    fs.stock_sk,
    ds.ticker,
    d.full_date,
    AVG(fs.close) AS avg_close,
    MAX(fs.close) AS max_close,
    MIN(fs.close) AS min_close,
    SUM(fs.volume) AS total_volume,
    AVG(fs.rsi) AS avg_rsi,
    AVG(fs.roc) AS avg_roc
FROM fact_stock_indicators fs
JOIN dim_stock ds ON ds.stock_sk = fs.stock_sk
JOIN dim_date d ON d.date_sk = fs.date_sk
GROUP BY fs.stock_sk, ds.ticker, d.full_date
ORDER BY d.full_date, ds.ticker
WITH DATA;


-- Index cho Daily Stock Summary
CREATE UNIQUE INDEX idx_agg_daily_ticker_date ON agg_daily_stock_summary(ticker, full_date);

-- ===========================
-- 2. Monthly Stock Summary
-- ===========================
DROP MATERIALIZED VIEW IF EXISTS agg_monthly_stock_summary;

CREATE MATERIALIZED VIEW agg_monthly_stock_summary
SELECT
    fs.stock_sk,
    ds.ticker,
    d.year,
    d.month,
    AVG(fs.close) AS avg_close,
    SUM(fs.volume) AS total_volume,
    AVG(fs.rsi) AS avg_rsi,
    AVG(fs.roc) AS avg_roc
FROM fact_stock_indicators fs
JOIN dim_stock ds ON ds.stock_sk = fs.stock_sk
JOIN dim_date d ON d.date_sk = fs.date_sk
GROUP BY fs.stock_sk, ds.ticker, d.year, d.month
ORDER BY d.year, d.month, ds.ticker;
WITH DATA;
-- Index cho Monthly Stock Summary
CREATE UNIQUE  INDEX idx_agg_monthly_ticker_month ON agg_monthly_stock_summary(ticker, year, month);

-- ===========================
-- 3. Top Volatile Stocks
-- ===========================
DROP MATERIALIZED VIEW IF EXISTS agg_top_volatile_stocks;

CREATE MATERIALIZED VIEW agg_top_volatile_stocks
SELECT
    fs.stock_sk,
    ds.ticker,
    AVG(ABS(fs.roc)) AS avg_volatility
FROM fact_stock_indicators fs
JOIN dim_stock ds ON ds.stock_sk = fs.stock_sk
GROUP BY fs.stock_sk, ds.ticker
ORDER BY avg_volatility DESC;
WITH DATA;

CREATE UNIQUE INDEX idx_agg_top_volatility ON agg_top_volatile_stocks(avg_volatility DESC);

-- ===========================
-- 4. Volume by Date
-- ===========================
DROP MATERIALIZED VIEW IF EXISTS agg_volume_by_date;

CREATE MATERIALIZED VIEW agg_volume_by_date
SELECT
    d.full_date,
    SUM(fs.volume) AS total_volume,
    COUNT(*) AS num_records
FROM fact_stock_indicators fs
JOIN dim_date d ON d.date_sk = fs.date_sk
GROUP BY d.full_date
ORDER BY d.full_date;
WITH DATA;

CREATE UNIQUE INDEX idx_agg_volume_date ON agg_volume_by_date(full_date);

-- ===========================
-- 5. Stock Performance Summary
-- ===========================
DROP MATERIALIZED VIEW IF EXISTS agg_stock_performance;

CREATE MATERIALIZED VIEW agg_stock_performance
SELECT
    fs.stock_sk,
    ds.ticker,
    MIN(fs.close) AS min_close,
    MAX(fs.close) AS max_close,
    (MAX(fs.close) - MIN(fs.close)) AS price_change,
    AVG(fs.rsi) AS avg_rsi,
    AVG(fs.roc) AS avg_roc
FROM fact_stock_indicators fs
JOIN dim_stock ds ON ds.stock_sk = fs.stock_sk
GROUP BY fs.stock_sk, ds.ticker
ORDER BY price_change DESC;
WITH DATA;

CREATE UNIQUE INDEX idx_agg_stock_perf ON agg_stock_performance(stock_sk, ticker);

-- ============================================
-- REFRESH ALL AGGREGATES PROCEDURE
-- ============================================
CREATE OR REPLACE PROCEDURE sp_refresh_all_aggregates()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing agg_daily_stock_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_daily_stock_summary;

    RAISE NOTICE 'Refreshing agg_monthly_stock_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_monthly_stock_summary;

    RAISE NOTICE 'Refreshing agg_top_volatile_stocks...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_top_volatile_stocks;

    RAISE NOTICE 'Refreshing agg_volume_by_date...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_volume_by_date;

    RAISE NOTICE 'Refreshing agg_stock_performance...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_stock_performance;

    RAISE NOTICE 'All aggregates refreshed successfully.';
END;
$$;
