
-- Drop các object cũ
DROP PROCEDURE IF EXISTS etl_load_fact_price_daily;
DROP PROCEDURE IF EXISTS sp_refresh_dm_daily_volatility;
DROP PROCEDURE IF EXISTS sp_refresh_dm_monthly_stock_summary;
DROP MATERIALIZED VIEW IF EXISTS agg_daily_volatility CASCADE;
DROP MATERIALIZED VIEW IF EXISTS agg_monthly_stock_summary CASCADE;
DROP TABLE IF EXISTS fact_price_daily CASCADE;
DROP TABLE IF EXISTS dm_daily_volatility CASCADE;
DROP TABLE IF EXISTS dm_monthly_stock_summary CASCADE;
DROP INDEX IF EXISTS idx_agg_daily_volatility;
-- ===================================
-- 2. FACT: PRICE DAILY (Data Mart)
-- ===================================
CREATE TABLE fact_price_daily (
    date_key INT NOT NULL,
    stock_key INT NOT NULL,
    full_date DATE,
    close NUMERIC,
    volume BIGINT,
    change NUMERIC,
    change_percent NUMERIC,
    rsi NUMERIC,
    roc NUMERIC,
    PRIMARY KEY (date_key, stock_key)
);

CREATE OR REPLACE PROCEDURE etl_load_fact_price_daily()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Sử dụng INSERT ... ON CONFLICT để xử lý việc chạy lại không bị lỗi
    INSERT INTO fact_price_daily (
        date_key,
        stock_key,
        full_date,
        close,
        volume,
        change,
        change_percent,
        rsi,
        roc
    )
    SELECT DISTINCT ON (f.date_sk, f.stock_sk) -- Đảm bảo chỉ lấy 1 dòng duy nhất cho mỗi ngày/mã
        f.date_sk,
        f.stock_sk,
        d.full_date,
        f.close,
        f.volume,
        f.diff AS change,
        f.percent_change_close AS change_percent,
        f.rsi,
        f.roc
    FROM fact_stock_indicators f
    JOIN dim_date d ON f.date_sk = d.date_sk
    WHERE f.close IS NOT NULL
    ORDER BY f.date_sk, f.stock_sk, f.record_sk DESC -- Ưu tiên lấy bản ghi mới nhất nếu có trùng
    ON CONFLICT (date_key, stock_key) 
    DO UPDATE SET
        close          = EXCLUDED.close,
        volume         = EXCLUDED.volume,
        change         = EXCLUDED.change,
        change_percent = EXCLUDED.change_percent,
        rsi            = EXCLUDED.rsi,
        roc            = EXCLUDED.roc;

    RAISE NOTICE 'Đã cập nhật dữ liệu vào bảng fact_price_daily thành công.';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Lỗi trong quá trình chạy Procedure etl_load_fact_price_daily: %', SQLERRM;
END;
$$;


-- ===================================
-- 3. DIMENSION: MONTHLY MARKET TREND MART
-- ===================================
-- Tạo Materialized View để tính toán trước dữ liệu Monthly
CREATE MATERIALIZED VIEW agg_monthly_stock_summary AS
SELECT 
    f.stock_sk,
    ds.ticker,
    dd.year,
    dd.month,
    AVG(f.close) AS avg_close,
    SUM(f.volume) AS total_volume,
    AVG(f.rsi) AS avg_rsi,
    AVG(f.roc) AS avg_roc
FROM fact_stock_indicators f
JOIN dim_date dd ON f.date_sk = dd.date_sk
JOIN dim_stock ds ON f.stock_sk = ds.stock_sk
GROUP BY f.stock_sk, ds.ticker, dd.year, dd.month
WITH DATA;

CREATE UNIQUE INDEX idx_agg_monthly ON agg_monthly_stock_summary(stock_sk, year, month);

-- Tạo bảng Data Mart Monthly
CREATE TABLE IF NOT EXISTS dm_monthly_stock_summary (
    month_sk SERIAL PRIMARY KEY,
    stock_sk INT NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_label TEXT GENERATED ALWAYS AS (year || '-' || LPAD(month::TEXT, 2, '0')) STORED,
    avg_close NUMERIC(18,6),
    total_volume BIGINT,
    avg_rsi NUMERIC(18,6),
    avg_roc NUMERIC(18,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_monthly_sk_ym
ON dm_monthly_stock_summary(stock_sk, year, month);

CREATE OR REPLACE PROCEDURE sp_refresh_dm_monthly_stock_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing materialized view: agg_monthly_stock_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_monthly_stock_summary;

    RAISE NOTICE 'Loading data mart: dm_monthly_stock_summary...';
    INSERT INTO dm_monthly_stock_summary (
        stock_sk, ticker, year, month,
        avg_close, total_volume, avg_rsi, avg_roc
    )
    SELECT 
        stock_sk, ticker, year, month,
        avg_close, total_volume, avg_rsi, avg_roc
    FROM agg_monthly_stock_summary
    ON CONFLICT (stock_sk, year, month)
    DO UPDATE SET
        avg_close     = EXCLUDED.avg_close,
        total_volume  = EXCLUDED.total_volume,
        avg_rsi       = EXCLUDED.avg_rsi,
        avg_roc       = EXCLUDED.avg_roc,
        created_at    = CURRENT_TIMESTAMP;

    RAISE NOTICE 'dm_monthly_stock_summary refresh completed.';
END;
$$;


-- ===================================
-- 4. DIMENSION: VOLATILITY MART
-- ===================================
CREATE TABLE IF NOT EXISTS dm_daily_volatility (
    date_key INT NOT NULL,
    stock_sk INT NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    daily_volatility NUMERIC(18,6),
    stddev_5d NUMERIC(18,6),
    stddev_20d NUMERIC(18,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, stock_sk)
);

-- Tạo Materialized View
CREATE MATERIALIZED VIEW agg_daily_volatility AS
SELECT
    f.date_sk AS date_key,
    f.stock_sk,
    d.ticker,

    -- daily volatility
    f.percent_change_close AS daily_volatility,

    -- Std Dev 5 days
    STDDEV_SAMP(f.close) OVER (
        PARTITION BY f.stock_sk ORDER BY f.date_sk ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS stddev_5d,

    -- Std Dev 20 days
    STDDEV_SAMP(f.close) OVER (
        PARTITION BY f.stock_sk ORDER BY f.date_sk ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS stddev_20d

FROM fact_stock_indicators f
JOIN dim_stock d ON d.stock_sk = f.stock_sk
ORDER BY f.date_sk, f.stock_sk
WITH DATA;

-- Index này bây giờ sẽ chạy an toàn vì dữ liệu nguồn đã được clean ở bước 1
CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_daily_volatility
ON agg_daily_volatility(stock_sk, date_key);


CREATE OR REPLACE PROCEDURE sp_refresh_dm_daily_volatility()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing agg_daily_volatility...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY agg_daily_volatility;

    RAISE NOTICE 'Loading Data Mart dm_daily_volatility...';
    INSERT INTO dm_daily_volatility (
        date_key, stock_sk, ticker,
        daily_volatility,
        stddev_5d, stddev_20d
    )
    SELECT
        date_key, stock_sk, ticker,
        daily_volatility,
        stddev_5d, stddev_20d
    FROM agg_daily_volatility
    ON CONFLICT (date_key, stock_sk)
    DO UPDATE SET
        daily_volatility = EXCLUDED.daily_volatility,
        stddev_5d        = EXCLUDED.stddev_5d,
        stddev_20d       = EXCLUDED.stddev_20d,
        created_at       = CURRENT_TIMESTAMP;

    RAISE NOTICE 'dm_daily_volatility refresh completed.';
END;
$$;