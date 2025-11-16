-- ===================================
-- DROP TABLES (nếu đã tồn tại)
-- ===================================
DROP TABLE IF EXISTS fact_price_daily CASCADE;

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
        dd.date_sk,
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
    JOIN dim_date dd ON f.date_sk = dd.date_sk
    -- Nếu muốn join dim_datetime để lấy chi tiết thời gian:
    -- JOIN dim_datetime dt ON f.datetime_utc = dt.datetime_utc
    ;

END;
$$;


-- ===================================
-- DIMENSION: MONTHLY MARKET TREND MART
-- ===================================
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

INSERT INTO dm_monthly_stock_summary (
    stock_sk, ticker, year, month,
    avg_close, total_volume, avg_rsi, avg_roc
)
SELECT 
    stock_sk,
    ticker,
    year,
    month,
    avg_close,
    total_volume,
    avg_rsi,
    avg_roc
FROM agg_monthly_stock_summary
ON CONFLICT (stock_sk, year, month)
DO UPDATE SET
    avg_close     = EXCLUDED.avg_close,
    total_volume  = EXCLUDED.total_volume,
    avg_rsi       = EXCLUDED.avg_rsi,
    avg_roc       = EXCLUDED.avg_roc,
    created_at    = CURRENT_TIMESTAMP;

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
-- DIMENSION: VOLATILITY MART
-- ===================================
CREATE TABLE IF NOT EXISTS dm_daily_volatility (
    date_key INT NOT NULL,
    stock_sk INT NOT NULL,
    ticker VARCHAR(20) NOT NULL,

    -- Biến động close-to-close
    daily_volatility NUMERIC(18,6),

    -- True Range (giá trị lớn nhất trong: high-low, high-prev_close, low-prev_close)
    true_range NUMERIC(18,6),

    -- ATR 14 ngày
    atr_14 NUMERIC(18,6),

    -- Độ lệch chuẩn (biến động thống kê)
    stddev_5d NUMERIC(18,6),
    stddev_20d NUMERIC(18,6),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (date_key, stock_sk)
);

DROP MATERIALIZED VIEW IF EXISTS agg_daily_volatility;

CREATE MATERIALIZED VIEW agg_daily_volatility AS
SELECT
    f.date_sk AS date_key,
    f.stock_sk,
    d.ticker,

    -- daily volatility (close-to-close percent change)
    CASE 
        WHEN LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk) IS NULL 
        THEN NULL
        ELSE (f.close - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk))
             / LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk)
    END AS daily_volatility,

    -- True Range
    GREATEST(
        f.high_price - f.low_price,
        ABS(f.high_price - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk)),
        ABS(f.low_price  - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk))
    ) AS true_range,

    -- ATR 14
    AVG(
        GREATEST(
            f.high_price - f.low_price,
            ABS(f.high_price - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk)),
            ABS(f.low_price  - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk))
        )
    ) OVER (PARTITION BY f.stock_sk ORDER BY f.date_sk ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
    AS atr_14,

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
        daily_volatility, true_range, atr_14,
        stddev_5d, stddev_20d
    )
    SELECT
        date_key,
        stock_sk,
        ticker,
        daily_volatility,
        true_range,
        atr_14,
        stddev_5d,
        stddev_20d
    FROM agg_daily_volatility
    ON CONFLICT (date_key, stock_sk)
    DO UPDATE SET
        daily_volatility = EXCLUDED.daily_volatility,
        true_range       = EXCLUDED.true_range,
        atr_14           = EXCLUDED.atr_14,
        stddev_5d        = EXCLUDED.stddev_5d,
        stddev_20d       = EXCLUDED.stddev_20d,
        created_at       = CURRENT_TIMESTAMP;

    RAISE NOTICE 'dm_daily_volatility refresh completed.';
END;
$$;