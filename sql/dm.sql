CREATE OR REPLACE PROCEDURE sp_build_dm_trend_analysis()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'ETL Trend Analysis via FDW...';

    -- DAILY TREND
    CREATE TABLE IF NOT EXISTS dm.stock_daily_trend (
        ticker VARCHAR(20),
        full_date DATE,
        avg_close NUMERIC,
        max_close NUMERIC,
        min_close NUMERIC,
        total_volume BIGINT,
        avg_rsi NUMERIC,
        avg_roc NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dm.stock_daily_trend;

    INSERT INTO dm.stock_daily_trend
    SELECT 
        ticker, full_date, avg_close, max_close, min_close,
        total_volume, avg_rsi, avg_roc
    FROM dm_dw.agg_daily_stock_summary;


    -- MONTHLY TREND
    CREATE TABLE IF NOT EXISTS dm.stock_monthly_trend (
        ticker VARCHAR(20),
        year INT,
        month INT,
        avg_close NUMERIC,
        total_volume BIGINT,
        avg_rsi NUMERIC,
        avg_roc NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dm.stock_monthly_trend;

    INSERT INTO dm.stock_monthly_trend
    SELECT 
        ticker, year, month,
        avg_close, total_volume,
        avg_rsi, avg_roc
    FROM dm_dw.agg_monthly_stock_summary;

    RAISE NOTICE 'DM Trend Analysis Completed.';
END;
$$;
CREATE OR REPLACE PROCEDURE sp_build_dm_stock_ranking()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'ETL Stock Ranking via FDW...';

    CREATE TABLE IF NOT EXISTS dm.stock_ranking_snapshot (
        ticker VARCHAR(20),
        min_close NUMERIC,
        max_close NUMERIC,
        price_change NUMERIC,
        avg_rsi NUMERIC,
        avg_roc_performance NUMERIC,
        avg_volatility NUMERIC,
        risk_category VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dm.stock_ranking_snapshot;

    INSERT INTO dm.stock_ranking_snapshot (
        ticker, min_close, max_close, price_change,
        avg_rsi, avg_roc_performance, avg_volatility, risk_category
    )
    SELECT
        p.ticker,
        p.min_close,
        p.max_close,
        p.price_change,
        p.avg_rsi,
        p.avg_roc,
        v.avg_volatility,
        CASE 
            WHEN v.avg_volatility > 5 THEN 'High Risk'
            WHEN v.avg_volatility > 2 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END
    FROM dm_dw.agg_stock_performance p
    JOIN dm_dw.agg_top_volatile_stocks v 
        ON p.stock_sk = v.stock_sk;

    RAISE NOTICE 'DM Stock Ranking Completed.';
END;
$$;
CREATE OR REPLACE PROCEDURE sp_build_dm_market_overview()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'ETL Market Overview via FDW...';

    CREATE TABLE IF NOT EXISTS dm.market_liquidity_history (
        full_date DATE PRIMARY KEY,
        total_market_volume BIGINT,
        stocks_traded_count INT,
        volume_moving_avg_7d BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dm.market_liquidity_history;

    INSERT INTO dm.market_liquidity_history
    SELECT
        full_date,
        total_volume,
        num_records,
        AVG(total_volume) OVER (ORDER BY full_date
                                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    FROM dm_dw.agg_volume_by_date;

    RAISE NOTICE 'DM Market Overview Completed.';
END;
$$;
