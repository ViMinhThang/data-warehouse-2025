-- =========================================
-- DM Trend Analysis (Daily & Monthly)
-- =========================================
CREATE OR REPLACE PROCEDURE sp_build_dm_trend_analysis()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'ETL Trend Analysis via FDW...';

    -- DAILY TREND
    CREATE TABLE IF NOT EXISTS stock_daily_trend (
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

    -- Ensure primary key exists
    DO $_$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            WHERE c.relname = 'stock_daily_trend' AND i.indisprimary
        ) THEN
            ALTER TABLE stock_daily_trend
                ADD CONSTRAINT stock_daily_trend_pk PRIMARY KEY(ticker, full_date);
        END IF;
    END $_$;

    INSERT INTO stock_daily_trend(ticker, full_date, avg_close, max_close, min_close, total_volume, avg_rsi, avg_roc)
    SELECT ticker, full_date, avg_close, max_close, min_close,
           total_volume, avg_rsi, avg_roc
    FROM dm_dw.agg_daily_stock_summary
    ON CONFLICT (ticker, full_date) DO NOTHING;

    -- MONTHLY TREND
    CREATE TABLE IF NOT EXISTS stock_monthly_trend (
        ticker VARCHAR(20),
        year INT,
        month INT,
        avg_close NUMERIC,
        total_volume BIGINT,
        avg_rsi NUMERIC,
        avg_roc NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Ensure primary key exists
    DO $_$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            WHERE c.relname = 'stock_monthly_trend' AND i.indisprimary
        ) THEN
            ALTER TABLE stock_monthly_trend
                ADD CONSTRAINT stock_monthly_trend_pk PRIMARY KEY(ticker, year, month);
        END IF;
    END $_$;

    INSERT INTO stock_monthly_trend(ticker, year, month, avg_close, total_volume, avg_rsi, avg_roc)
    SELECT ticker, year, month,
           avg_close, total_volume,
           avg_rsi, avg_roc
    FROM dm_dw.agg_monthly_stock_summary
    ON CONFLICT (ticker, year, month) DO NOTHING;

    RAISE NOTICE 'DM Trend Analysis Completed.';
END;
$$;

-- =========================================
-- DM Stock Ranking
-- =========================================
CREATE OR REPLACE PROCEDURE sp_build_dm_stock_ranking()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'ETL Stock Ranking via FDW...';

    CREATE TABLE IF NOT EXISTS stock_ranking_snapshot (
        ticker VARCHAR(20),
        snapshot_date DATE DEFAULT CURRENT_DATE,
        min_close NUMERIC,
        max_close NUMERIC,
        price_change NUMERIC,
        avg_rsi NUMERIC,
        avg_roc_performance NUMERIC,
        avg_volatility NUMERIC,
        risk_category VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Ensure primary key exists
    DO $_$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            WHERE c.relname = 'stock_ranking_snapshot' AND i.indisprimary
        ) THEN
            ALTER TABLE stock_ranking_snapshot
                ADD CONSTRAINT stock_ranking_snapshot_pk PRIMARY KEY(ticker, snapshot_date);
        END IF;
    END $_$;

    INSERT INTO stock_ranking_snapshot(
        ticker, snapshot_date, min_close, max_close, price_change, 
        avg_rsi, avg_roc_performance, avg_volatility, risk_category
    )
    SELECT
        p.ticker,
        CURRENT_DATE,
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
        ON p.stock_sk = v.stock_sk
    ON CONFLICT (ticker, snapshot_date) DO NOTHING;

    RAISE NOTICE 'DM Stock Ranking Completed.';
END;
$$;

-- =========================================
-- DM Market Overview
-- =========================================
CREATE OR REPLACE PROCEDURE sp_build_dm_market_overview()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'ETL Market Overview via FDW...';

    CREATE TABLE IF NOT EXISTS market_liquidity_history (
        full_date DATE PRIMARY KEY,
        total_market_volume BIGINT,
        stocks_traded_count INT,
        volume_moving_avg_7d BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO market_liquidity_history(full_date, total_market_volume, stocks_traded_count, volume_moving_avg_7d)
    SELECT
        full_date,
        total_volume,
        num_records,
        AVG(total_volume) OVER (
            ORDER BY full_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::BIGINT AS volume_moving_avg_7d
    FROM dm_dw.agg_volume_by_date
    ON CONFLICT (full_date) DO NOTHING;

    RAISE NOTICE 'DM Market Overview Completed.';
END;
$$;
