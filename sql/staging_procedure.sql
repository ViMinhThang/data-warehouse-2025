CREATE OR REPLACE PROCEDURE sp_transform_market_prices()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rsi_window INT;
    v_roc_window INT;
    v_bb_window INT;
BEGIN
    -- Lấy cấu hình từ config database
    SELECT rsi_window, roc_window, bb_window
    INTO v_rsi_window, v_roc_window, v_bb_window
    FROM dblink(
        'dbname=config user=postgres password=204863 host=localhost port=5432',
        'SELECT rsi_window, roc_window, bb_window
         FROM config_transform
         WHERE is_active = TRUE
         ORDER BY id DESC
         LIMIT 1'
    ) AS t(rsi_window INT, roc_window INT, bb_window INT);

    -- Insert dim_stock
    INSERT INTO dim_stock (ticker)
    SELECT DISTINCT ticker
    FROM stg_market_prices
    ON CONFLICT (ticker) DO NOTHING;

    -- Insert dim_datetime
    INSERT INTO dim_datetime (date, year, month, day, hour, weekday)
    SELECT DISTINCT
        date_trunc('hour', datetime_utc) AS date,
        EXTRACT(YEAR FROM datetime_utc)::INT AS year,
        EXTRACT(MONTH FROM datetime_utc)::INT AS month,
        EXTRACT(DAY FROM datetime_utc)::INT AS day,
        EXTRACT(HOUR FROM datetime_utc)::INT AS hour,
        TO_CHAR(datetime_utc, 'Day') AS weekday
    FROM stg_market_prices
    ON CONFLICT (date) DO NOTHING;

    -- Tính các indicator và insert vào fact_stock_indicators
    WITH sp AS (
        SELECT s.*, ds.stock_id, dd.datetime_id
        FROM stg_market_prices s
        JOIN dim_stock ds ON ds.ticker = s.ticker
        JOIN dim_datetime dd ON dd.date = date_trunc('hour', s.datetime_utc)
    ),
    roc_calc AS (
        SELECT *, LAG(close, v_roc_window) OVER (PARTITION BY ticker ORDER BY datetime_utc) AS close_n
        FROM sp
    ),
    bb_calc AS (
        SELECT *, AVG(close) OVER (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN v_bb_window-1 PRECEDING AND CURRENT ROW) AS ma,
                 STDDEV(close) OVER (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN v_bb_window-1 PRECEDING AND CURRENT ROW) AS std
        FROM roc_calc
    ),
    rsi_calc AS (
        SELECT *, SUM(CASE WHEN change>0 THEN change ELSE 0 END) OVER w AS gain_sum,
                  SUM(CASE WHEN change<0 THEN -change ELSE 0 END) OVER w AS loss_sum
        FROM (
            SELECT *, close - LAG(close) OVER (PARTITION BY ticker ORDER BY datetime_utc) AS change
            FROM bb_calc
        ) t
        WINDOW w AS (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN v_rsi_window-1 PRECEDING AND CURRENT ROW)
    ),
    final_calc AS (
        SELECT *,
            CASE WHEN gain_sum IS NULL OR loss_sum IS NULL THEN NULL
                 WHEN loss_sum=0 THEN 100
                 ELSE 100 - (100 / (1 + gain_sum/loss_sum))
            END AS rsi,
            CASE WHEN close_n IS NULL THEN NULL ELSE ((close - close_n)/close_n)*100 END AS roc,
            CASE WHEN ma IS NOT NULL AND std IS NOT NULL THEN ma + 2*std ELSE NULL END AS bb_upper,
            CASE WHEN ma IS NOT NULL AND std IS NOT NULL THEN ma - 2*std ELSE NULL END AS bb_lower
        FROM rsi_calc
    )
    INSERT INTO fact_stock_indicators (
        stock_id, datetime_id, close, volume, diff, percent_change_close,
        rsi, roc, bb_upper, bb_lower, created_at
    )
    SELECT stock_id, datetime_id, close, volume::BIGINT, diff, percent_change_close,
           rsi, roc, bb_upper, bb_lower, CURRENT_TIMESTAMP
    FROM final_calc
    WHERE close IS NOT NULL AND volume IS NOT NULL
          AND rsi IS NOT NULL AND roc IS NOT NULL
          AND bb_upper IS NOT NULL AND bb_lower IS NOT NULL;

    -- Truncate stg_market_prices sau khi transform
    TRUNCATE TABLE stg_market_prices;

    RAISE NOTICE 'Transform thành công, stg_market_prices đã truncate.';
END;
$$;
