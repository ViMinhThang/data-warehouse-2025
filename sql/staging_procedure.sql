CREATE OR REPLACE PROCEDURE sp_transform_market_prices(
    p_rsi_window INT,
    p_roc_window INT,
    p_bb_window INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Cập nhật dim_stock
    INSERT INTO dim_stock (ticker)
    SELECT DISTINCT ticker
    FROM stg_market_prices
    ON CONFLICT (ticker) DO NOTHING;

    -- CTE: join stock, tính toán ROC, RSI, BB
    WITH sp AS (
        SELECT s.*,
               ds.stock_sk
        FROM stg_market_prices s
        JOIN dim_stock ds ON ds.ticker = s.ticker
    ),
    roc_calc AS (
        SELECT *, LAG(close, p_roc_window) OVER (PARTITION BY ticker ORDER BY datetime_utc) AS close_n
        FROM sp
    ),
    bb_calc AS (
        SELECT *,
               AVG(close) OVER (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN p_bb_window-1 PRECEDING AND CURRENT ROW) AS ma,
               STDDEV(close) OVER (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN p_bb_window-1 PRECEDING AND CURRENT ROW) AS std
        FROM roc_calc
    ),
    rsi_calc AS (
        SELECT *, SUM(CASE WHEN change>0 THEN change ELSE 0 END) OVER w AS gain_sum,
                  SUM(CASE WHEN change<0 THEN -change ELSE 0 END) OVER w AS loss_sum
        FROM (
            SELECT *, close - LAG(close) OVER (PARTITION BY ticker ORDER BY datetime_utc) AS change
            FROM bb_calc
        ) t
        WINDOW w AS (PARTITION BY ticker ORDER BY datetime_utc ROWS BETWEEN p_rsi_window-1 PRECEDING AND CURRENT ROW)
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
        stock_sk, close, volume, diff, percent_change_close,
        rsi, roc, bb_upper, bb_lower, created_at, datetime_utc
    )
    SELECT stock_sk, close, volume::BIGINT, diff, percent_change_close,
           rsi, roc, bb_upper, bb_lower, CURRENT_TIMESTAMP, datetime_utc
    FROM final_calc
    WHERE close IS NOT NULL AND volume IS NOT NULL
          AND rsi IS NOT NULL AND roc IS NOT NULL
          AND bb_upper IS NOT NULL AND bb_lower IS NOT NULL;


    -- Xóa dữ liệu staging
    TRUNCATE TABLE stg_market_prices;

END;
$$;
