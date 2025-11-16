import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_DB_URL = os.getenv("WAREHOUSE_DB_URL")
DM_DB_URL = os.getenv("DM_DB_URL")

engine_wh = create_engine(WAREHOUSE_DB_URL)
engine_dm = create_engine(DM_DB_URL)

# 1. Load FACT PRICE DAILY (tính change + change_percent)
def load_fact_price_daily():
    sql = """
        SELECT
            fpi.date_key,
            fpi.stock_key,
            fpi.open,
            fpi.high,
            fpi.low,
            fpi.close,
            fpi.volume,
            d.dt,
            s.ticker
        FROM dw.fact_price_indicators fpi
        JOIN dw.dim_date d ON fpi.date_key = d.date_key
        JOIN dw.dim_stock s ON fpi.stock_key = s.stock_key
        ORDER BY fpi.stock_key, d.dt
    """

    df = pd.read_sql(sql, engine_wh, parse_dates=["dt"])

    if df.empty:
        print("No data in warehouse.fact_price.")
        return

    # Tính cột change và change_percent cho từng stock
    df["change"] = df.groupby("stock_key")["close"].diff()
    df["change_percent"] = df.groupby("stock_key")["close"].pct_change() * 100

    # Chọn đúng cột mart
    df_mart = df[[
        "date_key",
        "stock_key",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "change",
        "change_percent"
    ]]

    df_mart.to_sql("fact_price_daily", engine_dm, schema="dm", if_exists="replace", index=False)
    print("[OK] fact_price_daily loaded.")


if __name__ == "__main__":
    load_fact_price_daily()
    print("=== Data Mart Load Completed ===")