import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from utils.service_util import init_services

# ==========================================
# 1️SETUP
# ==========================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


# ==========================================
# ==========================================
def load_to_dw(staging_engine, dw_engine):
    """Load dữ liệu từ staging.transform_stock vào DW."""
    start_time = time.time()

    # Đọc dữ liệu staging
    query = "SELECT * FROM staging.transform_stock"
    df = pd.read_sql(query, staging_engine)
    if df.empty:
        raise ValueError("Bảng staging.transform_stock không có dữ liệu để load.")

    logging.info(f"Đọc {len(df)} dòng từ staging.transform_stock")

    with dw_engine.begin() as conn:
        # ==========================================
        # DIM_STOCK
        # ==========================================
        tickers = df["ticker"].unique().tolist()
        for ticker in tickers:
            conn.execute(
                text("""
                    INSERT INTO dim_stock (ticker)
                    VALUES (:ticker)
                    ON CONFLICT (ticker) DO NOTHING
                """),
                {"ticker": ticker},
            )

        # ==========================================
        # DIM_DATETIME
        # ==========================================
        dates = df["Date"].unique().tolist()
        for date in dates:
            conn.execute(
                text("""
                    INSERT INTO dim_datetime (date, year, month, day, weekday)
                    VALUES (:date, EXTRACT(YEAR FROM :date), EXTRACT(MONTH FROM :date), 
                            EXTRACT(DAY FROM :date), TO_CHAR(:date, 'Day'))
                    ON CONFLICT (date) DO NOTHING
                """),
                {"date": date},
            )

        # ==========================================
        # FACT_STOCK_INDICATORS
        # ==========================================
        insert_sql = text("""
            INSERT INTO fact_stock_indicators (
                stock_id, datetime_id, close, volume, diff, percent_change_close, 
                rsi, roc, bb_upper, bb_lower
            )
            SELECT 
                s.stock_id,
                d.datetime_id,
                t."Close" AS close,
                t."Volume" AS volume,
                t."Diff" AS diff,
                t."Percent_Change_Close" AS percent_change_close,
                t."RSI" AS rsi,
                t."ROC" AS roc,
                t."BB_Upper" AS bb_upper,
                t."BB_Lower" AS bb_lower
            FROM staging.transform_stock t
            JOIN dim_stock s ON s.ticker = t."Ticker"
            JOIN dim_datetime d ON d.date = t."Date";
        """)
        conn.execute(insert_sql)

    duration = round(time.time() - start_time, 2)
    logging.info(f"Load DW thành công ({len(df)} bản ghi, {duration}s)")
    return len(df), duration


# ==========================================
# 4️MAIN PROCESS
# ==========================================
def main():
    logging.info("=== Bắt đầu LOAD DW ===")
    services = init_services(['config_load_db', 'log_db', 'email_service', 'staging_engine', 'dw_engine'])
    config_db = services['config_load_db']
    log_db = services['log_db']
    email_service = services['email_service']
    staging_engine = services['staging_engine']
    dw_engine = services['dw_engine']

    try:
        configs = config_db.get_active_configs()
        if not configs:
            logging.warning("Không có config load DW nào đang active.")
            return

        for cfg in configs:
            config_id = cfg["id"]
            try:
                log_db.insert_log("LOAD_DW", config_id, "PROCESSING", "Bắt đầu load DW.")
                config_db.mark_config_status(config_id, "PROCESSING")

                rows, duration = load_to_dw(staging_engine, dw_engine)

                msg = f"Đã load {rows} bản ghi vào DW trong {duration}s."
                log_db.insert_log("LOAD_DW", config_id, "SUCCESS", msg)
                config_db.mark_config_status(config_id, "SUCCESS")

            except Exception as e:
                logging.error(f"Lỗi LOAD DW (Config ID={config_id}): {e}")
                log_db.insert_log("LOAD_DW", config_id, "FAILURE", str(e))
                config_db.mark_config_status(config_id, "FAILURE")

                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL LOAD DW] Lỗi Config ID={config_id}",
                    body=f"Đã xảy ra lỗi khi load dữ liệu vào DW:\n\n{e}",
                )

    except Exception as e:
        logging.error(f"Lỗi tổng thể trong LOAD DW main(): {e}")

    finally:
        config_db.close()
        log_db.close()
        staging_engine.dispose()
        dw_engine.dispose()
        logging.info("🏁 Kết thúc LOAD DW.")


# ==========================================
# 5️ENTRY POINT
# ==========================================
if __name__ == "__main__":
    main()
