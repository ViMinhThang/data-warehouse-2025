import os
from dotenv import load_dotenv

from utils.service_util import init_services
from utils.logger_util import log_message

load_dotenv()

# =====================================================
#  HÀM 1 — LOAD fact_price_daily (DW → DM)
# =====================================================
def load_fact_price_daily(config, dw_db, dm_db, log_db):
    config_id = config["id"]
    try:
        log_message(log_db, "LOAD_DM", config_id, "PROCESSING",
                    message="Bắt đầu load fact_price_daily...")

        query = """
            SELECT 
                f.stock_sk,
                f.datetime_utc,
                f.open_price AS open,
                f.high_price AS high,
                f.low_price AS low,
                f.close AS close,
                f.volume,
                f.close - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.datetime_utc) AS change,
                CASE 
                    WHEN LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.datetime_utc) IS NULL THEN NULL
                    ELSE ((f.close - LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.datetime_utc)) / 
                          LAG(f.close) OVER (PARTITION BY f.stock_sk ORDER BY f.datetime_utc)) * 100
                END AS change_percent,
                EXTRACT(YEAR FROM f.datetime_utc)::INT * 10000 +
                EXTRACT(MONTH FROM f.datetime_utc)::INT * 100 +
                EXTRACT(DAY FROM f.datetime_utc)::INT AS date_key
            FROM fact_stock_indicators f
            WHERE f.close IS NOT NULL AND f.volume IS NOT NULL
        """

        df = dw_db.read_sql(query)
        if df.empty:
            raise ValueError("Không có dữ liệu DW để load sang fact_price_daily.")

        dm_db.copy_from_dataframe(df, "fact_price_daily")

        log_message(log_db, "LOAD_DM", config_id, "SUCCESS",
                    f"Load thành công {len(df)} bản ghi vào fact_price_daily.")
        return True

    except Exception as e:
        log_message(log_db, "LOAD_DM", config_id, "FAILURE",
                    f"Lỗi load_fact_price_daily(): {e}")
        raise

# =====================================================
#  HÀM 2 — LOAD/REFRESH dm_monthly_stock_summary
# =====================================================
def load_dm_monthly_stock_summary(config, dw_db, dm_db, log_db):
    config_id = config["id"]

    try:
        log_message(log_db, "LOAD_DM", config_id, "PROCESSING",
                    message="Refresh dm_monthly_stock_summary...")

        # Gọi procedure SQL mà bạn đã tạo:
        dm_db.execute("CALL sp_refresh_dm_monthly_stock_summary();")

        log_message(log_db, "LOAD_DM", config_id, "SUCCESS",
                    "Refresh dm_monthly_stock_summary thành công.")

        return True

    except Exception as e:
        log_message(log_db, "LOAD_DM", config_id, "FAILURE",
                    f"Lỗi load_dm_monthly_stock_summary(): {e}")
        raise

# =====================================================
#  HÀM 3 — LOAD/REFRESH dm_daily_volatility
# =====================================================
def load_dm_daily_volatility(dm_db, log_db, config_id=None):
    """
    Load dữ liệu từ DW sang Data Mart dm_daily_volatility
    bằng Stored Procedure sp_refresh_dm_daily_volatility().
    """
    try:
        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "PROCESSING",
            message="Bắt đầu load dữ liệu dm_daily_volatility..."
        )

        dm_db.execute("CALL sp_refresh_dm_daily_volatility();")

        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "SUCCESS",
            message="Load dm_daily_volatility thành công."
        )
        return True

    except Exception as e:
        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "FAILURE",
            message=f"Lỗi trong load_dm_daily_volatility(): {e}"
        )
        raise

# =====================================================
#  MAIN ETL
# =====================================================
def main():
    print("=== Bắt đầu LOAD DATA MART ===")

    services = init_services(['config_load_dm_db', 'dw_db', 'dm_db', 'log_db', 'email_service'])
    config_db = services['config_load_dm_db']
    dw_db = services['dw_db']
    dm_db = services['dm_db']
    log_db = services['log_db']
    email_service = services['email_service']

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(log_db, "LOAD_DM", None, "WARNING", "Không có config DM active.")
            return

        for config in configs:
            config_id = config["id"]
            dm_type = config.get("dm_type")   # ví dụ: FACT_PRICE_DAILY, MONTHLY_SUMMARY, DAILY_VOLATILITY

            log_message(log_db, "LOAD_DM", config_id, "READY",
                        f"Bắt đầu xử lý cấu hình DM loại: {dm_type}")

            load_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3

            while not load_success and retry_count < max_retries:
                try:
                    # So sánh loại DM để chạy đúng hàm load
                    if dm_type == "FACT_PRICE_DAILY":
                        load_fact_price_daily(config, dw_db, dm_db, log_db)

                    elif dm_type == "MONTHLY_SUMMARY":
                        load_dm_monthly_stock_summary(config, dw_db, dm_db, log_db)

                    elif dm_type == "DAILY_VOLATILITY":
                        load_dm_daily_volatility(dm_db, log_db, config_id)

                    else:
                        raise ValueError(f"dm_type không hợp lệ: {dm_type}")

                    load_success = True

                except Exception as e:
                    retry_count += 1

                    log_message(log_db, "LOAD_DM", config_id, "FAILURE",
                                f"Lỗi lần {retry_count}: {e}")

                    email_service.send_email(
                        to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                        subject=f"[ETL] Lỗi Load DM ({dm_type})",
                        body=f"Lỗi khi load DM:\n\n{e}"
                    )

    finally:
        config_db.close()
        dw_db.close()
        dm_db.close()
        log_db.close()
        print("=== Kết thúc LOAD DATA MART ===")

if __name__ == "__main__":
    main()