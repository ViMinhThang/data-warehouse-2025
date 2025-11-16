import os
from dotenv import load_dotenv

from utils.service_util import init_services
from utils.logger_util import log_message

load_dotenv()

# =====================================================
#  HÀM LOAD DW → DATA MART
# =====================================================
def load_dw_to_dm(config, dw_db, dm_db, log_db):
    """
    Load dữ liệu từ Data Warehouse sang Data Mart fact_price_daily.
    config: dict chứa thông tin cấu hình (table, điều kiện, v.v.)
    dw_db: service kết nối DW
    dm_db: service kết nối Data Mart
    log_db: service log
    """
    config_id = config["id"]
    try:
        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "PROCESSING",
            message="Bắt đầu load dữ liệu từ DW sang Data Mart...",
        )

        # 1. Lấy dữ liệu từ fact_stock_indicators
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
            raise ValueError("Không có dữ liệu từ DW để load sang DM.")

        # 2. Load dữ liệu vào Data Mart fact_price_daily
        dm_db.copy_from_dataframe(df, "fact_price_daily")

        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "SUCCESS",
            message=f"Load thành công {len(df)} bản ghi vào fact_price_daily.",
        )
        return True

    except Exception as e:
        log_message(
            log_db,
            "LOAD_DM",
            config_id,
            "FAILURE",
            message=f"Lỗi trong load_dw_to_dm(): {e}",
        )
        raise


# =====================================================
#  QUY TRÌNH CHÍNH
# =====================================================
def main():
    print("=== Bắt đầu quá trình LOAD DW → DATA MART ===")
    services = init_services(['config_load_dm_db', 'dw_db', 'dm_db', 'log_db', 'email_service'])
    config_db = services['config_load_dm_db']
    dw_db = services['dw_db']
    dm_db = services['dm_db']
    log_db = services['log_db']
    email_service = services['email_service']

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "LOAD_DM",
                None,
                "WARNING",
                message="Không có config load DM active.",
            )
            return

        for config in configs:
            config_id = config["id"]
            log_message(
                log_db,
                "LOAD_DM",
                config_id,
                "READY",
                message="Bắt đầu xử lý config load DM.",
            )

            load_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3

            while not load_success and retry_count < max_retries:
                try:
                    load_dw_to_dm(config, dw_db, dm_db, log_db)
                    load_success = True
                except Exception as e:
                    retry_count += 1
                    log_message(
                        log_db,
                        "LOAD_DM",
                        config_id,
                        "FAILURE",
                        message=f"Lỗi lần {retry_count}: {e}",
                    )
                    email_service.send_email(
                        to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                        subject=f"[ETL] Lỗi Load DM (config ID={config_id})",
                        body=f"Lỗi khi load dữ liệu từ DW sang DM:\n\n{e}",
                    )

            if not load_success:
                log_message(
                    log_db,
                    "LOAD_DM",
                    config_id,
                    "FAILURE",
                    message=f"Load DM thất bại sau {max_retries} lần retry.",
                )

    except Exception as e:
        log_message(
            log_db,
            "LOAD_DM",
            None,
            "FAILURE",
            message=f"Lỗi tổng thể trong main(): {e}",
        )

    finally:
        config_db.close()
        dw_db.close()
        dm_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD DW → DATA MART.")


if __name__ == "__main__":
    main()