import os
from dotenv import load_dotenv

from db.config_load_db import ConfigLoadDWDatabase
from db.log_db import LogDatabase
from db.staging_db import StagingDatabase
from db.dw_db import DWDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

load_dotenv()


def init_services():
    """Khởi tạo các kết nối DB và email service."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigLoadDWDatabase(dbname=os.getenv("DB_NAME_CONFIG"), **db_params)
    log_db = LogDatabase(dbname=os.getenv("DB_NAME_CONFIG"), **db_params)
    staging_db = StagingDatabase(dbname=os.getenv("DB_NAME_STAGING"), **db_params)
    dw_db = DWDatabase(dbname=os.getenv("DB_NAME_DW"), **db_params)

    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo thành công các service cho Load Warehouse.")
    return config_db, log_db, staging_db, dw_db, email_service


def load_data_to_warehouse(config, staging_db, dw_db, log_db):
    """Thực hiện load dữ liệu từ staging vào data warehouse cho một config."""
    config_id = config["id"]
    source_table = config["source_table"]
    target_table = config["target_table"]

    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "PROCESSING",
        message=f"Bắt đầu load DW từ {source_table} vào {target_table}.",
    )

    # Đọc dữ liệu từ bảng staging
    df = staging_db.read_data(source_table)
    if df.empty:
        log_message(
            log_db,
            "LOAD_DW",
            config_id,
            "WARNING",
            message=f"Bảng {source_table} không có dữ liệu để load.",
        )
        return

    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "SUCCESS",
        message=f"Đọc {len(df)} dòng từ {source_table}",
    )

    # Đồng bộ dimensions
    dw_db.sync_dim_stock(df)
    dw_db.sync_dim_datetime(df)

    # Load fact table
    new_rows_count = dw_db.load_fact_stock_indicators(df, target_table)

    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "SUCCESS",
        message=f"Load DW thành công. Có {new_rows_count} bản ghi mới trong {target_table}.",
    )


def main():
    print("=== Bắt đầu quá trình LOAD WAREHOUSE ===")
    config_db, log_db, staging_db, dw_db, email_service = init_services()

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "LOAD_DW",
                None,
                "WARNING",
                "Không có config load DW nào đang active.",
            )
            return

        for config in configs:
            config_id = config["id"]
            log_message(
                log_db, "LOAD_DW", config_id, "READY", "Bắt đầu xử lý config load DW."
            )

            load_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3

            while not load_success and retry_count < max_retries:
                try:
                    load_data_to_warehouse(config, staging_db, dw_db, log_db)
                    load_success = True

                except Exception as e:
                    retry_count += 1
                    error_msg = f"Lỗi lần {retry_count}: {e}"
                    log_message(
                        log_db,
                        "LOAD_DW",
                        config_id,
                        "FAILURE",
                        error_message=error_msg,
                    )
                    email_service.send_email(
                        to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                        subject=f"[ETL] Lỗi Load Warehouse (config ID={config_id})",
                        body=f"Lỗi khi load dữ liệu vào warehouse:\n\n{e}",
                    )

            if load_success:
                log_message(
                    log_db, "LOAD_DW", config_id, "SUCCESS", "Hoàn tất xử lý config."
                )
            else:
                log_message(
                    log_db,
                    "LOAD_DW",
                    config_id,
                    "FAILURE",
                    f"Load DW thất bại sau {max_retries} lần retry.",
                )

    except Exception as e:
        log_message(
            log_db, "LOAD_DW", None, "FAILURE", f"Lỗi tổng thể trong main(): {e}"
        )

    finally:
        if config_db:
            config_db.close()
        if log_db:
            log_db.close()
        if staging_db:
            staging_db.close()
        if dw_db:
            dw_db.close()
        print("Kết thúc quá trình LOAD WAREHOUSE.")


if __name__ == "__main__":
    main()
