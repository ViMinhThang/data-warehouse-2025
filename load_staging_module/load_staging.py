import os
import io
from dotenv import load_dotenv
from db.config_load_staging_db import ConfigLoadStagingDatabase
from db.staging_db import StagingDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.file_util import get_latest_csv_file, read_csv_file
from utils.logger_util import log_message


def init_services():
    """Khởi tạo database, logger, email service."""
    load_dotenv()

    # DB config
    db_params_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG", "config"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    # DB staging
    db_params_staging = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_STAGING", "staging"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigLoadStagingDatabase(**db_params_config)
    staging_db = StagingDatabase(**db_params_staging)
    log_db = LogDatabase(**db_params_config)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo thành công các service.")
    return config_db, staging_db, log_db, email_service


def load_csv_to_staging(config, staging_db, log_db):
    """Load dữ liệu CSV vào bảng staging."""
    config_id = config["id"]

    try:
        source_path = config.get("source_path")
        target_table = config.get("target_table")
        delimiter = config.get("delimiter", ",")
        has_header = config.get("has_header", True)
        load_mode = config.get("load_mode", "append").lower()

        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            f"Đang xử lý {target_table}...",
        )

        latest_file = get_latest_csv_file(source_path, log_db, config_id)
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            f"Đang load file: {latest_file}",
        )

        df = read_csv_file(latest_file, delimiter, has_header, log_db, config_id)
        if df.empty:
            raise ValueError(f"File {latest_file} rỗng, không có dữ liệu để load.")

        # Xử lý load_mode
        if load_mode == "truncate":
            staging_db.truncate_table(target_table)
        elif load_mode == "overwrite":
            staging_db.drop_table(target_table)
        elif load_mode != "append":
            raise ValueError(f"Giá trị load_mode không hợp lệ: {load_mode}")

        staging_db.copy_from_dataframe(df, target_table)

        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "SUCCESS",
            f"Load thành công {len(df)} bản ghi vào {target_table} ({load_mode.upper()})",
        )

        return True

    except Exception as e:
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "FAILURE",
            f"Lỗi trong load_csv_to_staging(): {e}",
        )
        raise


# =====================================================
#  QUY TRÌNH CHÍNH
# =====================================================


def main():
    print("=== Bắt đầu quá trình LOAD STAGING ===")
    config_db, staging_db, log_db, email_service = init_services()

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "LOAD_STAGING",
                None,
                "WARNING",
                message="Không có config load staging nào đang active.",
            )
            return

        for config in configs:
            config_id = config["id"]
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "READY",
                "Bắt đầu xử lý config load staging.",
            )

            load_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3

            while not load_success and retry_count < max_retries:
                try:
                    load_csv_to_staging(config, staging_db, log_db)
                    load_success = True
                except Exception as e:
                    retry_count += 1
                    log_message(
                        log_db,
                        "LOAD_STAGING",
                        config_id,
                        "FAILURE",
                        f"Lỗi lần {retry_count}: {e}",
                    )
                    email_service.send_email(
                        to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                        subject=f"[ETL] Lỗi Load Staging (config ID={config_id})",
                        body=f"Lỗi khi load dữ liệu staging:\n\n{e}",
                    )

            if not load_success:
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    config_id,
                    "FAILURE",
                    f"Load staging thất bại sau {max_retries} lần retry.",
                )

    except Exception as e:
        log_message(
            log_db, "LOAD_STAGING", None, "FAILURE", f"Lỗi tổng thể trong main(): {e}"
        )

    finally:
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD STAGING.")


if __name__ == "__main__":
    main()
