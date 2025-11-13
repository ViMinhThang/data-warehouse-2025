import os
import logging
from dotenv import load_dotenv
from db.staging_db import StagingDatabase
from db.config_load_staging_db import ConfigLoadStagingDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

load_dotenv()


def init_services():
    """Khởi tạo DB và Email service"""
    db_params_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG", "config"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

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

    print("Đã khởi tạo các service TRANSFORM.")
    return config_db, staging_db, log_db, email_service


def run_transform_procedure(
    staging_db: StagingDatabase, log_db: LogDatabase, email_service: EmailService
):
    """Chạy procedure transform nếu LOAD_STAGING thành công"""
    try:
        latest_load_log = log_db.get_latest_log("LOAD_STAGING", None)
        if not latest_load_log or latest_load_log.get("status") != "SUCCESS":
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "WARNING",
                message="LOAD_STAGING chưa thành công, bỏ qua TRANSFORM.",
            )
            print("Bỏ qua TRANSFORM vì LOAD_STAGING chưa SUCCESS.")
            return

        # Log bắt đầu transform
        log_message(
            log_db,
            "TRANSFORM",
            None,
            "READY",
            message="Bắt đầu chạy procedure transform.",
        )

        # Chạy procedure
        with staging_db.conn.cursor() as cursor:
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "PROCESSING",
                message="Đang chạy sp_transform_market_prices()...",
            )
            cursor.execute("CALL sp_transform_market_prices();")
            staging_db.conn.commit()

        log_message(
            log_db,
            "TRANSFORM",
            None,
            "SUCCESS",
            message="Procedure transform hoàn tất.",
        )
        print("TRANSFORM completed successfully.")

    except Exception as e:
        log_message(
            log_db, "TRANSFORM", None, "FAILURE", message=f"Lỗi khi chạy TRANSFORM: {e}"
        )
        email_service.send_email(
            to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
            subject="[ETL Transform] Lỗi procedure transform",
            body=f"Lỗi khi chạy procedure transform:\n\n{e}",
        )
        raise


def main():
    print("=== Bắt đầu quá trình TRANSFORM ===")
    config_db, staging_db, log_db, email_service = init_services()

    try:
        run_transform_procedure(staging_db, log_db, email_service)
    finally:
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc TRANSFORM.")


if __name__ == "__main__":
    main()
