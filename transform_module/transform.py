import csv
import os
import logging
from dotenv import load_dotenv
from db.staging_db import StagingDatabase
from db.config_transform_db import ConfigTransformDatabase
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

    config_db = ConfigTransformDatabase(**db_params_config)
    staging_db = StagingDatabase(**db_params_staging)
    log_db = LogDatabase(**db_params_config)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo các service TRANSFORM.")
    return config_db, staging_db, log_db, email_service


def get_transform_config(config_db):
    """Lấy rsi_window, roc_window, bb_window từ config"""
    query = """
    SELECT rsi_window, roc_window, bb_window
    FROM config_transform
    WHERE is_active = TRUE
    ORDER BY id DESC
    LIMIT 1
    """
    with config_db.conn.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            raise ValueError("Không tìm thấy cấu hình transform nào active")
        return row


def run_transform_procedure(
    staging_db: StagingDatabase,
    config_db: ConfigTransformDatabase,
    log_db: LogDatabase,
    email_service: EmailService,
):
    """Chạy procedure transform nếu LOAD_STAGING thành công"""
    rsi_window, roc_window, bb_window = get_transform_config(config_db)
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

        with staging_db.conn.cursor() as cursor:
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "PROCESSING",
                message="Đang chạy sp_transform_market_prices()...",
            )
            cursor.execute(
                "CALL sp_transform_market_prices(%s, %s, %s);",
                (rsi_window, roc_window, bb_window),
            )
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


def export_table_to_csv(staging_db, table_name, file_path):
    with staging_db.conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)


def main():
    print("=== Bắt đầu quá trình TRANSFORM ===")
    config_db, staging_db, log_db, email_service = init_services()

    try:
        run_transform_procedure(staging_db, config_db, log_db, email_service)
        export_table_to_csv(
            staging_db, "dim_stock", "/home/fragile/PostgresExports/dim_stock.csv"
        )
        export_table_to_csv(
            staging_db,
            "fact_stock_indicators",
            "/home/fragile/PostgresExports/fact_stock_indicators.csv",
        )
    finally:
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc TRANSFORM.")


if __name__ == "__main__":
    main()
