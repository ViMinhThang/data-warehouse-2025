import csv
import os
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


def run_transform_procedure(
    staging_db: StagingDatabase,
    config_db: ConfigTransformDatabase,
    log_db: LogDatabase,
    email_service: EmailService,
):
    try:
        config = config_db.get_latest_active_config()
        rsi_window = config["rsi_window"]
        roc_window = config["roc_window"]
        bb_window = config["bb_window"]
        dim_path = config["dim_path"]
        fact_path = config["fact_path"]
        procedure = config.get("procedure_transform", "sp_transform_market_prices")

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

        log_message(
            log_db,
            "TRANSFORM",
            None,
            "READY",
            message="Bắt đầu chạy procedure transform.",
        )

        # Thực hiện procedure transform
        with staging_db.conn.cursor() as cursor:
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "PROCESSING",
                message=f"Đang chạy {procedure}...",
            )
            cursor.execute(
                f"CALL {procedure}(%s, %s, %s);", (rsi_window, roc_window, bb_window)
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

        # Export CSV theo đường dẫn từ config
        export_table_to_csv(staging_db, "dim_stock", dim_path)
        export_table_to_csv(staging_db, "fact_stock_indicators", fact_path)

    except Exception as e:
        log_message(
            log_db,
            "TRANSFORM",
            None,
            "FAILURE",
            message=f"Lỗi khi chạy TRANSFORM: {e}",
        )
        emails = config.get("emails")
        if not emails:
            emails = []
        email_service.send_email(
            to_addrs=emails,
            subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
            body=f"Lỗi tổng thể trong process_config:\n\n{e}",
            )
        raise


def export_table_to_csv(staging_db, table_name, file_path):
    """Export dữ liệu bảng staging sang CSV"""
    with staging_db.conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)
    print(f"Export {len(rows)} rows from {table_name} to {file_path}")


def main():
    print("=== Bắt đầu quá trình TRANSFORM ===")
    config_db, staging_db, log_db, email_service = init_services()

    try:
        run_transform_procedure(staging_db, config_db, log_db, email_service)
    finally:
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc TRANSFORM.")


if __name__ == "__main__":
    main()
