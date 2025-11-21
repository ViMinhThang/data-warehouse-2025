import csv
import os
from dotenv import load_dotenv
from db.staging_db import StagingDatabase
from db.config_transform_db import ConfigTransformDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

load_dotenv()

# 2. Gọi hàm init_services()
# - Tự động load cấu hình trong file .env, cụ thể thể load các biến môi trường:  DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, 
# DB_NAME_CONFIG, DB_NAME_STAGING, DB_NAME_DW, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SIMULATE, EMAIL_ADMIN, DEFAULT_RETRY
# - Khởi tạo lần lượt các  service theo danh sách truyển vào (config_load_staging, staging_db, log_db, email_service)
# Với config_load_staging - đọc cấu hình cho LOAD_STAGING
# Với staging_db - kết nối cơ dữ liệu staging để load dữ liệu tạm
# Với log_db - kết nối cơ sở dữ liệu log, kiểm tra log EXTRACT và ghi load LOAD_STAGING
# Với email_service - gửi thông báo khi có sự cố trong quá trình hoàn tất
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

# 3. Gọi hàm run_transform_procedure()
# Bắt đầu thực hiện xử lý logic cho toàn bộ TRANSFORM
def run_transform_procedure(
    staging_db: StagingDatabase,
    config_db: ConfigTransformDatabase,
    log_db: LogDatabase,
    email_service: EmailService,
):
    try:
# 3.1. khởi tạo config = config_db.get_latest_active_config()
# Nhằm lấy cấu hình TRANSFORM mới nhất
# Với các giá trị config gồm (rsi_window, roc_window, bb_window, dim_path, 
# fact_path, source_table,procedure_transform) 
        config = config_db.get_latest_active_config()
        rsi_window = config["rsi_window"]
        roc_window = config["roc_window"]
        bb_window = config["bb_window"]
        dim_path = config["dim_path"]
        fact_path = config["fact_path"]
        procedure = config.get("procedure_transform", "sp_transform_market_prices")
# 3.1.1 (YES) 3.2. Khởi tạo latest_load_log = log_db.get_latest_log("LOAD_STAGING", None)
# Nhằm lấy trạng thái LOAD_STAGING mới nhất
        latest_load_log = log_db.get_latest_log("LOAD_STAGING", None)
# 3.2.1. Load_staging đã thành công chưa? 
# Có log hoặc status == SUCCESS
        if not latest_load_log or latest_load_log.get("status") != "SUCCESS":
            # 3.2.1 (NO) Ghi log: "TRANSFORM – WARNING – LOAD_STAGING chưa thành công, bỏ qua TRANSFORM. "
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "WARNING",
                message="LOAD_STAGING chưa thành công, bỏ qua TRANSFORM.",
            )
            print("Bỏ qua TRANSFORM vì LOAD_STAGING chưa SUCCESS.")
            return
        # 3.2.1 (YES) 3.3. Ghi log: "TRANSFORM – READY – Bắt đầu chạy procedure transform" 
        log_message(
            log_db,
            "TRANSFORM",
            None,
            "READY",
            message="Bắt đầu chạy procedure transform.",
        )
        # 3.4. Bắt đầu chạy procedure transform
        with staging_db.conn.cursor() as cursor:
            # 3.4.1. Ghi log: "TRANSFORM – PROCESSING – Đang chạy {procedure}..."
            log_message(
                log_db,
                "TRANSFORM",
                None,
                "PROCESSING",
                message=f"Đang chạy {procedure}...",
            )
# 3.4.2. Thực thi CALL {procedure}(rsi_window, roc_window, bb_window)
# Nhằm gọi procedure bên sql để thực hiện:
# Cập nhập dim_stock
# Tính ROC, RSI, BB
# Chèn dữ liệu sau khi xử lý vào bảng fact_stock_indicators
# Truncate bảng stg_market_prices
            cursor.execute(
                f"CALL {procedure}(%s, %s, %s);", (rsi_window, roc_window, bb_window)
            )
            # 3.4.3. Thực hiện commit dữ liệu DB staging
            staging_db.conn.commit()
# 3.4.4. Ghi log: "TRANSFORM – SUCCESS – Procedure transform hoàn tất"
        log_message(
            log_db,
            "TRANSFORM",
            None,
            "SUCCESS",
            message="Procedure transform hoàn tất.",
        )
        print("TRANSFORM completed successfully.")
# 3.5. Gọi hàm export_table_to_csv(staging_db, table_name, file_path)
# Thực hiện xuất dữ liệu ra file CSV lần lượt với hai bảng dim_stock và fact_stock_indicators
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

# 3.5. Gọi hàm export_table_to_csv(staging_db, table_name, file_path)
# Thực hiện xuất dữ liệu ra file CSV lần lượt với hai bảng dim_stock và fact_stock_indicators
def export_table_to_csv(staging_db, table_name, file_path):
    with staging_db.conn.cursor() as cursor:
# 3.5.1. Thực hiện truy vấn (SELECT * FROM {table_name})
# Để lấy toàn bộ dữ liệu từ bảng staging
        cursor.execute(f"SELECT * FROM {table_name}")
        # 3.5.2. Ghi tên cột làm header CSV 
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

    with open(file_path, "w", newline="") as f:
        # 3.5.3. Ghi dữ liệu rows xuống file CSV
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)
        # 3.5.4. Console: "Export {len(rows)} rows from {table_name} to {file_path}"
    print(f"Export {len(rows)} rows from {table_name} to {file_path}")

# 1. Gọi hàm main(), để bắt đầu khởi động quá trình TRANSFORM
# In ra màn hình: "=== Bắt đầu quá trình TRANSFORM ==="
def main():
    print("=== Bắt đầu quá trình TRANSFORM ===")
# 2. Gọi hàm init_services()
# - Tự động load cấu hình trong file .env, cụ thể thể load các biến môi trường:  DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, 
# DB_NAME_CONFIG, DB_NAME_STAGING, DB_NAME_DW, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SIMULATE, EMAIL_ADMIN, DEFAULT_RETRY
# - Khởi tạo lần lượt các  service theo danh sách truyển vào (config_load_staging, staging_db, log_db, email_service)
# Với config_load_staging - đọc cấu hình cho LOAD_STAGING
# Với staging_db - kết nối cơ dữ liệu staging để load dữ liệu tạm
# Với log_db - kết nối cơ sở dữ liệu log, kiểm tra log EXTRACT và ghi load LOAD_STAGING
# Với email_service - gửi thông báo khi có sự cố trong quá trình hoàn tất
    config_db, staging_db, log_db, email_service = init_services()

    try:
# 3. Gọi hàm run_transform_procedure()
# Bắt đầu thực hiện xử lý logic cho toàn bộ TRANSFORM
        run_transform_procedure(staging_db, config_db, log_db, email_service)
    finally:
# 4. Kết thúc toàn bộ quá trình
# Đóng kết nối config_db.close(), staging_dbclose(), log_dbclose()
# Console: "Kết thúc TRANSFORM."
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc TRANSFORM.")


if __name__ == "__main__":
    main()
