import os
import logging
import pandas as pd
from dotenv import load_dotenv
from db.config_transform_db import ConfigTransformDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.util import compute_stock_indicators

# ==========================
# Logging & .env setup
# ==========================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# ==========================
# Khởi tạo Service
# ==========================
def init_services():
    """Khởi tạo tất cả service cần thiết cho Transform."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigTransformDatabase(**db_params)
    log_db = LogDatabase(**db_params)

    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    return config_db, log_db, email_service


# ==========================
# Chạy Transform cho từng config
# ==========================
def run_transform(config):
    """Thực hiện bước TRANSFORM cho một cấu hình."""
    config_id = config["id"]
    source_table = config["source_table"]
    dest_table = config["destination_table"]

    logging.info(f"Transforming data: {source_table} → {dest_table}")

    # Đọc file nguồn
    source_path = f"./output/{source_table}.csv"
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = pd.read_csv(source_path)
    logging.info(f"Loaded {len(df)} rows from {source_path}")

    # Tính toán các chỉ báo kỹ thuật
    df_transformed = compute_stock_indicators(df, ticker_col="ticker")

    # Ghi dữ liệu ra thư mục transform
    output_path = f"./output/transform/{dest_table}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_transformed.to_csv(output_path, index=False)

    logging.info(f"Transform completed. Saved to {output_path}")
    return output_path


# ==========================
# Hàm Main
# ==========================
def main():
    logging.info("Bắt đầu quá trình TRANSFORM")
    config_db, log_db, email_service = init_services()

    try:
        configs = config_db.get_active_configs()
        if not configs:
            logging.warning("Không có config transform nào đang active.")
            return

        for cfg in configs:
            config_id = cfg["id"]
            try:
                log_db.insert_log("TRANSFORM", config_id, "PROCESSING", "Start transform.")
                config_db.mark_config_status(config_id, "PROCESSING")

                # Thực thi transform
                run_transform(cfg)

                # Thành công
                log_db.insert_log("TRANSFORM", config_id, "SUCCESS", "Transform completed.")
                config_db.mark_config_status(config_id, "SUCCESS")

            except Exception as e:
                logging.error(f"Lỗi khi transform config ID={config_id}: {e}")
                log_db.insert_log("TRANSFORM", config_id, "FAILURE", error_message=str(e))
                config_db.mark_config_status(config_id, "FAILURE")

                # Gửi email cảnh báo
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL] Lỗi Transform Config ID={config_id}",
                    body=f"Đã xảy ra lỗi trong quá trình Transform:\n\n{e}",
                )

    except Exception as e:
        logging.error(f"Lỗi tổng thể trong TRANSFORM main(): {e}")

    finally:
        config_db.close()
        log_db.close()
        logging.info("Kết thúc quá trình TRANSFORM.")


if __name__ == "__main__":
    main()
