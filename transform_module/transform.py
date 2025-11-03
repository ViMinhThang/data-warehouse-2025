import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from db.config_transform_db import ConfigTransformDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.util import compute_stock_indicators


# ==========================
# 1️⃣ SETUP
# ==========================
# Load file .env
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# Cấu hình logging (có màu cho dễ nhìn)
logging.basicConfig(
    level=logging.INFO,
    format="\033[92m%(asctime)s [%(levelname)s]\033[0m %(message)s",
    handlers=[logging.StreamHandler()],
)


# ==========================
# 2️⃣ KHỞI TẠO SERVICE
# ==========================
def init_services():
    """Khởi tạo kết nối DB và email service."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname_config": os.getenv("DB_NAME_CONFIG", "config"),
        "dbname_staging": os.getenv("DB_NAME_STAGING", "staging"),
    }

    # Kết nối DB cấu hình
    config_db = ConfigTransformDatabase(
        host=db_params["host"],
        dbname=db_params["dbname_config"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    # Kết nối DB ghi log
    log_db = LogDatabase(
        host=db_params["host"],
        dbname=db_params["dbname_config"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    # Kết nối tới DB staging bằng SQLAlchemy (để load/push dataframe)
    staging_engine = create_engine(
        f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname_staging']}"
    )

    # Email service
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    return config_db, log_db, email_service, staging_engine


# ==========================
# 3️⃣ HÀM THỰC HIỆN TRANSFORM
# ==========================
def run_transform(cfg, engine):
    config_id = cfg["id"]
    source_table = cfg["source_table"]           # ví dụ: staging.raw_stock
    destination_table = cfg["destination_table"] # ví dụ: staging.transform_stock

    logging.info(f"🔄 Transform config ID={config_id}: {source_table} → {destination_table}")

    start_time = time.time()

    # Đọc dữ liệu từ bảng nguồn (DB staging)
    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, engine)
    if df.empty:
        raise ValueError(f"Bảng {source_table} không có dữ liệu để transform.")

    logging.info(f"📊 Đã đọc {len(df)} bản ghi từ {source_table}")

    # Tính toán chỉ báo kỹ thuật (RSI, ROC, Bollinger Band,...)
    try:
        df_transformed = compute_stock_indicators(df, ticker_col="ticker")
    except Exception as e:
        logging.warning(f"⚠️ Lỗi khi tính toán chỉ báo: {e}")
        df_transformed = df.copy()

    # Xóa dữ liệu cũ trong bảng đích và ghi mới
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {destination_table};"))
    df_transformed.to_sql(destination_table, engine, if_exists="append", index=False)

    duration = round(time.time() - start_time, 2)
    logging.info(f"✅ Transform thành công {len(df_transformed)} bản ghi trong {duration}s.")
    return len(df_transformed), duration


# ==========================
# 4️⃣ MAIN PROCESS
# ==========================
def main():
    logging.info("=== 🚀 Bắt đầu quá trình TRANSFORM ===")
    config_db, log_db, email_service, engine = init_services()
    success_count, failure_count = 0, 0

    try:
        configs = config_db.get_active_configs()
        if not configs:
            logging.warning("⚠️ Không có config transform nào đang active.")
            return

        for cfg in configs:
            config_id = cfg["id"]
            try:
                # Cập nhật trạng thái PROCESSING
                log_db.insert_log("TRANSFORM", config_id, "PROCESSING", "Bắt đầu transform.")
                config_db.mark_config_status(config_id, "PROCESSING")

                # Thực hiện transform
                rows, duration = run_transform(cfg, engine)

                # Log thành công
                log_db.insert_log("TRANSFORM", config_id, "SUCCESS", f"Transform thành công {rows} bản ghi.")
                config_db.mark_config_status(config_id, "SUCCESS")
                success_count += 1

            except Exception as e:
                # Log thất bại và gửi email
                logging.error(f"❌ Lỗi khi transform config ID={config_id}: {e}")
                log_db.insert_log("TRANSFORM", config_id, "FAILURE", error_message=str(e))
                config_db.mark_config_status(config_id, "FAILURE")
                failure_count += 1

                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL TRANSFORM] Lỗi Config ID={config_id}",
                    body=f"Lỗi khi xử lý transform:\n\n{e}",
                )

    except Exception as e:
        logging.error(f"🔥 Lỗi tổng thể trong TRANSFORM main(): {e}")

    finally:
        config_db.close()
        log_db.close()
        engine.dispose()
        logging.info(f"🏁 Kết thúc TRANSFORM — Thành công: {success_count}, Thất bại: {failure_count}")


# ==========================
# 5️⃣ ENTRY POINT
# ==========================
if __name__ == "__main__":
    main()
