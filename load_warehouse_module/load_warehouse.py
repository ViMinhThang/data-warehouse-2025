import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from db.config_load_db import ConfigLoadDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService

# ======================================================
# 1️. LOAD .env & Logging setup
# ======================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# ======================================================
# 2️. INIT SERVICE
# ======================================================
def init_services():
    """Khởi tạo các service: DB config, Log DB, Email, Engine."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    dw_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

    config_db = ConfigLoadDatabase(**db_params)
    log_db = LogDatabase(**db_params)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    engine = create_engine(dw_url)
    return config_db, log_db, email_service, engine


# ======================================================
# 3️. RUN LOAD FUNCTION
# ======================================================
def run_load(cfg, engine):
    """Thực hiện load dữ liệu vào Data Warehouse."""
    config_id = cfg["id"]
    source_path = cfg["source_path"]
    target_schema = cfg["target_schema"]
    target_table = cfg["target_table"]
    load_mode = cfg.get("load_mode", "append").lower()

    start_time = time.time()

    # Kiểm tra mode hợp lệ
    if load_mode not in ("append", "replace"):
        raise ValueError(f"Giá trị load_mode không hợp lệ: {load_mode}. Chỉ được 'append' hoặc 'replace'.")

    # Kiểm tra file nguồn
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Không tìm thấy file nguồn: {source_path}")

    df = pd.read_csv(source_path)
    if df.empty:
        raise ValueError(f"File {source_path} không có dữ liệu để load.")

    logging.info(f"Đang load {len(df)} rows vào {target_schema}.{target_table} (mode={load_mode})")

    # Kiểm tra schema & table tồn tại
    with engine.connect() as conn:
        schema_exists = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"),
            {"schema": target_schema},
        ).fetchone()
        if not schema_exists:
            raise ValueError(f"Schema '{target_schema}' không tồn tại trong database.")

        table_exists = conn.execute(
            text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table
            """),
            {"schema": target_schema, "table": target_table},
        ).fetchone()
        if not table_exists:
            raise ValueError(f"Bảng {target_schema}.{target_table} không tồn tại trong database.")

    # Thực hiện load vào database
    df.to_sql(
        target_table,
        engine,
        schema=target_schema,
        if_exists=load_mode,
        index=False,
        chunksize=1000,
        method="multi",
    )

    duration = round(time.time() - start_time, 2)
    logging.info(f"Load hoàn tất {target_schema}.{target_table} ({len(df)} rows, {duration}s)")
    return len(df), duration


# ======================================================
# 4️. MAIN PROCESS
# ======================================================
def main():
    logging.info("Bắt đầu quá trình LOAD dữ liệu...")
    config_db, log_db, email_service, engine = init_services()
    success_count, failure_count, total_rows = 0, 0, 0

    try:
        configs = config_db.get_active_configs()
        if not configs:
            logging.warning("Không có cấu hình load nào đang active.")
            return

        for cfg in configs:
            config_id = cfg["id"]
            try:
                log_db.insert_log("LOAD", config_id, "PROCESSING", "Bắt đầu load dữ liệu.")
                config_db.mark_config_status(config_id, "PROCESSING")

                # Thực hiện load
                rows_loaded, duration = run_load(cfg, engine)
                total_rows += rows_loaded

                msg = f"Loaded {rows_loaded} rows vào {cfg['target_table']} ({duration}s)."
                log_db.insert_log("LOAD", config_id, "SUCCESS", msg)
                config_db.mark_config_status(config_id, "SUCCESS")

                success_count += 1
                logging.info(f"Config ID={config_id} hoàn tất ({rows_loaded} rows).")

            except Exception as e:
                error_msg = str(e)
                logging.error(f"Lỗi khi load Config ID={config_id}: {error_msg}")
                log_db.insert_log("LOAD", config_id, "FAILURE", error_message=error_msg)
                config_db.mark_config_status(config_id, "FAILURE")
                failure_count += 1

                # Gửi email cảnh báo nếu có lỗi
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL LOAD] Lỗi khi load Config ID={config_id}",
                    body=f"Đã xảy ra lỗi trong quá trình load dữ liệu:\n\n{error_msg}",
                )

    except Exception as e:
        logging.error(f"Lỗi tổng thể trong LOAD main(): {e}")

    finally:
        config_db.close()
        log_db.close()
        engine.dispose()

        logging.info("Kết thúc quá trình LOAD.")
        logging.info(f"Tổng kết: {success_count} thành công, {failure_count} thất bại, {total_rows} bản ghi được load.")


if __name__ == "__main__":
    main()
