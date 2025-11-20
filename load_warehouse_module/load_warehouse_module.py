import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from db.config_dw_db import ConfigDWDatabase
from db.dw_db import DWDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message
# 1. import các thư viện cần thiết: os, dotenv, pandas, sqlalchemy.text 
# và đọc biến môi trường load_dotenv()
load_dotenv()

# 2. Khởi tạo các services: init_services()
# - ConfigDWDatabase: lưu cấu hình
# - DWDatabase: connection tới Data Warehouse 
# - LogDatabase: ghi log trạng thái 
# - EmailService: gửi alert khi có lỗi
def init_services():
    """Khởi tạo DB và Email service"""
    db_params_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    db_params_dw = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_DW"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigDWDatabase(**db_params_config)
    dw_db = DWDatabase(**db_params_dw)
    log_db = LogDatabase(**db_params_config)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo thành công các service DW.")
    return config_db, dw_db, log_db, email_service

def process_dw_load(config, log_db, dw_db, email_service):  
    # 3.1. Lấy thông tin config: id / dim_path / fact_path / procedure_name
    config_id = config["id"]
    dim_path = config["dim_path"]
    fact_path = config["fact_path"]
    procedure_name = config.get("procedure", "sp_load_stock_files_from_tmp")

    # 3.2. Ghi log: bắt đầu (READY)
    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "READY",
        message="Bắt đầu load files dim_stock, fact_stock",
    )

    try:
        engine = dw_db.engine
        dim_df = pd.read_csv(dim_path)
        fact_df = pd.read_csv(fact_path, parse_dates=["datetime_utc"])

#     3.4. Mở transaction (engine.begin()):
        with engine.begin() as conn:
# - Tạo TEMP TABLE tmp_dim_stock (schema phải tương ứng với file CSV)
            conn.execute(text("DROP TABLE IF EXISTS tmp_dim_stock"))
            conn.execute(
                text(
                    "CREATE TEMP TABLE tmp_dim_stock (stock_sk INT, ticker VARCHAR(20))"
                )
            )
# - Load dim_df vào tmp_dim_stock (to_sql hoặc COPY)
            dim_df.to_sql(
                "tmp_dim_stock", conn, if_exists="append", index=False, method="multi"
            )

# - Tạo TEMP TABLE tmp_fact_stock
            conn.execute(text("DROP TABLE IF EXISTS tmp_fact_stock"))
            conn.execute(
                text(
                    """
                CREATE TEMP TABLE tmp_fact_stock (
                    record_sk INT,
                    stock_sk INT,
                    datetime_utc TIMESTAMPTZ,
                    close NUMERIC(12,4),
                    volume BIGINT,
                    diff NUMERIC(12,4),
                    percent_change_close NUMERIC(12,6),
                    rsi NUMERIC(8,4),
                    roc NUMERIC(8,4),
                    bb_upper NUMERIC(12,4),
                    bb_lower NUMERIC(12,4),
                    created_at TIMESTAMP
                )
            """
                )
            )
# - Load fact_df vào tmp_fact_stock
            fact_df.to_sql(
                "tmp_fact_stock", conn, if_exists="append", index=False, method="multi"
            )

#             3.5. Gọi stored procedure (CALL sp_load...):
# Procedure phải thực hiện: validate, dedupe, merge (INSERT/UPDATE vào dim/fact chính)
            conn.execute(text(f"CALL {procedure_name}()"))

# 3.6. Gọi sp_refresh_all_aggregates() để cập nhật dữ liệu tổng hợp
            conn.execute(text("CALL sp_refresh_all_aggregates()"))
# 3.7. Commit transaction (engine.begin() tự commit nếu không lỗi)
# 3.8. Ghi log SUCCESS hoặc nếu lỗi -> log FAILURE + gửi email
        log_message(
            log_db,
            "LOAD_DW",
            config_id,
            "SUCCESS",
            message="Load thành công dim_stock, fact_stock",
        )
    except Exception as e:
        log_message(
            log_db, "LOAD", config_id, "FAILURE", message=f"Lỗi khi load dữ liệu: {e}"
        )
        emails = config.get("emails")
        if not emails:
            emails = []
        email_service.send_email(
            to_addrs=emails,
            subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
            body=f"Lỗi tổng thể trong process_config:\n\n{e}",
        )

def load_csv_to_tmp_tables(dim_path: str, fact_path: str, dw_db: DWDatabase):
    """
    Load CSV vào các bảng tmp trong PostgreSQL (tmp_dim_stock, tmp_fact_stock)
    Chỉ load dữ liệu, không insert vào bảng chính
    """
    if not os.path.exists(dim_path):
        raise FileNotFoundError(f"File {dim_path} không tồn tại.")
    if not os.path.exists(fact_path):
        raise FileNotFoundError(f"File {fact_path} không tồn tại.")

    engine = dw_db.engine

    dim_df = pd.read_csv(dim_path)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS tmp_dim_stock"))
        conn.execute(
            text(
                """
            CREATE TEMP TABLE tmp_dim_stock (
                id INT,
                ticker VARCHAR(20)
            )
        """
            )
        )
    dim_df.to_sql("tmp_dim_stock", engine, if_exists="append", index=False)
    print(f"Loaded {len(dim_df)} bản ghi vào tmp_dim_stock")

    fact_df = pd.read_csv(fact_path, parse_dates=["datetime_utc"])

    # Tạo bảng tmp_fact_stock
    with engine.begin() as conn:
        conn.execute("DROP TABLE IF EXISTS tmp_fact_stock")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_fact_stock (
                record_sk INT,
                stock_sk INT,
                datetime_utc TIMESTAMPTZ,
                close NUMERIC(12,4),
                volume BIGINT,
                diff NUMERIC(12,4),
                percent_change_close NUMERIC(12,6),
                rsi NUMERIC(8,4),
                roc NUMERIC(8,4),
                bb_upper NUMERIC(12,4),
                bb_lower NUMERIC(12,4),
                created_at TIMESTAMP
            )
        """
        )
    fact_df.to_sql("tmp_fact_stock", engine, if_exists="append", index=False)
    print(f"Loaded {len(fact_df)} bản ghi vào tmp_fact_stock")


def main():
    print("=== Bắt đầu quá trình LOAD DW ===")
    config_db, dw_db, log_db, email_service = init_services()

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db, "LOAD_DW", None, "FAILURE", message="Không có config DW active."
            )
            return

        for config in configs:
            try:
                process_dw_load(config, log_db, dw_db, email_service)
            except Exception as e:
                log_message(
                    log_db,
                    "LOAD_DW",
                    config.get("id"),
                    "FAILURE",
                    message=f"Lỗi tổng thể xử lý config DW: {e}",
                )

    finally:
        config_db.close()
        dw_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD DW.")


if __name__ == "__main__":
    main()
