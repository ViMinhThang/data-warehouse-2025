import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from db.config_dw_db import ConfigDWDatabase
from db.dw_db import DWDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message
# 2.- Tải cấu hình trong .env vào môi trường: 
# + DB_HOST, DB_USER, DB_PASSWORD, DB_PORT
# + DB_NAME_STAGING, DB_NAME_CONFIG, DB_NAME_DM, DB_NAME_STAGING, DB_NAME_DW.
# + EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SIMULATE, EMAIL_ADMIN
# + DEFAULT_RETRY
load_dotenv()

def init_services():
    # 2.- Khởi tạo tham số kết nối với DB: db_params_config, db_params_dw
    # - Khởi tạo các services:
    # + config_db = ConfigDWDatabase(**db_params_config):
    # đọc cấu hình liên kết db dw
    # + dw_db = DWDatabase(**db_params_dw):
    # kết nối với csdl dw, dùng để lưu dữ liệu tạm thời
    # + log_db = LogDatabase(**db_params_config):
    # kết nối với csdl log, dùng để ghi log trong quá trình LOAD vào db
    # + email_services = EmailService(username, password, simulate):
    # gửi thông báo qua email nếu quá trình LOAD gặp lỗi
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

    # 2.- Xuất ra màn hình:
    # "Đã khởi tạo thành công các service DW"
    print("Đã khởi tạo thành công các service DW.")
    return config_db, dw_db, log_db, email_service

def process_dw_load(config, log_db, dw_db, email_service):  
    # 6.1.1. Trích xuất các đối tượng từ config cho quá trình load: config_id, dim_path, fact_path, procedure_name
    config_id = config["id"]
    dim_path = config["dim_path"]
    fact_path = config["fact_path"]
    procedure_name = config.get("procedure", "sp_load_stock_files_from_tmp")

    # 6.1.2. Ghi log: "LOAD_DW - config_id -  READY - Bắt đầu load files dim_stock, fact_stock"
    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "READY",
        message="Bắt đầu load files dim_stock, fact_stock",
    )

    try:
        # 6.1.3. Lấy engine từ dw_db vá đọc file csv qua các đối tượng được trích xuất từ config
        # engine = dw_db.engine
        # dim_df = pd.read_csv(dim_path)
        # fact_df = pd.read_csv(fact_path)
        engine = dw_db.engine
        dim_df = pd.read_csv(dim_path)
        fact_df = pd.read_csv(fact_path, parse_dates=["datetime_utc"])

        # 6.1.4. Mở Transaction, commit/rollback tự động, tránh lỗi tỏng quá trình thêm dữ liệu vào dw
        # with engine.begin() as conn:    
        with engine.begin() as conn:
            # 6.1.4.1.1.  Thực hiện xoá và tạo các bảng tạm:
            # - Xoá table tmp_dim_stock nếu tồn tại
            # conn.execute(text("DROP TABLE IF EXISTS tmp_dim_stock"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_dim_stock"))
            # - Tạo bảng tạm tmp_dim_stock
            # conn.execute(text("CREATE TEMP TABLE tmp_dim_stock (stock_sk INT, ticket VARCHAR(20))"))
            conn.execute(
                text(
                    "CREATE TEMP TABLE tmp_dim_stock (stock_sk INT, ticker VARCHAR(20))"
                )
            )
            # - dim_df gọi lệnh to_sql
            # dim_df.to_sql("tmp_dim_stock", conn, if_exists="append",....)
            dim_df.to_sql(
                "tmp_dim_stock", conn, if_exists="append", index=False, method="multi"
            )
            # - Xoá table tmp_fact_stock nếu tồn tại
            # conn.execute(text("DROP TABLE IF EXISTS tmp_fact_stock"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_fact_stock"))
            # - Tạo bảng tạm tmp_fact_stock
            # conn.execute(text("CREATE TEMP TABLE tmp_fact_stock (record_sk INT, stock_sk INT, ....)"))
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
            # - fact_df gọi lệnh to_sql
            # dim_df.to_sql("tmp_fact_stock", conn, if_exists="append",....)
            fact_df.to_sql(
                "tmp_fact_stock", conn, if_exists="append", index=False, method="multi"
            )
            # 6.1.4.1.2. Gọi procedure và refresh cho tất cả các bảng aggregate
            # conn.execute(text(f"CALL {procedure_name}()"))
            # conn.execute(text("CALL sp_refresh_all_aggregates()"))
            conn.execute(text(f"CALL {procedure_name}()"))
            conn.execute(text("CALL sp_refresh_all_aggregates()"))
        
        # 6.1.5. Ghi log: "LOAD_DW - config_id - SUCCESS - Load thành công dim_stock, fact_stock".
        log_message(
            log_db,
            "LOAD_DW",
            config_id,
            "SUCCESS",
            message="Load thành công dim_stock, fact_stock",
        )
    # 6.1.4.2.1. Lỗi trong quá trình transaction:
    except Exception as e:
        # - Ghi log: "LOAD_DW - config_id - FAILURE - Lỗi khi load dữ liệu: exception"
        log_message(
            log_db, "LOAD", config_id, "FAILURE", message=f"Lỗi khi load dữ liệu: {e}"
        )
        # - Trích xuất email từ config
        # emails = config.get("emails")
        emails = config.get("emails")
        # - Nếu emails = null, gán emails = [], để tránh lỗi.
        if not emails:
            emails = []
        # - Gọi send_email để thực hiện thông báo lỗi cho các emails được đăng kí.
        # email_services.send_email(to_addrs=emails, subjects=f"[ETL Extract] Lỗi Config ID=....", body=f"....")        
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
    # 1.Xuất ra màn hình: 
    # "=== Bắt đầu quá trình LOAD DW ==="
    print("=== Bắt đầu quá trình LOAD DW ===")
    # 2. Gọi hàm init_services():
    # - Nhận các đối tượng: config_db, dw_db, log_db, email_service
    config_db, dw_db, log_db, email_service = init_services()

    try:
        
        # 3. Lấy log TRANSFORM mới nhất
        # Gọi latest_extract_log = log_db.get_latest_log("TRANSFORM", None), gọi tới cơ sở dữ liệu log để lấy bảng ghi log TRANSFORM gần nhất

        # 4. Lấy ra danh sách các config còn active trong bảng config
        # configs = config_db.get_active_configs()    
        configs = config_db.get_active_configs()
        # 4.1. Đối tượng configs = null
        if not configs:
            # Ghi log: "LOAD_DW-FAILURE-Không có config DW active."
            log_message(
                log_db, "LOAD_DW", None, "FAILURE", message="Không có config DW active."
            )
            return

        # 5. Lặp qua từng config để load dữ liệu
        # for config in configs
        for config in configs:
            try:
                # 6. Gọi hàm process_dw_load(config, log_db, dw_db, email_service)
                process_dw_load(config, log_db, dw_db, email_service)
            # 6.2.1. Lỗi không bắt được trong def process_dw_load()
            except Exception as e:
                # Ghi log: "LOAD_DW - config_id - FAILURE - Lỗi tổng thể xử lý config DW: exception"
                log_message(
                    log_db,
                    "LOAD_DW",
                    config.get("id"),
                    "FAILURE",
                    message=f"Lỗi tổng thể xử lý config DW: {e}",
                )
            # 5.1. Hết config?
    # 7. Kết thúc process:
    # config_db.close()
    # dw_db.close()
    # log_db.close()
    # Xuất ra màn hình: "Kết thúc quá trình LOAD DW."
    finally:
        config_db.close()
        dw_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD DW.")

# 1. Gọi phương thức khởi động, 
# if __name__ = "__main__":
if __name__ == "__main__":
    # Gọi hàm main(), bắt đầu quá trình load_datawarehouse.
    main()
