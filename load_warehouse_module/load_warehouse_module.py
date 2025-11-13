import os
from dotenv import load_dotenv

from db.config_dw_db import ConfigDWDatabase
from db.dw_db import DWDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

load_dotenv()


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


def load_files_via_procedure(
    dim_path: str, fact_path: str, procedure_name: str, dw_db: DWDatabase
):
    """Gọi procedure trong DW truyền 2 file CSV path"""
    if not os.path.exists(dim_path):
        raise FileNotFoundError(f"File {dim_path} không tồn tại.")
    if not os.path.exists(fact_path):
        raise FileNotFoundError(f"File {fact_path} không tồn tại.")

    try:
        dw_db.execute_non_query(
            f"CALL {procedure_name}(%s, %s);", (dim_path, fact_path)
        )
        print(
            f"Gọi procedure {procedure_name} thành công với dim_stock={dim_path}, fact_stock={fact_path}"
        )
    except Exception as e:
        raise RuntimeError(f"Failed to call procedure: {e}")


def process_dw_load(config, log_db, dw_db, email_service):
    config_id = config["id"]
    dim_path = config["dim_path"]
    fact_path = config["fact_path"]
    procedure_name = config.get("procedure", "sp_load_stock_files")

    log_message(
        log_db,
        "LOAD_DW",
        config_id,
        "READY",
        message=f"Bắt đầu load files dim_stock, fact_stock",
    )

    try:
        load_files_via_procedure(dim_path, fact_path, procedure_name, dw_db)
        log_message(
            log_db,
            "LOAD_DW",
            config_id,
            "SUCCESS",
            message=f"Load thành công dim_stock, fact_stock",
        )
    except Exception as e:
        log_message(
            log_db, "LOAD", config_id, "FAILURE", message=f"Lỗi khi load dữ liệu: {e}"
        )
        email_service.send_email(
            to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
            subject=f"[ETL LOAD] Lỗi Config ID={config_id}",
            body=f"Lỗi khi load dữ liệu DW:\n\n{e}",
        )


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
