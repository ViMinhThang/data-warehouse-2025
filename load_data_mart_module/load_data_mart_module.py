import os
import time
from dotenv import load_dotenv

from db.config_dw_db import ConfigDWDatabase
from db.config_data_mart_db import ConfigDMDatabase
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

    config_db = ConfigDMDatabase(**db_params_config)
    dw_db = DWDatabase(**db_params_dw)
    log_db = LogDatabase(**db_params_config)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    return config_db, dw_db, log_db, email_service


def execute_task_with_retry(dw_db, log_db, step_config):

    config_id = step_config["id"]
    proc_name = step_config["procedure_name"]
    task_desc = step_config.get("description", f"Execute {proc_name}")
    max_retries = step_config.get("retry_count", 3) or 3

    retry_count = 0
    success = False
    last_error = None

    while not success and retry_count < max_retries:
        try:
            log_message(
                log_db,
                "LOAD_DM",
                config_id,
                "PROCESSING",
                f"Đang chạy: {task_desc} (Lần {retry_count + 1})",
            )

            # SỬA LỖI TẠI ĐÂY: Dùng execute_non_query thay vì execute
            dw_db.execute_non_query(f"CALL {proc_name}();")

            log_message(
                log_db, "LOAD_DM", config_id, "SUCCESS", f"Hoàn thành: {task_desc}"
            )
            success = True

        except Exception as e:
            retry_count += 1
            last_error = str(e)
            print(f"Warning: {task_desc} gặp lỗi lần {retry_count}: {e}")

            if retry_count < max_retries:
                time.sleep(5)

    if not success:
        error_msg = (
            f"Task '{task_desc}' thất bại sau {max_retries} lần thử.\nLỗi: {last_error}"
        )
        log_message(log_db, "LOAD_DM", config_id, "FAILURE", error_msg)
        return False, last_error

    return True, None


def main():
    print("=== Bắt đầu PIPELINE ETL (DYNAMIC DB CONFIG) ===")
    config_db, dw_db, log_db, email_service = init_services()

    try:
        pipeline_steps = config_db.get_active_configs()

        if not pipeline_steps:
            log_message(
                log_db,
                "LOAD_DM",
                None,
                "WARNING",
                "Không tìm thấy config bước nào active trong bảng config_load_datamart.",
            )
            return

        log_message(
            log_db,
            "LOAD_DM",
            None,
            "READY",
            f"Tìm thấy {len(pipeline_steps)} bước cần chạy theo thứ tự.",
        )

        for step_config in pipeline_steps:
            success, error_msg = execute_task_with_retry(dw_db, log_db, step_config)

            if not success:
                step_desc = step_config.get("description", "Unknown Task")
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL - ERROR] Pipeline dừng tại: {step_desc}",
                    body=f"Lỗi tại bước ID {step_config['id']} ({step_config['procedure_name']}).\n\n{error_msg}",
                )

                is_critical = step_config.get("is_critical", False)

                if is_critical:
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "STOPPED",
                        f"Dừng pipeline vì bước CRITICAL '{step_desc}' thất bại.",
                    )
                    break
                else:
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "WARNING",
                        f"Bỏ qua bước '{step_desc}' và tiếp tục bước sau (Non-Critical).",
                    )

    except Exception as e:
        print(f"Lỗi fatal tại main: {e}")
        if "email_service" in locals():
            email_service.send_email(
                to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                subject="[ETL - CRASH] Script lỗi nghiêm trọng",
                body=str(e),
            )
    finally:
        if "config_db" in locals():
            config_db.close()
        if "dw_db" in locals():
            dw_db.close()
        if "log_db" in locals():
            log_db.close()
        print("=== Kết thúc PIPELINE ===")


if __name__ == "__main__":
    main()
