import os
import time
from dotenv import load_dotenv

# Giả định các module này bạn đã có
from db.config_data_mart_db import ConfigDMDatabase
from db.dw_db import DWDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

load_dotenv()


def init_services():
    """Khởi tạo kết nối DB và Email Service"""
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
    """
    Thực thi Procedure với cơ chế Retry
    """
    config_id = step_config["id"]
    proc_name = step_config["procedure_name"]
    task_desc = step_config.get("description", f"Execute {proc_name}")
    max_retries = step_config.get("retry_count", 3)

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

            # Gọi Procedure trong DW
            print(f"--- Executing: CALL {proc_name}(); ---")
            dw_db.execute_non_query(f"CALL {proc_name}();")

            log_message(
                log_db, "LOAD_DM", config_id, "SUCCESS", f"Hoàn thành: {task_desc}"
            )
            success = True

        except Exception as e:
            retry_count += 1
            last_error = str(e)
            print(f"Warning: {task_desc} gặp lỗi lần {retry_count}: {e}")

            # Ghi log warning
            log_message(
                log_db,
                "LOAD_DM",
                config_id,
                "WARNING",
                f"Lỗi lần {retry_count}: {str(e)}",
            )

            if retry_count < max_retries:
                time.sleep(5)  # Chờ 5s trước khi thử lại

    if not success:
        error_msg = f"Task '{task_desc}' thất bại sau {max_retries} lần thử.\nLỗi cuối cùng: {last_error}"
        log_message(log_db, "LOAD_DM", config_id, "FAILURE", error_msg)
        return False, last_error

    return True, None


def main():
    print("=== Bắt đầu PIPELINE LOAD DATAMART ===")
    config_db, dw_db, log_db, email_service = init_services()

    try:
        pipeline_steps = config_db.get_active_configs()

        if not pipeline_steps:
            log_message(
                log_db,
                "LOAD_DM",
                None,
                "WARNING",
                "Không tìm thấy config active trong bảng config_load_datamart.",
            )
            return

        print(f"Tìm thấy {len(pipeline_steps)} bước. Bắt đầu thực thi...")

        for step_config in pipeline_steps:
            success, error_msg = execute_task_with_retry(dw_db, log_db, step_config)

            if not success:
                step_desc = step_config.get("description", "Unknown Task")
                emails = step_config.get("emails") or []

                if emails:
                    email_service.send_email(
                        to_addrs=emails,
                        subject=f"[ETL Load DM - FAILURE] Config ID={step_config.get('id')}",
                        body=f"Quy trình: {step_desc}\n\nTrạng thái: Thất bại.\n\nChi tiết lỗi:\n{error_msg}",
                    )

                is_critical = step_config.get("is_critical", False)

                if is_critical:
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "FAILURE",
                        f"Dừng pipeline vì bước CRITICAL '{step_desc}' thất bại.",
                    )
                    print("!!! CRITICAL FAILURE - STOPPING PIPELINE !!!")
                    break  # Dừng vòng lặp ngay lập tức
                else:
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "INFO",
                        f"Bỏ qua bước '{step_desc}' và tiếp tục (Non-Critical).",
                    )

    except Exception as e:
        print(f"Lỗi fatal tại main: {e}")
        if "email_service" in locals():
            email_service.send_email(
                to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                subject="[ETL Load DM - CRASH] Script lỗi nghiêm trọng",
                body=f"Script gặp lỗi không xử lý được:\n{str(e)}",
            )
    finally:
        # Đóng kết nối an toàn
        if "config_db" in locals() and hasattr(config_db, "close"):
            config_db.close()
        if "dw_db" in locals() and hasattr(dw_db, "close"):
            dw_db.close()
        if "log_db" in locals() and hasattr(log_db, "close"):
            log_db.close()
        print("=== Kết thúc PIPELINE ===")


if __name__ == "__main__":
    main()
