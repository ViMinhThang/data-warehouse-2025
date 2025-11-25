import os
import time
from dotenv import load_dotenv

# Giả định các module này bạn đã có
from db.config_data_mart_db import ConfigDMDatabase
from db.dm_db import DMDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message

# 2. Gọi hàm load_dotenv()
# - Tự động load cấu hình trong file .env, cụ thể load các biến môi trường: DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, DB_NAME_ CONFIG, DB_NAME_DM, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SIMULATE, EMAIL_ADMIN

# - Khởi tạo lần lượt các service theo danh sách truyền vào (config_db, dw_db, log_db, email_service):
# + Với config_db: đọc cấu hình kết nối database
# + Với dw_db: kết nối cơ sở dữ liệu data warehouse để load dữ liệu
# + Với log_db: kết nối cơ sở dữ liệu log
# + Với email_service: gửi thông báo khi có sự cố trong quá trình hoàn tất
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

    db_params_dm = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_DM"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigDMDatabase(**db_params_config)
    dw_db = DMDatabase(**db_params_dm)
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
    # 9. Lấy thông tin của step, chuẩn bị thông tin step để log
    # - config_id = step_config['id"]
    # - proc_name = step_config["procedure_name"]
    # - task_desc = step_config.get("description", f"Execute {proc_name}")
    # - max_retries = step_config.get("retry_count", 3)
    config_id = step_config["id"]
    proc_name = step_config["procedure_name"]
    task_desc = step_config.get("description", f"Execute {proc_name}")
    max_retries = step_config.get("retry_count", 3)

    # 10. Thiết lập các thuộc tính cần thiết
    # - SET max_retries = config.retry_count hoặc 3
    # - SET retry_count = 0
    # - SET success = False
    # - SET last_error = None
    retry_count = 0
    success = False
    last_error = None

    # 11. Kiểm tra điều kiện vòng lặp: retry_count < max_retries AND success == False ?
    while not success and retry_count < max_retries:
        try:
            # 12. Ghi log PROCESSING
            # In ra màn hình "Đang chạy: {task_desc} (Lần {retry_count + 1})
            log_message(
                log_db,
                "LOAD_DM",
                config_id,
                "PROCESSING",
                f"Đang chạy: {task_desc} (Lần {retry_count + 1})",
            )

            # 13. Thực thi procedure trong data warehouse
            # In ra màn hình "--- Executing: CALL {proc_name}(); ---"
            # Gọi dw_db.execute_non_query(f"CALL {proc_name}();")
            # Gọi Procedure trong DW
            print(f"--- Executing: CALL {proc_name}(); ---")
            dw_db.execute_non_query(f"CALL {proc_name}();")
            # 14. Kiểm tra quá trình gọi procedure thành công hay chưa ?

            # 14.1. Ghi log SUCCESS
            # In ra màn hình "Hoàn thành: {task_desc}"
            log_message(
                log_db, "LOAD_DM", config_id, "SUCCESS", f"Hoàn thành: {task_desc}"
            )

            # 14.1.1. Gắn trạng thái hoàn thành
            # SET success = True
            success = True

        except Exception as e:
            # 15. Điều chỉnh lại giá trị các thuộc tính
            # - retry_count += 1
            # - last_error = str(e) với e là Exception
            retry_count += 1
            last_error = str(e)
            print(f"Warning: {task_desc} gặp lỗi lần {retry_count}: {e}")

            # 16. Ghi log WARNING
            # Lỗi LOAD_DM, in ra màn hình dòng "Lỗi lần {retry_count}: {str(e)}"
            log_message(
                log_db,
                "LOAD_DM",
                config_id,
                "WARNING",
                f"Lỗi lần {retry_count}: {str(e)}",
            )

            # 17. Kiểm tra retry thêm lần nữa ?
            # retry_count < max_retries
            if retry_count < max_retries:
                # 17.1. Chờ 5s trước khi thử lại
                # time.sleep(5)
                time.sleep(5)

    # 18. Kiểm tra điều kiện: success == False ?
    if not success:
        # 19. Ghi log FAILURE
        # Lỗi LOAD_DM, in ra màn hình dòng "Task '{task_desc}' thất bại sau {max_retries} lần thứ.\nLỗi cuối cùng: {last_error}"
        error_msg = f"Task '{task_desc}' thất bại sau {max_retries} lần thử.\nLỗi cuối cùng: {last_error}"
        log_message(log_db, "LOAD_DM", config_id, "FAILURE", error_msg)
        return False, last_error

    return True, None


def main():
    print("=== Bắt đầu PIPELINE LOAD DATAMART ===")

    # 3. Khởi tạo dịch vụ trong hàm init_services()
    # Load các biến môi trường DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, DB_NAME_ CONFIG, DB_NAME_DM, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_SIMULATE
    config_db, dw_db, log_db, email_service = init_services()

    # 4. Bắt đầu khởi chạy khối try...except...finally
    # Kiểm tra xử lý lỗi tổng thể trong main
    try:
        # 5. Gọi config_db.get_active_configs()
        # Lấy danh sách cấu hình có trạng thái "active" từ database, config_load_datamart có is_active = TRUE
        pipeline_steps = config_db.get_active_configs()

        # 6. Kiểm tra xem có cấu hình nào không ?
        if not pipeline_steps:
            # 6.1. Ghi log WARNING
            # Lỗi LOAD_DM, in ra màn hình dòng "Không tìm thấy config active trong bảng config_load_datamart"
            log_message(
                log_db,
                "LOAD_DM",
                None,
                "WARNING",
                "Không tìm thấy config active trong bảng config_load_datamart.",
            )
            return

        print(f"Tìm thấy {len(pipeline_steps)} bước. Bắt đầu thực thi...")

        # 7. Chạy vòng lặp for duyệt qua từng step_config trong danh sách pipeline_steps
        for step_config in pipeline_steps:
            # 8. Gọi hàm execute_task_with_retry()
            # Chạy procedure và retry nếu có lỗi: success, error_msg = execute_task_with_retry(dw_db, log_db, step_config)
            success, error_msg = execute_task_with_retry(dw_db, log_db, step_config)

            if not success:
                step_desc = step_config.get("description", "Unknown Task")
                emails = step_config.get("emails") or []

                if emails:
                    # 20. Gửi email cho người liên quan
                    # - Gửi thông báo qua email EmailService.send_email() với danh sách truyền vào (to_addrs, subject, body):
                    # + Với to_addrs = step_config.get("emails") or []
                    # + Với subject = f"[ETL Load DM - FAILURE] Config ID={step_config.get('id')}"
                    # + Với body = f"Quy trình: {step_desc}\n\nTrạng thái: Thất bại.\n\nChi tiết lỗi:\n{error_msg}"
                    email_service.send_email(
                        to_addrs=emails,
                        subject=f"[ETL Load DM - FAILURE] Config ID={step_config.get('id')}",
                        body=f"Quy trình: {step_desc}\n\nTrạng thái: Thất bại.\n\nChi tiết lỗi:\n{error_msg}",
                    )

                is_critical = step_config.get("is_critical", False)

                # 21. Kiểm tra điều kiện: is_critical ?
                if is_critical:
                    # 22. Ghi log FAILURE
                    # Lỗi LOAD_DM, in ra màn hình dòng "Dừng pipeline vì bước CRITICAL '{step_desc}' thất bại."
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "FAILURE",
                        f"Dừng pipeline vì bước CRITICAL '{step_desc}' thất bại.",
                    )

                    # 23. Thông báo xử lí dừng vòng lặp ngay lập tức
                    # In ra màn hình "!!! CRITICAL FAILURE - STOPPING PIPELING"
                    # Sử dụng lệnh break
                    print("!!! CRITICAL FAILURE - STOPPING PIPELINE !!!")
                    break
                else:
                    # 21.1. Ghi log INFO
                    # Lỗi LOAD_DM, in ra màn hình dòng "Bỏ qua bước '{step_desc}' và tiếp tục (Non-Critical)."
                    log_message(
                        log_db,
                        "LOAD_DM",
                        step_config["id"],
                        "INFO",
                        f"Bỏ qua bước '{step_desc}' và tiếp tục (Non-Critical).",
                    )

    # 4.1. Ghi log lỗi tổng thể trong main
    # In ra màn hình dòng "Lỗi fatal tại main: {e}"
    except Exception as e:
        print(f"Lỗi fatal tại main: {e}")

        # 4.1.1. Kiểm tra điều kiện: thuộc tính "email_service" in locals()
        if "email_service" in locals():
            # 4.1.2. Gửi email thông báo lỗi
            # - Gửi thông báo lỗi qua email EmailService.send_email() với danh sách truyền vào (to_addrs, subject, body):
            # + Với to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")]
            # + Với subject="[ETL Load DM - CRASH] Script lỗi nghiêm trọng"
            # + Với body=f"Script gặp lỗi không xử lý được:\n{str(e)}"
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
    # 1. Gọi hàm main() bắt đầu chạy
    # In ra màn hình "=== Bắt đầu PIPELINE LOAD DATAMART ==="
    main()
