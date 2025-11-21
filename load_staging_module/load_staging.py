import os
from dotenv import load_dotenv

from utils.service_util import init_services
from utils.file_util import get_latest_csv_file, read_csv_file
from utils.logger_util import log_message


def load_csv_to_staging(config, staging_db, log_db):
    """Load dữ liệu CSV vào bảng staging."""
    config_id = config["id"]

    try:
        source_path = config.get("source_path")
        target_table = config.get("target_table")
        delimiter = config.get("delimiter", ",")
        has_header = config.get("has_header", True)
# 7.1. Ghi log "Đăng xử lý {target_table}..."
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            message=f"Đang xử lý {target_table}...",
        )
# 7.2. Gọi hàm get_latest_csv_file(source_path, log_db, config_id)
# Lấy file CSV mới nhất trong thư mục nguồn
        latest_file = get_latest_csv_file(source_path, log_db, config_id)
        # 7.3. Ghi log "Đang load file: {latest_file}"
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            message=f"Đang load file: {latest_file}",
        )
# 7.4. Gọi hàm read_csv_file(latest_file, delimiter, has_header, log_db, config_id)
# Đọc nội dung file CSV và chuyển thành DataFrame
        df = read_csv_file(latest_file, delimiter, has_header, log_db, config_id)
        # 7.5 Kiểm tra File rỗng (df.empty) ?
        if df.empty:
            raise ValueError(f"File {latest_file} rỗng, không có dữ liệu để load.")
#7.5 (NO) 8. Gọi staging_db.copy_from_dataframe(df, target_table)
# Nạp dữ liệu vào bảng staging tương ứng
        staging_db.copy_from_dataframe(df, target_table)
# 9. Ghi log "Load thành công {len(df)} bản ghi vào {target_table}"
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "SUCCESS",
            message=f"Load thành công {len(df)} bản ghi vào {target_table}",
        )

        return True

    except Exception as e:
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "FAILURE",
            message=f"Lỗi trong load_csv_to_staging(): {e}",
        )
        raise



# 1. Gọi hàm main() bắt đầu chạy
# In ra màn hình: "=== Bắt đầu quá trình LOAD STAGING ==="
def main():
    print("=== Bắt đầu quá trình LOAD STAGING ===")
#  2. Gọi hàm init_services()
# - Tự động load cấu hình trong file .env, cụ thể thể load các biến môi trường:  
# DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, DB_NAME_CONFIG, DB_NAME_STAGING, DB_NAME_DW, EMAIL_USERNAME, 
# EMAIL_PASSWORD, EMAIL_SIMULATE, EMAIL_ADMIN, DEFAULT_RETRY
# - Khởi tạo lần lượt các  service theo danh sách truyển vào (config_load_staging, staging_db, log_db, email_service)
# Với config_load_staging - đọc cấu hình cho LOAD_STAGING
# Với staging_db - kết nối cơ dữ liệu staging để load dữ liệu tạm
# Với log_db - kết nối cơ sở dữ liệu log, kiểm tra log EXTRACT và ghi load LOAD_STAGING
# Với email_service - gửi thông báo khi có sự cố trong quá trình hoàn tất
    services = init_services(['config_load_staging_db', 'staging_db', 'log_db', 'email_service'])
    config_db = services['config_load_staging_db']
    staging_db = services['staging_db']
    log_db = services['log_db']
    email_service = services['email_service']
    
    try:
# 3. Lấy log EXTRACT mới nhất
# Gọi latest_extract_log = log_db.get_latest_log("EXTRACT", None), 
# gọi tới cơ sở dữ liệu log để lấy bảng ghi log EXTRACT gần nhất
        latest_extract_log = log_db.get_latest_log("EXTRACT", None)
        # 3.1. Kiểm trạng thái log EXTRACT "SUCCESS ?"       
        if not latest_extract_log or latest_extract_log.get("status") != "SUCCESS":
            #3.1(NO) Ghi log: "Log EXTRACT mới nhất chưa thành công, bỏ qua quá trình LOAD_STAGING."
            log_message(
                log_db,
                "LOAD_STAGING",
                None,
                "WARNING",
                message="Log EXTRACT mới nhất chưa thành công, bỏ qua quá trình LOAD_STAGING.",
            )
            print("Quá trình LOAD_STAGING bị bỏ qua vì EXTRACT chưa thành công.")
            return
#3.1 (YES) 4. Gọi config_db.getactive_configs()
# Lấy danh sách cấu hình config_load_stanging có is_active = true
        configs = config_db.get_active_configs()
        # 4.1 Có config nào active không ?
        if not configs:
            #4.1 (NO) Ghi log "Không có config load staging nào đang active."
            log_message(
                log_db,
                "LOAD_STAGING",
                None,
                "WARNING",
                message="Không có config load staging nào đang active.",
            )
            return
#4.1 (YES) 5. Thực hiện Truncate bảng staging 
# Lặp qua tất cả target_table từ các config active
# Thực thi staging_db.truncate_table(table) 
        staging_tables = {cfg["target_table"] for cfg in configs}
        for table in staging_tables:
            # 5.1 Truncate thành công ?
            try:
                staging_db.truncate_table(table)
                #5.1 (YES) Ghi log "Đã truncate bảng {table} trước khi load."
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    None,
                    "SUCCESS",
                    message=f"Đã truncate bảng {table} trước khi load.",
                )
            except Exception as e:
                #5.1 (NO) Ghi log:  "Không thể truncate bảng {table}"
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    None,
                    "WARNING",
                    message=f"Không thể truncate bảng {table}: {e}",
                )
# Còn config khác không ? (YES) 6. Lặp qua từng config
# Đối với mỗi config active:
# Ghi log READY ("Bắt đầu xử lý config load staging.")
# Đặt retry_count = 0, load_success = False
        for config in configs:
            config_id = config["id"]
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "READY",
                message="Bắt đầu xử lý config load staging.",
            )
            load_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3
# 6.1. Triển khai vòng lặp while. 
# Kiểm tra điều kiện retry
# retry_count < max_retries & load_success == False
            while not load_success and retry_count < max_retries:
                try:
#6.1 (YES) 7. Gọi hàm load_csv_to_staging(config, staging_db, log_db)
# Hàm thực hiện đọc file CSV mới nhất từ thư mục gốc, kiểm tra dữ liệu, và nạp vào bảng staging 
                    load_csv_to_staging(config, staging_db, log_db)
                    load_success = True
                    emails = config.get("emails")
                    print(config)
                    if not emails:
                        emails = []
                    email_service.send_email(
                    to_addrs=emails,
                    subject=f"test",
                    body=f"test",
                    )
                    
                except Exception as e:
#6.1 (NO) Ghi log "Lỗi lần {retry_count}: {e}"
# Gửi log qua email EmailService.send_email(
# "[ETL] Lỗi Load staging (config ID={config_id}",
# "Lỗi khi lòa dữ liệu staging:\n\n{e}")
# Tăng retry_count +=1
                    retry_count += 1
                    log_message(
                        log_db,
                        "LOAD_STAGING",
                        config_id,
                        "FAILURE",
                        message=f"Lỗi lần {retry_count}: {e}",
                    )
                    emails = config.get("emails")
                    if not emails:
                        emails = []
                    email_service.send_email(
                        to_addrs=emails,
                        subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
                        body=f"Lỗi tổng thể trong process_config:\n\n{e}",
                    )
# 10. Kiểm tra load_success == True ?
            if not load_success:
                # 10 (NO) Ghi log: "Load staging thất bại sau {max_retries} lần retry."
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    config_id,
                    "FAILURE",
                    message=f"Load staging thất bại sau {max_retries} lần retry.",
                )

    except Exception as e:
        log_message(
            log_db, "LOAD_STAGING", None, "FAILURE", f"Lỗi tổng thể trong main(): {e}"
        )
# 10 (YES) Còn config khác không ? (NO) 11. Kết thúc toàn bộ quá trình
# Đóng kết nối config_db.close(), staging_dbclose(), log_dbclose()
# Ghi log tổng kết "Kết thúc quá trình LOAD STAGING."
    finally:  
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD STAGING.")


if __name__ == "__main__":
    main()
