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

        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            message=f"Đang xử lý {target_table}...",
        )

        latest_file = get_latest_csv_file(source_path, log_db, config_id)
        log_message(
            log_db,
            "LOAD_STAGING",
            config_id,
            "PROCESSING",
            message=f"Đang load file: {latest_file}",
        )

        df = read_csv_file(latest_file, delimiter, has_header, log_db, config_id)
        if df.empty:
            raise ValueError(f"File {latest_file} rỗng, không có dữ liệu để load.")

        # Chỉ append (vì truncate đã được thực hiện 1 lần trước đó)
        staging_db.copy_from_dataframe(df, target_table)

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


# =====================================================
#  QUY TRÌNH CHÍNH
# =====================================================


def main():
    print("=== Bắt đầu quá trình LOAD STAGING ===")
    services = init_services(['config_load_staging_db', 'staging_db', 'log_db', 'email_service'])
    config_db = services['config_load_staging_db']
    staging_db = services['staging_db']
    log_db = services['log_db']
    email_service = services['email_service']

    try:
        latest_extract_log = log_db.get_latest_log("EXTRACT", None)
        if not latest_extract_log or latest_extract_log.get("status") != "SUCCESS":
            log_message(
                log_db,
                "LOAD_STAGING",
                None,
                "WARNING",
                message="Log EXTRACT mới nhất chưa thành công, bỏ qua quá trình LOAD_STAGING.",
            )
            print("Quá trình LOAD_STAGING bị bỏ qua vì EXTRACT chưa thành công.")
            return

        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "LOAD_STAGING",
                None,
                "WARNING",
                message="Không có config load staging nào đang active.",
            )
            return

        # Truncate tất cả bảng staging 1 lần duy nhất
        staging_tables = {cfg["target_table"] for cfg in configs}
        for table in staging_tables:
            try:
                staging_db.truncate_table(table)
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    None,
                    "SUCCESS",
                    message=f"Đã truncate bảng {table} trước khi load.",
                )
            except Exception as e:
                log_message(
                    log_db,
                    "LOAD_STAGING",
                    None,
                    "WARNING",
                    message=f"Không thể truncate bảng {table}: {e}",
                )

        # Sau đó load tuần tự từng config
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

            while not load_success and retry_count < max_retries:
                try:
                    load_csv_to_staging(config, staging_db, log_db)
                    load_success = True
                except Exception as e:
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

            if not load_success:
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

    finally:
        emails = config.get("emails")
        if not emails:
            emails = []
        email_service.send_email(
            to_addrs=emails,
            subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
            body=f"Lỗi tổng thể trong process_config:\n\n{e}",
        )
        config_db.close()
        staging_db.close()
        log_db.close()
        print("Kết thúc quá trình LOAD STAGING.")


if __name__ == "__main__":
    main()
