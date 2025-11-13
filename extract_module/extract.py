import os
import shutil
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from db.config_extract_db import ConfigExtractDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.logger_util import log_message
from utils.extract_util import (
    parse_tickers,
    compute_stock_indicators,
    fetch_yfinance_data,
    build_records_from_df,
)

# ==========================
# 1️ SETUP
# ==========================
"""1. Khởi tạo: Load biến môi trường từ file .env"""
load_dotenv()


def init_services():
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    """3. Khởi tạo DB: Tạo instance ConfigExtractDatabase"""
    config_db = ConfigExtractDatabase(**db_params)
    """4. Khởi tạo DB: Tạo instance LogDatabase"""
    log_db = LogDatabase(**db_params)
    """5. Khởi tạo Email: Tạo instance EmailService"""
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo thành công các service.")
    return config_db, log_db, email_service


# ==========================
# 2️ EXTRACT LOGIC
# ==========================
"""17. Extract ticker: Xử lý dữ liệu cho từng ticker"""


def extract_ticker_data(ticker, period, interval, config_id, log_db):
    """18. Log processing: Ghi log PROCESSING khi bắt đầu extract"""
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Đang extract {ticker}...",
    )

    """19. Gọi API: Gọi fetch_yfinance_data() để lấy dữ liệu từ Yahoo Finance"""
    data = fetch_yfinance_data(ticker, period, interval)
    """20. Kiểm tra data: Kiểm tra nếu DataFrame rỗng"""
    if data.empty:
        """21. Xử lý lỗi ticker: Raise ValueError nếu không có dữ liệu"""
        raise ValueError(f"Không có dữ liệu trả về cho {ticker} từ Yahoo Finance.")

    """22. Tính chỉ báo: Tính toán các chỉ báo kỹ thuật"""
    indicators = compute_stock_indicators(data, ticker)
    """23. Build records: Chuyển đổi DataFrame sang records"""
    records = build_records_from_df(ticker, indicators)

    """24. Log success: Ghi log SUCCESS sau khi extract thành công ticker"""
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Extract {ticker} thành công.",
    )
    return records


"""14. Chạy crawl: Chạy quy trình crawl dữ liệu theo config"""


def run_crawl_data_with_config(config, log_db, config_id):
    """15. Parse tickers: Phân tích danh sách tickers từ config"""
    tickers = parse_tickers(config.get("tickers", []))
    period = config.get("period", "1mo")
    interval = config.get("interval", "1d")

    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Bắt đầu crawl dữ liệu cho {len(tickers)} ticker.",
    )

    all_rows = []
    """16. Vòng lặp ticker: Lặp qua từng ticker để xử lý"""
    for ticker in tickers:
        try:
            """17. Extract ticker: Gọi extract_ticker_data() cho từng ticker"""
            records = extract_ticker_data(ticker, period, interval, config_id, log_db)
            all_rows.extend(records)
        except Exception as e:
            """25. Xử lý lỗi từng ticker: Log lỗi nếu extract ticker thất bại"""
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi khi extract {ticker}: {e}",
            )

    """26. Kiểm tra dữ liệu: Kiểm tra nếu all_rows rỗng"""
    if not all_rows:
        """27. Raise lỗi: Raise RuntimeError nếu không có dữ liệu hợp lệ"""
        raise RuntimeError("Không có dữ liệu hợp lệ cho bất kỳ ticker nào.")

    """28. Tạo DataFrame: Tạo DataFrame từ all_rows và làm tròn 4 chữ số"""
    df = pd.DataFrame(all_rows).round(4)
    """29. Log crawl success: Ghi log số bản ghi crawl thành công"""
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Crawl thành công {len(df)} bản ghi.",
    )
    return df


"""30. Lưu kết quả: Lưu DataFrame ra file CSV"""


def save_extract_result(df, config, config_id, log_db):
    """31. Tạo thư mục: Tạo thư mục output nếu chưa tồn tại"""
    raw_path = os.path.join(config["output_path"], "")
    os.makedirs(raw_path, exist_ok=True)

    """32. Tạo tên file: Tạo tên file theo format configId_timestamp.csv"""
    file_name = f"{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(raw_path, file_name)

    """33. Ghi CSV: Ghi DataFrame ra file CSV"""
    df.round(4).to_csv(file_path, index=False)
    """34. Log lưu file: Ghi log sau khi lưu file thành công"""
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "SUCCESS",
        message=f"Đã ghi file kết quả: {file_path}",
    )
    return file_path


"""10. Bắt đầu xử lý: Xử lý từng config với retry mechanism"""


def process_config(config, log_db, email_service):
    config_id = config["id"]
    """10.1. Log READY: Ghi log READY khi bắt đầu xử lý config"""
    log_message(log_db, "EXTRACT", config_id, "READY", message="Bắt đầu xử lý config.")

    """11. Dọn dẹp: Xóa thư mục output cũ nếu tồn tại"""
    output_path = config.get("output_path")
    if output_path and os.path.exists(output_path):
        shutil.rmtree(output_path)
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "PROCESSING",
            message=f"Đã xóa thư mục output: {output_path}",
        )

    """12. Khởi tạo retry: Khởi tạo biến điều khiển vòng lặp retry"""
    craw_success = False
    retry_count = 0
    max_retries = config.get("retry_count", 3) or 3
    """13. Vòng lặp retry: Lặp cho đến khi thành công hoặc hết retry"""
    while not craw_success and retry_count < max_retries:
        try:
            """14. Chạy crawl: Gọi run_crawl_data_with_config() để crawl dữ liệu"""
            df = run_crawl_data_with_config(config, log_db, config_id)
            """30. Lưu kết quả: Gọi save_extract_result() để lưu file CSV"""
            save_extract_result(df, config, config_id, log_db)
            """35. Đánh dấu thành công: Đặt craw_success = True để thoát vòng lặp"""
            craw_success = True
        except Exception as e:
            """37. Catch exception: Bắt lỗi trong quá trình crawl"""
            retry_count += 1
            """38. Log retry: Ghi log lỗi theo số lần retry"""
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi lần {retry_count}/{max_retries}: {e}",
            )
            """39. Gửi email lỗi: Gửi email cảnh báo khi crawl thất bại"""
            email_service.send_email(
                to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                subject=f"[ETL Extract] Lỗi Config ID={config_id}",
                body=f"Lỗi khi crawl dữ liệu:\n\n{e}",
            )

    """40. Kiểm tra kết quả: Kiểm tra kết quả sau khi kết thúc vòng lặp retry"""
    if craw_success:
        """41. Log hoàn thành: Ghi log SUCCESS nếu extract thành công"""
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "SUCCESS",
            message="Extract hoàn tất thành công.",
        )
    else:
        """42. Log thất bại: Ghi log FAILURE nếu hết retry mà vẫn thất bại"""
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "FAILURE",
            message="Thất bại sau khi retry tối đa.",
        )


# ==========================
# 3️ MAIN
# ==========================
"""3️ MAIN: Hàm chính điều phối toàn bộ quy trình EXTRACT"""


def main():
    print("=== Bắt đầu quá trình EXTRACT ===")
    """2. Setup Service: Gọi init_services() để khởi tạo các service"""
    config_db, log_db, email_service = init_services()

    """6. Lấy config: Lấy danh sách config active từ database"""
    success, fail = 0, 0

    try:
        """6. Lấy config: Gọi config_db.get_active_configs()"""
        configs = config_db.get_active_configs()
        """7. Kiểm tra config: Kiểm tra nếu không có config nào active"""
        if not configs:
            """8. Log và kết thúc: Ghi log và return nếu không có config"""
            log_message(
                log_db,
                "EXTRACT",
                None,
                "FAILURE",
                message="Không có config nào đang active.",
            )
            return

        """9. Vòng lặp config: Lặp qua từng config để xử lý"""
        for config in configs:
            try:
                """10. Bắt đầu xử lý: Gọi process_config() để xử lý từng config"""
                process_config(config, log_db, email_service)
                """44. Đếm thành công: Tăng biến đếm success"""
                success += 1
            except Exception as e:
                """50. Catch config lỗi: Bắt lỗi nếu xử lý config thất bại"""
                """45. Đếm thất bại: Tăng biến đếm fail"""
                fail += 1
                """50. Log config lỗi: Ghi log lỗi xử lý config"""
                log_message(
                    log_db,
                    "EXTRACT",
                    config.get("id"),
                    "FAILURE",
                    message=f"Lỗi xử lý config ID={config.get('id')}: {e}",
                )
                """51. Gửi email config lỗi: Gửi email cảnh báo lỗi config"""
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
                    body=f"Lỗi tổng thể trong process_config:\n\n{e}",
                )

    except Exception as e:
        """50. Log lỗi tổng thể: Bắt lỗi ngoại lệ trong main()"""
        log_message(
            log_db,
            "EXTRACT",
            None,
            "FAILURE",
            message=f"Lỗi tổng thể trong main(): {e}",
        )

    finally:
        """47. Dọn dẹp: Đóng kết nối database trong khối finally"""
        config_db.close()
        log_db.close()
        """48. Log kết quả: Ghi log tổng kết với success và fail"""
        log_message(
            None,
            "EXTRACT",
            None,
            "INFO",
            message=f"Hoàn tất EXTRACT — Thành công: {success}, Thất bại: {fail}",
        )
        """49. In kết thúc: In thông báo kết thúc ra console"""
        print("Kết thúc quá trình EXTRACT.")


if __name__ == "__main__":
    """MAIN: Chạy hàm main() khi file được execute trực tiếp"""
    main()
