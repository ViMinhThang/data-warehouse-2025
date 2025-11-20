import os
import shutil
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from db.config_extract_db import ConfigExtractDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.extract_util import (
    build_records_from_df,
    compute_stock_indicators,
    fetch_yfinance_data,
    parse_tickers,
)
from utils.logger_util import log_message

# 2.load_env() load các biến môi trường
#    DB_HOST,DB_USER DB_PASSWORD,DB_PORT,DB_NAME_STAGING,
#    DB_NAME_CONFIG,DB_NAME_STAGING,DB_NAME_DW,
#    EMAIL_USERNAME,EMAIL_PASSWORD, EMAIL_SIMULATEEMAIL_ADMIN,
#    DEFAULT_RETRY=3
load_dotenv()


def init_services():
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME_CONFIG"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    config_db = ConfigExtractDatabase(**db_params)
    log_db = LogDatabase(**db_params)
    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    print("Đã khởi tạo thành công các service.")
    return config_db, log_db, email_service


def extract_ticker_data(ticker, period, interval, config_id, log_db):
    # 10.8.7.1 Ghi log PROCESSING với message "Đang extract {ticker}..."."""
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Đang extract {ticker}...",
    )
    # 10.8.7.2 Gọi fetch_yfinance_data(ticker, period, interval) để lấy dữ liệu thô từ Yahoo Finance..
    #        Ghi log để thông báo đang lấy dữ liệu.  Gọi yf.download() để tải dữ liệu theo ticker, period, interval.  Nếu không có dữ liệu → trả về DataFrame rỗng.  Chuẩn hóa timezone của index về UTC (nếu chưa có timezone thì gán, nếu có thì chuyển về UTC).  Trả về DataFrame chứa dữ liệu giá đã chuẩn hóa.
    data = fetch_yfinance_data(ticker, period, interval)
    # 10.8.7.3 Kiểm tra dữ liệu trả về có rỗng không?
    if data.empty:
        # 10.8.7.4 Ném ra ValueError.
        raise ValueError(f"Không có dữ liệu trả về cho {ticker} từ Yahoo Finance.")

    # 10.8.7.5  Gọi compute_stock_indicators(data, ticker) để tính các chỉ báo kỹ thuật.
    #        Lấy cột Close và Volume tương ứng với mã cổ phiếu.  Tính Diff: mức chênh lệch giá đóng cửa so với ngày trước đó.  Tính PercentChangeClose: phần trăm thay đổi giá đóng cửa so với ngày trước đó.  Trả về một DataFrame mới chứa các chỉ số trên.
    indicators = compute_stock_indicators(data, ticker)

    # 10.8.7.6  Gọi build_records_from_df(ticker, indicators) để chuyển đổi DataFrame thành danh sách các bản ghi.
    #        Lặp qua từng dòng trong DataFrame.  Với mỗi dòng, tạo một dict gồm:  ticker: mã cổ phiếu  datetime_utc: thời điểm của dòng dữ liệu (index của DataFrame)  close, volume, diff, percent_change_close: các giá trị tương ứng trong dòng  extracted_at: thời điểm trích xuất (thời gian hiện tại UTC)  Trả về một danh sách các dict.
    records = build_records_from_df(ticker, indicators)

    # 10.8.7.7 Ghi log PROCESSING với message "Extract {ticker} thành công.".
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Extract {ticker} thành công.",
    )
    # 10.8.7.8 Trả về danh sách các bản ghi.
    return records


# 10.8.1 Bắt đầu hàm run_crawl_data_with_config


def run_crawl_data_with_config(config, log_db, config_id):
    # 10.8.2 Lấy danh sách tickers, period, interval từ cấu hình.
    tickers = parse_tickers(config.get("tickers", []))
    period = config.get("period", "1mo")
    interval = config.get("interval", "1d")
    # 10.8.3 Ghi log PROCESSING với message "Bắt đầu crawl dữ liệu cho {len(tickers)} ticker.".
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Bắt đầu crawl dữ liệu cho {len(tickers)} ticker.",
    )
    # 10.8.4 Khởi tạo danh sách rỗng all_rows = [].
    all_rows = []
    # 10.8.5 Bắt đầu vòng lặp for qua từng ticker
    for ticker in tickers:
        # 10.8.6  Bắt đầu khối try...except để xử lý lỗi cho từng ticker.
        try:
            # 10.8.7 Gọi extract_ticker_data(ticker, ...) để lấy dữ liệu cho một mã cổ phiếu.
            records = extract_ticker_data(ticker, period, interval, config_id, log_db)
            # 10.8.8 Thêm các bản ghi (records) trả về vào all_rows(append)
            all_rows.extend(records)
        except Exception as e:
            # 10.8.6.1 Ghi log FAILURE với message "Lỗi khi extract {ticker}: {lỗi}".
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi khi extract {ticker}: {e}",
            )
    # 10.8.9 Kiểm tra all_rows có rỗng không?
    if not all_rows:
        # 10.8.10 Nếu rỗng (không lấy được dữ liệu của ticker nào): Ném ra RuntimeError để hàm process_config bắt được và thực hiện retry.
        raise RuntimeError("Không có dữ liệu hợp lệ cho bất kỳ ticker nào.")

    # 10.8.11 Tạo DataFrame df từ all_rows
    df = pd.DataFrame(all_rows).round(4)
    # 10.8.12 Ghi log PROCESSING với message "Crawl thành công {len(df)} bản ghi.".
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Crawl thành công {len(df)} bản ghi.",
    )
    # 10.8.13 Trả về DataFrame df.
    return df


# 10.9.1 Bắt đầu hàm save_extract_result


def save_extract_result(df, config, config_id, log_db):
    # 10.9.2 Tạo thư mục output_path nếu nó chưa tồn tại.
    raw_path = os.path.join(config["output_path"], "")
    os.makedirs(raw_path, exist_ok=True)

    # 10.9.3 Tạo tên file duy nhất dựa trên config_id và timestamp hiện tại (ví dụ: 123_20231027_103000.csv).
    file_name = f"{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(raw_path, file_name)

    # 10.9.4 Ghi DataFrame df ra file CSV tại đường dẫn đã tạo.
    df.round(4).to_csv(file_path, index=False)

    # 10.9.5 Ghi log SUCCESS với message "Đã ghi file kết quả: {file_path}".
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "SUCCESS",
        message=f"Đã ghi file kết quả: {file_path}",
    )

    # 10.9.6 Trả về đường dẫn file vừa tạo.
    return file_path


# 10.1 Bắt đầu hàm process_config


def process_config(config, log_db, email_service):
    # 10.2Lấy config_id từ cấu hình.Ghi log READY với message "Bắt đầu xử lý config.".
    config_id = config["id"]
    log_message(log_db, "EXTRACT", config_id, "READY", message="Bắt đầu xử lý config.")

    # 10.3.Lấy output_path từ cấu hình.
    output_path = config.get("output_path")

    # 10.4 output_path có tồn tại không?
    if output_path and os.path.exists(output_path):
        shutil.rmtree(output_path)

        # 10.4.1.Xóa toàn bộ thư mục và nội dung bên trong (shutil.rmtree).
        #            Ghi log PROCESSING với message "Đã xóa thư mục output: {output_path}".
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "PROCESSING",
            message=f"Đã xóa thư mục output: {output_path}",
        )

    # 10.5 Khởi tạo craw_success = False, retry_count = 0.Lấy số lần thử lại tối đa max_retries từ cấu hình (mặc định là 3).
    craw_success = False
    retry_count = 0
    max_retries = config.get("retry_count", 3) or 3

    # 10.6.Bắt đầu vòng lặp while để thử lại khi thất bại (while not craw_success and retry_count < max_retries)
    while not craw_success and retry_count < max_retries:
        # 10.7.Bắt đầu khối try...except cho mỗi lần thử.
        try:
            # 10.8.Gọi run_crawl_data_with_config(config, ...) để crawl dữ liệu.
            df = run_crawl_data_with_config(config, log_db, config_id)

            # 10.9. Gọi save_extract_result(df, ...) để lưu kết quả trả về từ bước trên.

            save_extract_result(df, config, config_id, log_db)
            # 10.10.Đặt craw_success = True để thoát khỏi vòng lặp while
            craw_success = True
        except Exception as e:
            # 10.7.1 Tăng retry_count lên 1.
            #            Ghi log FAILURE với message "Lỗi lần {retry_count}/{max_retries}: {lỗi}".
            #            Gửi email cho admin thông báo về lỗi lần thử này.
            retry_count += 1
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi lần {retry_count}/{max_retries}: {e}",
            )
            emails = config.get("emails")
            if not emails:
                emails = []
            email_service.send_email(
                to_addrs=emails,
                subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
                body=f"Lỗi tổng thể trong process_config:\n\n{e}",
            )
    # 10.11 Kết thúc vòng while
    # 10.12 Kiểm tra craw_success.

    if craw_success:
        # 10.13 Ghi log SUCCESS với message "Extract hoàn tất thành công.".
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "SUCCESS",
            message="Extract hoàn tất thành công.",
        )
    else:
        # 10.14 Ghi log FAILURE với message "Thất bại sau khi retry tối đa.".
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


def main():
    # 1.Bắt đầu chương trình chính (main) In ra màn hình Bắt đầu quá trình extract
    print("=== Bắt đầu quá trình EXTRACT ===")

    # 3.khởi tạo dịch vụ trong hàm init_services()
    #        Thiết lập thông số kết nối cơ sở dữ liệu từ biến môi trường (host, dbname, user, password, port).
    #        Khởi tạo đối tượng ConfigExtractDatabase để tương tác với bảng cấu hình.
    #        Khởi tạo đối tượng LogDatabase để ghi log vào DB.
    #        Khởi tạo đối tượng EmailService để gửi email thông báo.
    #        In ra màn hình thông báo Đã khởi tạo thành công các service.
    #        Trả về các dịch vụ đã khởi tạo: config_db, log_db, email_service
    config_db, log_db, email_service = init_services()

    # 4.Khởi tạo biến success = 0 và fail = 0 để đếm số cấu hình xử lý thành công/thất bại.
    success, fail = 0, 0

    # 5.Bắt đầu khối try...except...finally trong main để xử lý lỗi tổng thể.
    try:
        # 6.Gọi config_db.get_active_configs() để lấy các cấu hình có trạng thái "active" từ DB.
        configs = config_db.get_active_configs()

        # 7.Kiểm tra xem có cấu hình nào không?
        if not configs:
            # 7.1.Nếu không có (configs rỗng):Ghi log FAILURE với message "Không có config nào đang active.".Kết thúc hàm main.
            log_message(
                log_db,
                "EXTRACT",
                None,
                "FAILURE",
                message="Không có config nào đang active.",
            )
            return

        # 8.Bắt đầu vòng lặp for qua từng config trong danh sách configs
        for config in configs:
            # 9.Bắt đầu khối try...except để xử lý lỗi cho từng config riêng lẻ.
            try:
                # 10.Gọi hàm process_config(config, log_db, email_service) để xử lý một cấu hình.
                process_config(config, log_db, email_service)

                # 11.1 Tăng biến success lên 1
                success += 1

                # 9.1Ghi Log FAILURE lỗi xử lý config gửi email
            except Exception as e:
                # 11.2 Tăng biến fail lên 1. Ghi log FAILURE .Gửi email cho admin.
                fail += 1
                log_message(
                    log_db,
                    "EXTRACT",
                    config.get("id"),
                    "FAILURE",
                    message=f"Lỗi xử lý config ID={config.get('id')}: {e}",
                )
                emails = config.get("emails")
                if not emails:
                    emails = []
                email_service.send_email(
                    to_addrs=emails,
                    subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
                    body=f"Lỗi tổng thể trong process_config:\n\n{e}",
                )

        # 5.1 Ghi log lỗi tổng thể trong main
    except Exception as e:
        log_message(
            log_db,
            "EXTRACT",
            None,
            "FAILURE",
            message=f"Lỗi tổng thể trong main(): {e}",
        )

    finally:
        # 12.Đóng kết nối config_db.Đóng kết nối log_db.
        #        Ghi log INFO cuối cùng với message "Hoàn tất EXTRACT — Thành công: {success}, Thất bại: {fail}".
        #        In ra màn hình "Kết thúc quá trình EXTRACT.".
        config_db.close()
        log_db.close()
        log_message(
            None,
            "EXTRACT",
            None,
            "INFO",
            message=f"Hoàn tất EXTRACT — Thành công: {success}, Thất bại: {fail}",
        )
        print("Kết thúc quá trình EXTRACT.")


if __name__ == "__main__":
    main()
