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


# ==========================
# 2️ EXTRACT LOGIC
# ==========================


def extract_ticker_data(ticker, period, interval, config_id, log_db):
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Đang extract {ticker}...",
    )

    data = fetch_yfinance_data(ticker, period, interval)
    if data.empty:
        raise ValueError(f"Không có dữ liệu trả về cho {ticker} từ Yahoo Finance.")

    indicators = compute_stock_indicators(data, ticker)
    records = build_records_from_df(ticker, indicators)

    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Extract {ticker} thành công.",
    )
    return records


def run_crawl_data_with_config(config, log_db, config_id):
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
    for ticker in tickers:
        try:
            records = extract_ticker_data(ticker, period, interval, config_id, log_db)
            all_rows.extend(records)
        except Exception as e:
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi khi extract {ticker}: {e}",
            )

    if not all_rows:
        raise RuntimeError("Không có dữ liệu hợp lệ cho bất kỳ ticker nào.")

    df = pd.DataFrame(all_rows).round(4)
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "PROCESSING",
        message=f"Crawl thành công {len(df)} bản ghi.",
    )
    return df


def save_extract_result(df, config, config_id, log_db):
    raw_path = os.path.join(config["output_path"], "")
    os.makedirs(raw_path, exist_ok=True)

    file_name = f"{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(raw_path, file_name)

    df.round(4).to_csv(file_path, index=False)
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "SUCCESS",
        message=f"Đã ghi file kết quả: {file_path}",
    )
    return file_path


def process_config(config, log_db, email_service):
    config_id = config["id"]
    log_message(log_db, "EXTRACT", config_id, "READY", message="Bắt đầu xử lý config.")

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

    craw_success = False
    retry_count = 0
    max_retries = config.get("retry_count", 3) or 3
    while not craw_success and retry_count < max_retries:
        try:
            df = run_crawl_data_with_config(config, log_db, config_id)
            save_extract_result(df, config, config_id, log_db)
            craw_success = True
        except Exception as e:
            retry_count += 1
            log_message(
                log_db,
                "EXTRACT",
                config_id,
                "FAILURE",
                message=f"Lỗi lần {retry_count}/{max_retries}: {e}",
            )
            email_service.send_email(
                to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                subject=f"[ETL Extract] Lỗi Config ID={config_id}",
                body=f"Lỗi khi crawl dữ liệu:\n\n{e}",
            )

    if craw_success:
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "SUCCESS",
            message="Extract hoàn tất thành công.",
        )
    else:
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
    print("=== Bắt đầu quá trình EXTRACT ===")
    config_db, log_db, email_service = init_services()

    success, fail = 0, 0

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "EXTRACT",
                None,
                "FAILURE",
                message="Không có config nào đang active.",
            )
            return

        for config in configs:
            try:
                process_config(config, log_db, email_service)
                success += 1
            except Exception as e:
                fail += 1
                log_message(
                    log_db,
                    "EXTRACT",
                    config.get("id"),
                    "FAILURE",
                    message=f"Lỗi xử lý config ID={config.get('id')}: {e}",
                )
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL Extract] Lỗi Config ID={config.get('id')}",
                    body=f"Lỗi tổng thể trong process_config:\n\n{e}",
                )

    except Exception as e:
        log_message(
            log_db,
            "EXTRACT",
            None,
            "FAILURE",
            message=f"Lỗi tổng thể trong main(): {e}",
        )

    finally:
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
