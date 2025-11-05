import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from utils.service_util import init_services
from utils.logger_util import log_message
from utils.extract_util import (
    parse_tickers,
    compute_stock_indicators,
    fetch_yfinance_data,
    build_records_from_df,
)

load_dotenv()


def extract_ticker_data(ticker, period, interval, config_id, log_db):
    log_message(
        log_db, "EXTRACT", config_id, "PROCESSING", ticker, f"Đang extract {ticker}..."
    )

    data = fetch_yfinance_data(ticker, period, interval)
    if data.empty:
        raise ValueError("Không có dữ liệu trả về từ Yahoo Finance")

    indicators = compute_stock_indicators(data, ticker)
    records = build_records_from_df(ticker, indicators)
    log_message(
        log_db, "EXTRACT", config_id, "SUCCESS", ticker, f"Extract {ticker} thành công"
    )
    return records


def run_crawl_data_with_config(config, log_db, config_id):
    tickers = parse_tickers(config.get("tickers", []))
    period = config.get("period", "1mo")
    interval = config.get("interval", "1d")

    all_rows = []

    for ticker in tickers:
        try:
            records = extract_ticker_data(ticker, period, interval, config_id, log_db)
            all_rows.extend(records)
        except Exception as e:
            log_message(
                log_db, "EXTRACT", config_id, "FAILURE", ticker, error_message=str(e)
            )

    if not all_rows:
        raise RuntimeError("Không có dữ liệu hợp lệ cho bất kỳ ticker nào.")

    df = pd.DataFrame(all_rows).round(4)
    log_message(
        log_db,
        "EXTRACT",
        config_id,
        "SUCCESS",
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
        log_db, "EXTRACT", config_id, "SUCCESS", message=f"Đã ghi file {file_path}"
    )
    return file_path


def process_config(config, log_db, email_service):
    config_id = config["id"]
    log_message(log_db, "EXTRACT", config_id, "READY", message="Bắt đầu xử lý config.")

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
                error_message=f"Lỗi lần {retry_count}: {e}",
            )
            email_service.send_email(
                to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                subject=f"Lỗi Extract config ID={config_id}",
                body=f"Lỗi khi crawl dữ liệu:\n\n{e}",
            )

    if craw_success:
        log_message(
            log_db, "EXTRACT", config_id, "SUCCESS", message="Extract hoàn tất."
        )
    else:
        log_message(
            log_db,
            "EXTRACT",
            config_id,
            "FAILURE",
            message="Thất bại sau khi retry tối đa.",
        )


def main():
    print("=== Bắt đầu quá trình EXTRACT ===")
    services = init_services(['config_extract_db', 'log_db', 'email_service'])
    config_db = services['config_extract_db']
    log_db = services['log_db']
    email_service = services['email_service']

    try:
        configs = config_db.get_active_configs()
        if not configs:
            log_message(
                log_db,
                "EXTRACT",
                None,
                "WARNING",
                message="Không có config nào đang active.",
            )
            return

        for config in configs:
            process_config(config, log_db, email_service)

    except Exception as e:
        log_message(
            log_db,
            "EXTRACT",
            None,
            "FAILURE",
            error_message=f"Lỗi tổng thể trong main(): {e}",
        )
    finally:
        config_db.close()
        log_db.close()
        print("Kết thúc quá trình EXTRACT.")


if __name__ == "__main__":
    main()
