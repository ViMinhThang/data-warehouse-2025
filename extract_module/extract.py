import os
import logging
from datetime import datetime
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from db.config_extract_db import ConfigExtractDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.util import (
    parse_tickers,
    compute_stock_indicators,
    fetch_yfinance_data,
    build_records_from_df,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


load_dotenv()


def init_services():
    """Khởi tạo tất cả service (DB, LogDB, Email)."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    try:
        config_db = ConfigExtractDatabase(**db_params)
        log_db = LogDatabase(**db_params)

        email_service = EmailService(
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
            simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
        )

        logging.info("Đã khởi tạo thành công các service.")
        return config_db, log_db, email_service

    except Exception as e:
        logging.error(f"Lỗi khi khởi tạo service: {e}")
        raise


def run_crawl_data_with_config(config):
    try:
        tickers = parse_tickers(config.get("tickers", []))
        period = config.get("period", "1mo")
        interval = config.get("interval", "1d")

        logging.info(
            f"Bắt đầu crawl dữ liệu: {tickers}, period={period}, interval={interval}"
        )

        all_rows = []

        for ticker in tickers:
            data = fetch_yfinance_data(ticker, period, interval)
            if data.empty:
                continue

            indicators = compute_stock_indicators(data, ticker)
            records = build_records_from_df(ticker, indicators)
            all_rows.extend(records)

        if not all_rows:
            raise Exception("Không lấy được dữ liệu nào từ Yahoo Finance.")

        df = pd.DataFrame(all_rows)
        logging.info(f"Crawl thành công {len(df)} bản ghi từ {len(tickers)} ticker.")
        return df

    except Exception as e:
        logging.error(f"Lỗi trong run_crawl_data_with_config: {e}")
        raise


# ========================
# Hàm chính
# ========================
def main():
    logging.info("Bắt đầu quá trình EXTRACT")

    config_db, log_db, email_service = init_services()

    try:
        configs = config_db.get_active_configs()
        if not configs:
            logging.warning("Không có config nào đang active.")
            return

        for config in configs:
            config_id = config["id"]
            log_db.insert_log("extract", config_id, "READY")

            craw_success = False
            retry_count = 0
            max_retries = config.get("retry_count", 3) or 3

            while not craw_success and retry_count < max_retries:
                try:
                    log_db.insert_log("extract", config_id, "PROCESSING")

                    data = run_crawl_data_with_config(config)

                    raw_path = os.path.join(config["output_path"], "raw")
                    os.makedirs(raw_path, exist_ok=True)

                    file_name = (
                        f"{config_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
                    file_path = os.path.join(raw_path, file_name)

                    data.round(4).to_csv(file_path, index=False)
                    logging.info(f"Đã ghi file {file_path}")

                    log_db.insert_log("extract", config_id, "SUCCESS")
                    craw_success = True

                except Exception as e:
                    retry_count += 1
                    log_db.insert_log("extract", config_id, "FAILURE", str(e))
                    logging.error(f"❌ Lỗi lần {retry_count}/{max_retries}: {e}")

                    email_service.send_email(
                        to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                        subject=f"Lỗi Extract config ID={config_id}",
                        body=f"Lỗi khi crawl dữ liệu:\n\n{e}",
                    )

            if craw_success:
                logging.info(f"Extract thành công cho config ID={config_id}")
            else:
                logging.warning(f"Extract thất bại sau {max_retries} lần retry.")

    except Exception as e:
        logging.error(f"Lỗi tổng thể trong main(): {e}")

    finally:
        config_db.close()
        log_db.close()
        logging.info("Kết thúc quá trình EXTRACT.")


if __name__ == "__main__":
    main()
