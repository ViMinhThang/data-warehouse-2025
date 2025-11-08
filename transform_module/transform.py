import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv

from db.config_transform_db import ConfigTransformDatabase
from db.transform_db import TransformDatabase
from db.log_db import LogDatabase
from email_service.email_service import EmailService
from utils.transform_utils.rsi import calculate_rsi
from utils.transform_utils.roc import calculate_roc
from utils.transform_utils.bb import calculate_bb
from utils.logger_util import log_message

# ==========================
# 1️SETUP
# ==========================
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="\033[92m%(asctime)s [%(levelname)s]\033[0m %(message)s",
    handlers=[logging.StreamHandler()],
)


def init_services():
    """Khởi tạo kết nối DB và email service."""
    db_params = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname_config": os.getenv("DB_NAME_CONFIG", "config"),
        "dbname_staging": os.getenv("DB_NAME_STAGING", "staging"),
    }

    cfg_tech_db = ConfigTransformDatabase(
        host=db_params["host"],
        dbname=db_params["dbname_config"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    transform_db = TransformDatabase(
        host=db_params["host"],
        dbname=db_params["dbname_staging"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    log_db = LogDatabase(
        host=db_params["host"],
        dbname=db_params["dbname_config"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    email_service = EmailService(
        username=os.getenv("EMAIL_USERNAME"),
        password=os.getenv("EMAIL_PASSWORD"),
        simulate=os.getenv("EMAIL_SIMULATE", "True").lower() == "true",
    )

    return cfg_tech_db, transform_db, log_db, email_service


# ==========================
# 2️TRANSFORM
# ==========================
def run_transformations(cfg, transform_db):
    source_table = cfg["source_table"]
    destination_table = cfg["destination_table"]
    logging.info(f"Running transformations from {source_table} to {destination_table}")

    df = transform_db.read_data(source_table)
    logging.info(f"Read {len(df)} rows from {source_table}")
    if df.empty:
        raise ValueError(f"Source table {source_table} is empty.")

    # Group by ticker to apply transformations
    transformed_dfs = []
    for ticker, group in df.groupby("ticker"):
        group = group.sort_values(by="datetime_utc").reset_index(drop=True)

        logging.info(f"Calculating indicators for {ticker}...")
        if cfg.get("rsi_window"):
            logging.info(f"  - RSI(window={cfg['rsi_window']})")
            group["rsi"] = calculate_rsi(group, window=cfg["rsi_window"])

        if cfg.get("roc_window"):
            logging.info(f"  - ROC(window={cfg['roc_window']})")
            group["roc"] = calculate_roc(group, window=cfg["roc_window"])

        if cfg.get("bb_window"):
            logging.info(f"  - BB(window={cfg['bb_window']})")
            group["bb_upper"], group["bb_lower"] = calculate_bb(
                group, window=cfg["bb_window"]
            )

        transformed_dfs.append(group)

    if not transformed_dfs:
        raise ValueError("No data transformed.")

    final_df = pd.concat(transformed_dfs)

    # Drop rows with NaN values from indicator calculations
    initial_rows = len(final_df)
    final_df.dropna(inplace=True)
    logging.info(f"Dropped {initial_rows - len(final_df)} rows with NaN values.")

    # Select and reorder columns to match destination table
    dest_columns = [
        "ticker",
        "datetime_utc",
        "close",
        "volume",
        "diff",
        "percent_change_close",
        "rsi",
        "roc",
        "bb_upper",
        "bb_lower",
        "source_type",
    ]

    # Filter final_df to only include columns that exist in both dataframes
    final_df_columns = final_df.columns
    columns_to_write = [col for col in dest_columns if col in final_df_columns]
    final_df = final_df[columns_to_write]

    # Write to destination table
    transform_db.write_data(final_df, destination_table, if_exists="append")

    logging.info(
        f"Successfully transformed and loaded {len(final_df)} records to {destination_table}"
    )


# ==========================
# 3️MAIN
# ==========================
def main():
    cfg_tech_db, transform_db, log_db, email_service = init_services()
    log_message(None, "TRANSFORM", None, "INFO", message="Bắt đầu quá trình TRANSFORM")
    success, fail = 0, 0

    try:
        tech_configs = cfg_tech_db.get_active_configs()

        # Truncate destination table(s) before starting
        if tech_configs:
            # Get all unique destination tables
            dest_tables = {cfg["destination_table"] for cfg in tech_configs}
            for table in dest_tables:
                transform_db.truncate_table(table)
                logging.info(f"Truncated table {table} before transformations.")

        for cfg in tech_configs:
            try:
                cfg_tech_db.mark_config_status(cfg["id"], "PROCESSING")
                log_message(
                    log_db,
                    "TRANSFORM",
                    cfg["id"],
                    "PROCESSING",
                    message="Bắt đầu transform.",
                )
                run_transformations(cfg, transform_db)
                cfg_tech_db.mark_config_status(cfg["id"], "SUCCESS")
                log_message(
                    log_db,
                    "TRANSFORM",
                    cfg["id"],
                    "SUCCESS",
                    message="Hoàn tất transform.",
                )
                success += 1
            except Exception as e:
                fail += 1
                cfg_tech_db.mark_config_status(cfg["id"], "FAILURE")
                log_message(
                    log_db, "TRANSFORM", cfg["id"], "FAILURE", error_message=str(e)
                )
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL Transform] Lỗi Config ID={cfg['id']}",
                    body=f"Lỗi transform:\n{e}",
                )

    finally:
        cfg_tech_db.close()
        transform_db.close()
        log_db.close()
        log_message(
            None,
            "TRANSFORM",
            None,
            "INFO",
            message=f"Hoàn tất TRANSFORM — Thành công: {success}, Thất bại: {fail}",
        )


if __name__ == "__main__":
    main()
