import os
import time
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from utils.service_util import init_services
from utils.extract_util import compute_stock_indicators


# ==========================
# 1️SETUP
# ==========================
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="\033[92m%(asctime)s [%(levelname)s]\033[0m %(message)s",
    handlers=[logging.StreamHandler()],
)


# ==========================
# 2️TRANSFORM STAGING (BASIC)
# ==========================
def run_transform_staging(cfg, engine):
    config_id = cfg["id"]
    source = cfg["source_table"]
    dest = cfg["destination_table"]
    logging.info(f"Transform STAGING {source} → {dest}")

    start = time.time()
    df = pd.read_sql(f"SELECT * FROM {source}", engine)
    if df.empty:
        raise ValueError(f"Bảng {source} trống, không có dữ liệu transform.")

    try:
        transformations = json.loads(cfg.get("transformations", "[]"))
        for step in transformations:
            action = step.get("action")
            if action == "rename":
                df.rename(columns={step["from"]: step["to"]}, inplace=True)
            elif action == "drop":
                df.drop(columns=step["columns"], inplace=True, errors="ignore")
            elif action == "convert_type":
                df[step["column"]] = df[step["column"]].astype(step["to_type"])
    except Exception as e:
        logging.warning(f"Lỗi khi thực hiện transform JSON: {e}")

    # Ghi vào bảng đích
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {dest};"))
    df.to_sql(dest, engine, if_exists="append", index=False)

    duration = round(time.time() - start, 2)
    logging.info(f"Transform STAGING thành công {len(df)} bản ghi ({duration}s).")
    return len(df), duration


# ==========================
# 3️TRANSFORM TECHNICAL (RSI, ROC,...)
# ==========================
def run_transform_technical(cfg, engine):
    config_id = cfg["id"]
    source = cfg["source_table"]
    dest = cfg["destination_table"]
    logging.info(f"Transform TECHNICAL {source} → {dest}")

    start = time.time()
    df = pd.read_sql(f"SELECT * FROM {source}", engine)
    if df.empty:
        raise ValueError(f"Bảng {source} trống, không có dữ liệu transform.")

    try:
        df_trans = compute_stock_indicators(df, ticker_col="ticker")
    except Exception as e:
        logging.warning(f"Lỗi khi tính chỉ báo: {e}")
        df_trans = df.copy()

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {dest};"))
    df_trans.to_sql(dest, engine, if_exists="append", index=False)

    duration = round(time.time() - start, 2)
    logging.info(
        f"Transform TECHNICAL thành công {len(df_trans)} bản ghi ({duration}s)."
    )
    return len(df_trans), duration


# ==========================
# 4️MAIN
# ==========================
def main():
    logging.info("Bắt đầu quá trình TRANSFORM (STAGING + TECHNICAL)")
    services = init_services([
        'config_transform_staging_db',
        'config_transform_db',
        'log_db',
        'email_service',
        'staging_engine'
    ])
    cfg_stg_db = services['config_transform_staging_db']
    cfg_tech_db = services['config_transform_db']
    log_db = services['log_db']
    email_service = services['email_service']
    engine = services['staging_engine']
    success, fail = 0, 0

    try:
        # === Bước 1: Transform STAGING ===
        stg_configs = cfg_stg_db.get_active_configs()
        for cfg in stg_configs:
            try:
                cfg_stg_db.mark_config_status(cfg["id"], "PROCESSING")
                log_db.insert_log(
                    "TRANSFORM_STAGING",
                    cfg["id"],
                    "PROCESSING",
                    "Bắt đầu transform staging.",
                )
                run_transform_staging(cfg, engine)
                cfg_stg_db.mark_config_status(cfg["id"], "SUCCESS")
                log_db.insert_log(
                    "TRANSFORM_STAGING",
                    cfg["id"],
                    "SUCCESS",
                    "Hoàn tất transform staging.",
                )
                success += 1
            except Exception as e:
                fail += 1
                cfg_stg_db.mark_config_status(cfg["id"], "FAILURE")
                log_db.insert_log("TRANSFORM_STAGING", cfg["id"], "FAILURE", str(e))
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL Transform Staging] Lỗi Config ID={cfg['id']}",
                    body=f"Lỗi transform staging:\n{e}",
                )

        # === Bước 2: Transform TECHNICAL ===
        tech_configs = cfg_tech_db.get_active_configs()
        for cfg in tech_configs:
            try:
                cfg_tech_db.mark_config_status(cfg["id"], "PROCESSING")
                log_db.insert_log(
                    "TRANSFORM_TECH",
                    cfg["id"],
                    "PROCESSING",
                    "Bắt đầu transform kỹ thuật.",
                )
                run_transform_technical(cfg, engine)
                cfg_tech_db.mark_config_status(cfg["id"], "SUCCESS")
                log_db.insert_log(
                    "TRANSFORM_TECH",
                    cfg["id"],
                    "SUCCESS",
                    "Hoàn tất transform kỹ thuật.",
                )
                success += 1
            except Exception as e:
                fail += 1
                cfg_tech_db.mark_config_status(cfg["id"], "FAILURE")
                log_db.insert_log("TRANSFORM_TECH", cfg["id"], "FAILURE", str(e))
                email_service.send_email(
                    to_addrs=[os.getenv("EMAIL_ADMIN", "admin@example.com")],
                    subject=f"[ETL Transform Technical] Lỗi Config ID={cfg['id']}",
                    body=f"Lỗi transform kỹ thuật:\n{e}",
                )

    finally:
        cfg_stg_db.close()
        cfg_tech_db.close()
        log_db.close()
        engine.dispose()
        logging.info(f"Hoàn tất TRANSFORM — Thành công: {success}, Thất bại: {fail}")


if __name__ == "__main__":
    main()
