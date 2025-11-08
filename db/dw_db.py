
import logging
import pandas as pd
from sqlalchemy import create_engine
from db.base_db import BaseDatabase


class DWDatabase(BaseDatabase):
    def __init__(self, host, dbname, user, password, port=5432):
        super().__init__(host, dbname, user, password, port)
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )

    def sync_dim_stock(self, df: pd.DataFrame):
        logging.info("Đồng bộ dim_stock...")
        dim_stock_df = pd.DataFrame(df["ticker"].unique(), columns=["ticker"])

        with self.engine.begin() as conn:
            existing_stock_df = pd.read_sql("SELECT ticker FROM dim_stock", conn)
            new_stock_df = dim_stock_df[
                ~dim_stock_df["ticker"].isin(existing_stock_df["ticker"])
            ]
            if not new_stock_df.empty:
                new_stock_df.to_sql(
                    "dim_stock", conn, if_exists="append", index=False, method="multi"
                )
                logging.info(f"Thêm {len(new_stock_df)} ticker mới vào dim_stock.")
            else:
                logging.info("Không có ticker mới trong dim_stock.")

    def sync_dim_datetime(self, df: pd.DataFrame):
        logging.info("Đồng bộ dim_datetime...")
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])
        dim_datetime_df = pd.DataFrame(df["datetime_utc"].unique(), columns=["date"])
        dim_datetime_df["year"] = dim_datetime_df["date"].dt.year
        dim_datetime_df["month"] = dim_datetime_df["date"].dt.month
        dim_datetime_df["day"] = dim_datetime_df["date"].dt.day
        dim_datetime_df["hour"] = dim_datetime_df["date"].dt.hour
        dim_datetime_df["weekday"] = dim_datetime_df["date"].dt.day_name()

        with self.engine.begin() as conn:
            existing_dt_df = pd.read_sql("SELECT date FROM dim_datetime", conn)
            existing_dt_df["date"] = pd.to_datetime(existing_dt_df["date"])
            new_dt_df = dim_datetime_df[
                ~dim_datetime_df["date"].isin(existing_dt_df["date"])
            ]
            if not new_dt_df.empty:
                new_dt_df.to_sql(
                    "dim_datetime", conn, if_exists="append", index=False, method="multi"
                )
                logging.info(f"Thêm {len(new_dt_df)} bản ghi mới vào dim_datetime.")
            else:
                logging.info("Không có ngày mới trong dim_datetime.")

    def load_fact_stock_indicators(self, df: pd.DataFrame, target_table: str):
        logging.info(f"Đồng bộ {target_table}...")

        with self.engine.begin() as conn:
            dim_stock_dw_df = pd.read_sql("SELECT stock_id, ticker FROM dim_stock", conn)
            dim_datetime_dw_df = pd.read_sql(
                "SELECT datetime_id, date FROM dim_datetime", conn
            )
            dim_datetime_dw_df["date"] = pd.to_datetime(dim_datetime_dw_df["date"])

        fact_df = df.merge(dim_stock_dw_df, on="ticker")
        fact_df = fact_df.merge(
            dim_datetime_dw_df, left_on="datetime_utc", right_on="date"
        )

        fact_df = fact_df[
            [
                "stock_id",
                "datetime_id",
                "close",
                "volume",
                "diff",
                "percent_change_close",
                "rsi",
                "roc",
                "bb_upper",
                "bb_lower",
            ]
        ]

        with self.engine.begin() as conn:
            existing_fact = pd.read_sql(
                f"SELECT stock_id, datetime_id FROM {target_table}", conn
            )
            merged = fact_df.merge(
                existing_fact, on=["stock_id", "datetime_id"], how="left", indicator=True
            )
            new_rows = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

            if new_rows.empty:
                logging.info(f"Không có dữ liệu mới cần insert vào {target_table}.")
                return 0
            else:
                new_rows.to_sql(
                    target_table,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                logging.info(f"Thêm {len(new_rows)} bản ghi mới vào {target_table}.")
                return len(new_rows)

    def close(self):
        super().close()
        if self.engine:
            self.engine.dispose()
            logging.info("Closed DW DB engine")
