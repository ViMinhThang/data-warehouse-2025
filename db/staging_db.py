import io
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any, List, Dict
import pandas as pd
from sqlalchemy import create_engine
from db.base_db import BaseDatabase


class StagingDatabase(BaseDatabase):
    def __init__(self, host, dbname, user, password, port=5432):
        super().__init__(host, dbname, user, password, port)
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )

    def read_data(self, table_name: str) -> pd.DataFrame:
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
            logging.info(f"Successfully read {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            logging.error(f"Error reading data from {table_name}: {e}")
            raise

    def copy_from_dataframe(self, df, table_name):

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        with self.conn.cursor() as cursor:
            cursor.copy_expert(
                f"""
                COPY {table_name} (
                    ticker,
                    datetime_utc,
                    close,
                    volume,
                    diff,
                    percent_change_close,
                    extracted_at
                )
                FROM STDIN WITH CSV HEADER DELIMITER ','
                """,
                buffer,
            )

        self.conn.commit()

    def insert_records(self, table_name: str, records: List[Dict[str, Any]]):
        """
        Insert list dict (records) vào bảng staging.
        Ví dụ: insert_records("stg_market_prices", [{"ticker": "AAPL", "close": 173.5}, ...])
        """
        if not records:
            logging.warning(f"Không có record nào để insert vào {table_name}")
            return

        try:
            columns = records[0].keys()
            col_names = ", ".join(columns)
            values_template = ", ".join(["%s"] * len(columns))

            query = f"INSERT INTO {table_name} ({col_names}) VALUES ({values_template})"

            with self.conn.cursor() as cursor:
                cursor.executemany(query, [tuple(r.values()) for r in records])

            self.conn.commit()
            logging.info(f"Đã insert {len(records)} bản ghi vào bảng {table_name}")

        except Exception as e:
            logging.error(f"Lỗi khi insert records vào {table_name}: {e}")
            self.conn.rollback()
            raise

    def truncate_table(self, table_name: str):
        """Truncates a table in the staging database."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            logging.info(f"Table {table_name} truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating table {table_name}: {e}")
            self.conn.rollback()
            raise

    def drop_table(self, table_name: str):
        """Drops a table in the staging database."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            logging.info(f"Table {table_name} dropped successfully.")
        except Exception as e:
            logging.error(f"Error dropping table {table_name}: {e}")
            self.conn.rollback()
            raise

    def close(self):
        super().close()
        if self.engine:
            self.engine.dispose()
            logging.info("Closed Staging DB engine")
