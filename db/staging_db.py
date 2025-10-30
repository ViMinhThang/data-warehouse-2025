import io
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any, List, Dict
import pandas as pd
from db.base_db import BaseDatabase


class StagingDatabase(BaseDatabase):

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
            # Xây dựng query động
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
