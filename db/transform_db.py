from sqlalchemy import create_engine
import pandas as pd
import logging

from db.base_db import BaseDatabase


class TransformDatabase(BaseDatabase):

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

    def write_data(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logging.info(f"Successfully wrote {len(df)} rows to {table_name}")
        except Exception as e:
            logging.error(f"Error writing data to {table_name}: {e}")
            raise

    def truncate_table(self, table_name: str):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            logging.info(f"Table {table_name} truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating table {table_name}: {e}")
            raise
