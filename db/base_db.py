import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any, List, Dict


class BaseDatabase:

    def __init__(
        self, host: str, dbname: str, user: str, password: str, port: int = 5432
    ):
        try:
            self.conn = psycopg2.connect(
                host=host, dbname=dbname, user=user, password=password, port=port
            )
            self.conn.autocommit = True
            logging.info("Connected to PostgreSQL successfully")
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Thực thi query SELECT và trả về list dict"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_non_query(self, query: str, params: tuple = None) -> None:
        """Thực thi query INSERT/UPDATE/DELETE"""
        with self.conn.cursor() as cur:
            cur.execute(query, params)

    def close(self):
        """Đóng kết nối"""
        if self.conn:
            self.conn.close()
            logging.info("Closed DB connection")
