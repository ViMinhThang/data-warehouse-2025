import logging
from datetime import datetime
from typing import Any, Dict, Optional
from db.base_db import BaseDatabase


class LogDatabase(BaseDatabase):

    def insert_log(
        self,
        process_name: str,
        config_id: Optional[int],
        status: str,
        message: str = None,
    ):

        query = """
        INSERT INTO logs (stage, config_id, status,log_level, message,error_message, create_time,update_time)
        VALUES (%s, %s, %s, %s, %s);
        """
        try:
            self.execute_non_query(
                query, (process_name, config_id, status, message, datetime.utcnow())
            )
            logging.info(f"🪵 Inserted log: {process_name} [{status}]")
        except Exception as e:
            logging.error(f"Failed to insert log: {e}")

    def get_latest_log(
        self, process_name: str, config_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        query = """
        SELECT * FROM log
        WHERE stage = %s AND (config_id = %s OR %s IS NULL)
        ORDER BY created_at DESC
        LIMIT 1;
        """
        rows = self.execute_query(query, (process_name, config_id, config_id))
        return rows[0] if rows else None
