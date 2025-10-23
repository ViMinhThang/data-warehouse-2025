import logging
from datetime import datetime
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigExtractDatabase(BaseDatabase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._in_memory_logs = []

    def get_active_configs(self) -> List[Dict[str, Any]]:
        query = """
        SELECT id, tickers, is_active, output_path, period, interval, retry_count
        FROM config_extract
        WHERE is_active = true;
        """
        configs = self.execute_query(query)
        logging.info(f"📄 Fetched {len(configs)} active config_extract records.")
        return configs

    def get_today_config(self, config_id: int) -> bool:
        return any(
            log["config_id"] == config_id and log["date"] == datetime.now().date()
            for log in self._in_memory_logs
        )

    def create_today_config(self, config_id: int):
        self._in_memory_logs.append(
            {
                "config_id": config_id,
                "date": datetime.now().date(),
                "status": "READY_EXTRACT",
            }
        )
