import logging
from datetime import datetime
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigDWDatabase(BaseDatabase):
    """
    Quản lý bảng config_load_datawarehouse
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._in_memory_logs = []

    def get_active_configs(self) -> List[Dict[str, Any]]:
        """
        Lấy danh sách các config đang active
        """
        query = """
        SELECT id, dim_path,fact_path,procedure, is_active, created_at, updated_at, emails
        FROM config_load_datawarehouse
        WHERE is_active = TRUE;
        """
        configs = self.execute_query(query)
        logging.info(
            f"Fetched {len(configs)} active config_load_datawarehouse records."
        )
        return configs
