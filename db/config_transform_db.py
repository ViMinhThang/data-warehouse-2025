import logging
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigTransformDatabase(BaseDatabase):
    def get_active_configs(self) -> List[Dict[str, Any]]:
        query = """
        SELECT id, rsi_window, roc_window, bb_window,
               source_table, destination_table
        FROM config_transform
        WHERE is_active = TRUE;
        """
        configs = self.execute_query(query)
        logging.info(f"Found {len(configs)} active transform configs.")
        return configs
