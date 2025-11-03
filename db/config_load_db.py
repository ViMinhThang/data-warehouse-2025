import logging
from typing import Any, Dict, List
from db.base_db import BaseDatabase

class ConfigLoadDWDatabase(BaseDatabase):
    def get_active_configs(self) -> List[Dict[str, Any]]:
        query = """
        SELECT id, source_table, target_table, load_mode, is_active
        FROM config_load_dw
        WHERE is_active = TRUE;
        """
        configs = self.execute_query(query)
        logging.info(f"Found {len(configs)} active DW load configs.")
        return configs

    def mark_config_status(self, config_id: int, status: str):
        query = """
        UPDATE config_load_dw
        SET note = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """
        self.execute_non_query(query, (f"Status: {status}", config_id))
        logging.info(f"Updated DW load config ID={config_id} to {status}")
