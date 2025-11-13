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
