import io
import logging
from datetime import datetime
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigLoadStagingDatabase(BaseDatabase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._in_memory_logs = []

    def get_active_configs(self) -> List[Dict[str, Any]]:
        query = """
        SELECT id, source_path, target_table, file_type, has_header, delimiter,
               load_mode, retry_count, is_active
        FROM config_load_staging
        WHERE is_active = true;
        """
        configs = self.execute_query(query)
        logging.info(f"Fetched {len(configs)} active config_load_staging records.")
        return configs
