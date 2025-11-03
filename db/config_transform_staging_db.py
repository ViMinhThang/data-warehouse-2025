import logging
from typing import Any, Dict, List
from db.base_db import BaseDatabase

class ConfigTransformStagingDatabase(BaseDatabase):
    """Quản lý cấu hình transform giữa các bảng staging."""

    def get_active_configs(self) -> List[Dict[str, Any]]:
        query = """
        SELECT id, transform_name, description, source_table, destination_table, transformations
        FROM config_transform_staging
        WHERE is_active = TRUE;
        """
        configs = self.execute_query(query)
        logging.info(f"📄 Found {len(configs)} active transform_staging configs.")
        return configs

    def mark_config_status(self, config_id: int, status: str):
        query = """
        UPDATE config_transform_staging
        SET note = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """
        self.execute_non_query(query, (f"Status: {status}", config_id))
        logging.info(f"🪄 Updated transform_staging config ID={config_id} to {status}")
