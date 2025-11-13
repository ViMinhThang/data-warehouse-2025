import logging
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigTransformDatabase(BaseDatabase):
    def get_active_configs(self) -> List[Dict[str, Any]]:
        """Trả về danh sách các cấu hình transform active"""
        query = """
        SELECT id, rsi_window, roc_window, bb_window,
               source_table, procedure_transform,
               dim_path, fact_path
        FROM config_transform
        WHERE is_active = TRUE;
        """
        configs = self.execute_query(query)
        logging.info(f"Found {len(configs)} active transform configs.")
        return configs

    def get_latest_active_config(self) -> Dict[str, Any]:
        """Lấy cấu hình transform active mới nhất, bao gồm đường dẫn export"""
        query = """
        SELECT rsi_window, roc_window, bb_window,
               dim_path, fact_path,
               source_table, procedure_transform
        FROM config_transform
        WHERE is_active = TRUE
        ORDER BY id DESC
        LIMIT 1;
        """
        result = self.execute_query(query)
        if not result:
            raise ValueError("Không tìm thấy cấu hình transform nào active")
        return result[0]
