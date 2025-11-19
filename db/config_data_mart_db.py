import logging
from typing import Any, Dict, List
from db.base_db import BaseDatabase


class ConfigDMDatabase(BaseDatabase):
    """
    Lớp quản lý kết nối và lấy cấu hình cho quá trình Load Data Mart.
    Tương tác với bảng: config_load_datamart
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_active_configs(self) -> List[Dict[str, Any]]:
        """
        Lấy danh sách các bước cấu hình (Steps) đang active.
        Quan trọng: Kết quả được sắp xếp theo 'execution_order' tăng dần
        để đảm bảo quy trình ETL chạy đúng trình tự (Fact -> Summary -> Volatility).
        """
        query = """
        SELECT 
            id, 
            procedure_name, 
            description, 
            execution_order, 
            is_critical, 
            retry_count, 
            is_active
        FROM config_load_datamart
        WHERE is_active = true
        ORDER BY execution_order ASC;
        """

        try:
            configs = self.execute_query(query)
            logging.info(
                f"Fetched {len(configs)} active steps from config_load_datamart."
            )
            return configs
        except Exception as e:
            logging.error(f"Error fetching active configs: {e}")
            return []
