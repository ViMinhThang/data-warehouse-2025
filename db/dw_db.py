from sqlalchemy import create_engine
import pandas as pd
import logging

from db.base_db import BaseDatabase  # giả sử BaseDatabase quản lý psycopg2 connection


class DWDatabase(BaseDatabase):
    """
    Lớp DWDatabase để:
    - Gọi procedure, execute SQL
    - Sử dụng pandas read/write thông qua SQLAlchemy
    """

    def __init__(self, host, dbname, user, password, port=5432):
        super().__init__(host, dbname, user, password, port)
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )

    def call_procedure(self, procedure_name: str, params: tuple = ()):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"CALL {procedure_name}({', '.join(['%s']*len(params))});", params
                )
                self.conn.commit()
            logging.info(
                f" Procedure {procedure_name} executed successfully with params {params}"
            )
        except Exception as e:
            logging.error(f" Error executing procedure {procedure_name}: {e}")
            raise
