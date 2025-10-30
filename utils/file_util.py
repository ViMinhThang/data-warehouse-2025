import os
import pandas as pd
from utils.logger_util import log_message


def get_latest_csv_file(source_path: str, log_db=None, config_id=None):

    try:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Thư mục nguồn {source_path} không tồn tại.")

        csv_files = sorted(
            [
                os.path.join(source_path, f)
                for f in os.listdir(source_path)
                if f.endswith(".csv")
            ],
            key=os.path.getmtime,
            reverse=True,
        )

        if not csv_files:
            raise FileNotFoundError(f"Không có file CSV nào trong {source_path}")

        latest_file = csv_files[0]

        if log_db:
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "SUCCESS",
                message=f"Tìm thấy file CSV mới nhất: {latest_file}",
            )

        return latest_file

    except Exception as e:
        if log_db:
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "FAILURE",
                error_message=f"Lỗi khi lấy file CSV mới nhất: {e}",
            )
        raise


def read_csv_file(
    file_path: str,
    delimiter: str = ",",
    has_header: bool = True,
    log_db=None,
    config_id=None,
):

    try:
        df = pd.read_csv(
            file_path, delimiter=delimiter, header=0 if has_header else None
        )

        if df.empty:
            raise ValueError(f"File CSV '{file_path}' rỗng, không có dữ liệu.")

        if log_db:
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "SUCCESS",
                message=f"Đọc thành công {len(df)} dòng từ file {file_path}",
            )

        return df

    except Exception as e:
        if log_db:
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "FAILURE",
                error_message=f"Lỗi khi đọc file CSV {file_path}: {e}",
            )
        raise
