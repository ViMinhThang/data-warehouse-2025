import os
import pandas as pd
from utils.logger_util import log_message


def get_latest_csv_file(source_path: str, log_db=None, config_id=None):
    try:      
# 7.2.1. Kiểm tra thư mục nguồn source_path tồn tại không ?
        if not os.path.exists(source_path):
            #7.2.1 (NO) Ghi log: "Thu mục nguồn {source_path} không tồn tại." 
            raise FileNotFoundError(f"Thư mục nguồn {source_path} không tồn tại.")
#7.2.1 (YES) 7.2.2. Lấy danh sách file CSV
# Lọc tất cả file .csv trong thư mục (.endswith(".csv")) 
# Sắp xếp theo thời gian sửa đổi mới nhất đến cũ nhất 
# ( sorted([...] key=os.path.getmtime, reverse=True,) 
        csv_files = sorted(
            [
                os.path.join(source_path, f)
                for f in os.listdir(source_path)
                if f.endswith(".csv")
            ],
            key=os.path.getmtime,
            reverse=True,
        )
# 7.2.2.1. Có file CSV nào không?
        if not csv_files:
#7.2.2.1 (NO) Ghi log: "Không có file CSV nào trong {source_path}"
            raise FileNotFoundError(f"Không có file CSV nào trong {source_path}")
#7.2.2.1 (YES) 7.2.3. Lấy file CSV mới nhất lastest_file = cvs_files[0]
        latest_file = csv_files[0]
        if log_db:
              # 7.2.4. Ghi log: "Tìm thấy file CSV mới nhất: {latest_file}"       
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "SUCCESS",
                message=f"Tìm thấy file CSV mới nhất: {latest_file}",
            )
# 7.2.5. return latest_file
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
# 7.4.1. Thực hiện đọc file CSV 
# (df=pd.read_csv(file_path, delimiter=delimiter, header=0 if has_header else None)
        df = pd.read_csv(
            file_path, delimiter=delimiter, header=0 if has_header else None
        )
# 7.4.2. Kiểm tra DataFrame rỗng (df.empty)
# df.empty == True ?
        if df.empty:
            #7.4.2 (YES) Ghi log: "File CSV '{file_path}' rỗng, không có dữ liệu."
            raise ValueError(f"File CSV '{file_path}' rỗng, không có dữ liệu.")

        if log_db:          
# 7.4.3. Ghi log: SUCCESS – "Đọc thành công {len(df)} dòng từ file {file_path}"
            log_message(
                log_db,
                "LOAD_STAGING",
                config_id,
                "SUCCESS",
                message=f"Đọc thành công {len(df)} dòng từ file {file_path}",
            )
# 7.4.4. Trả về df
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