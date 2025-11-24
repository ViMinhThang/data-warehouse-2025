#!/bin/bash

# 1. Cấu hình đường dẫn
BASE_DIR="/home/fragile/data-warehouse-2025"
LOG_DIR="$BASE_DIR"
PYTHON_EXEC="$BASE_DIR/venv/bin/python"

# 2. Di chuyển vào thư mục dự án (BẮT BUỘC để fix lỗi ModuleNotFoundError)
cd "$BASE_DIR" || exit 1

# 3. Thêm thư mục hiện tại vào PYTHONPATH (BẮT BUỘC cho Crontab)
export PYTHONPATH="$BASE_DIR"

# --- Bắt đầu chạy các tiến trình ---

# === 1. Extract ===
# In thông báo start ra màn hình VÀ ghi vào file log
echo "$(date) - Start extract_module" | tee -a "$LOG_DIR/extract.log"

# Chạy Python, bắt cả lỗi (2>&1), hiển thị ra màn hình VÀ ghi vào file log
$PYTHON_EXEC -m extract_module.extract 2>&1 | tee -a "$LOG_DIR/extract.log"

echo "$(date) - End extract" | tee -a "$LOG_DIR/extract.log"
echo "------------------------------------------------"


# === 2. Load Staging ===
echo "$(date) - Start load_staging_module" | tee -a "$LOG_DIR/load_staging.log"
$PYTHON_EXEC -m load_staging_module.load_staging 2>&1 | tee -a "$LOG_DIR/load_staging.log"
echo "$(date) - End load_staging" | tee -a "$LOG_DIR/load_staging.log"
echo "------------------------------------------------"


# === 3. Transform ===
echo "$(date) - Start transform" | tee -a "$LOG_DIR/transform.log"
$PYTHON_EXEC -m transform_module.transform 2>&1 | tee -a "$LOG_DIR/transform.log"
echo "$(date) - End transform" | tee -a "$LOG_DIR/transform.log"
echo "------------------------------------------------"


# === 4. Load Warehouse ===
echo "$(date) - Start load_warehouse" | tee -a "$LOG_DIR/load_warehouse.log"
$PYTHON_EXEC -m load_warehouse_module.load_warehouse_module 2>&1 | tee -a "$LOG_DIR/load_warehouse.log"
echo "$(date) - End load_warehouse_module" | tee -a "$LOG_DIR/load_warehouse_module.log"
echo "------------------------------------------------"


# === 5. Load Data Mart ===
echo "$(date) - Start load_data_mart" | tee -a "$LOG_DIR/load_data_mart.log"
$PYTHON_EXEC -m load_data_mart_module.load_data_mart_module 2>&1 | tee -a "$LOG_DIR/load_data_mart.log"
echo "$(date) - End load_data_mart" | tee -a "$LOG_DIR/load_data_mart.log"
echo "------------------------------------------------"