#!/bin/bash

# 1. Định nghĩa thư mục gốc
BASE_DIR="/home/fragile/data-warehouse-2025"
LOG_DIR="$BASE_DIR"

# 2. Di chuyển vào thư mục
cd "$BASE_DIR" || exit 1

# 3. [QUAN TRỌNG NHẤT] Thêm thư mục hiện tại vào PYTHONPATH
# Dòng này giúp Python tìm thấy extract_module, transform_module...
export PYTHONPATH="$BASE_DIR"

# 4. Đường dẫn Python
PYTHON_EXEC="$BASE_DIR/venv/bin/python"

# --- Bắt đầu chạy ---

echo "$(date) - Start extract_module" >> "$LOG_DIR/extract.log"
$PYTHON_EXEC -m extract_module.extract >> "$LOG_DIR/extract.log" 2>&1
echo "$(date) - End extract" >> "$LOG_DIR/extract.log"

echo "$(date) - Start load_staging_module" >> "$LOG_DIR/load_staging.log"
$PYTHON_EXEC -m load_staging_module.load_staging >> "$LOG_DIR/load_staging.log" 2>&1
echo "$(date) - End load_staging" >> "$LOG_DIR/load_staging.log"

echo "$(date) - Start transform" >> "$LOG_DIR/transform.log"
$PYTHON_EXEC -m transform_module.transform >> "$LOG_DIR/transform.log" 2>&1
echo "$(date) - End transform" >> "$LOG_DIR/transform.log"

echo "$(date) - Start load_warehouse" >> "$LOG_DIR/load_warehouse.log"
$PYTHON_EXEC -m load_warehouse_module.load_warehouse_module >> "$LOG_DIR/load_warehouse.log" 2>&1
echo "$(date) - End load_warehouse_module" >> "$LOG_DIR/load_warehouse_module.log"

echo "$(date) - Start load_data_mart" >> "$LOG_DIR/load_data_mart.log"
$PYTHON_EXEC -m load_data_mart_module.load_data_mart_module >> "$LOG_DIR/load_data_mart.log" 2>&1
echo "$(date) - End load_data_mart" >> "$LOG_DIR/load_data_mart.log"