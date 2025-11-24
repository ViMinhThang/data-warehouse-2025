#!/bin/bash

# 1. Định nghĩa thư mục gốc
BASE_DIR="/home/fragile/data-warehouse-2025"
LOG_DIR="$BASE_DIR"

# 2. QUAN TRỌNG: Phải di chuyển vào thư mục dự án để python -m hoạt động
cd "$BASE_DIR" || exit 1

# 3. Sử dụng đường dẫn tuyệt đối đến Python trong venv (Không cần source activate)
PYTHON_EXEC="$BASE_DIR/venv/bin/python"

# --- Bắt đầu chạy các tiến trình ---

# Module Extract
echo "$(date) - Start extract_module" >> "$LOG_DIR/extract.log"
# Thêm 2>&1 để ghi cả lỗi (nếu có) vào log file
$PYTHON_EXEC -m extract_module.extract >> "$LOG_DIR/extract.log" 2>&1
echo "$(date) - End extract" >> "$LOG_DIR/extract.log"

# Module Load Staging
echo "$(date) - Start load_staging_module" >> "$LOG_DIR/load_staging.log"
$PYTHON_EXEC -m load_staging_module.load_staging >> "$LOG_DIR/load_staging.log" 2>&1
echo "$(date) - End load_staging" >> "$LOG_DIR/load_staging.log"

# Module Transform
echo "$(date) - Start transform" >> "$LOG_DIR/transform.log"
$PYTHON_EXEC -m transform_module.transform >> "$LOG_DIR/transform.log" 2>&1
echo "$(date) - End transform" >> "$LOG_DIR/transform.log"

# Module Load Warehouse
echo "$(date) - Start load_warehouse" >> "$LOG_DIR/load_warehouse.log"
$PYTHON_EXEC -m load_warehouse_module.load_warehouse_module >> "$LOG_DIR/load_warehouse.log" 2>&1
echo "$(date) - End load_warehouse_module" >> "$LOG_DIR/load_warehouse_module.log"

# Module Load Data Mart
echo "$(date) - Start load_data_mart" >> "$LOG_DIR/load_data_mart.log"
$PYTHON_EXEC -m load_data_mart_module.load_data_mart_module >> "$LOG_DIR/load_data_mart.log" 2>&1
echo "$(date) - End load_data_mart" >> "$LOG_DIR/load_data_mart.log"