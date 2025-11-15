#!/bin/bash
# ----------------------------
# Script chạy nhiều tiến trình Python tuần tự
# ----------------------------

source /home/fragile/data-warehouse-2025/venv/bin/activate

echo "$(date) - Start extract_module" >> /home/fragile/data-warehouse-2025/extract.log
python -m  extract_module.extract
echo "$(date) - End extract" >> /home/fragile/data-warehouse-2025/extract.log 2

echo "$(date) - Start load_staging_module" >> /home/fragile/data-warehouse-2025/load_staging.log
python -m load_staging_module.load_staging
echo "$(date) - End load_staging" >> /home/fragile/data-warehouse-2025/load_staging.log

echo "$(date) - Start transform" >> /home/fragile/data-warehouse-2025/transform.log
python -m transform_module.transform
echo "$(date) - End transform" >> /home/fragile/data-warehouse-2025/transform.log

echo "$(date) - Start load_warehouse" >> /home/fragile/data-warehouse-2025/load_warehouse.log
python -m   load_warehouse_module.load_warehouse_module
echo "$(date) - End load_warehouse_module" >> /home/fragile/data-warehouse-2025/load_warehouse_module.log
