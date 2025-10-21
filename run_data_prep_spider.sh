#!/bin/bash

# 定义根路径
SPIDER_DATASET_ROOT="/home/jamtc/paper-code/lifh/Text-to-SQL/datasets/Spider-DK"

# --- SPIDER 数据集处理 (train 模式) ---
echo "--- Running SPIDER train dataset processing ---"
SPIDER_DB_DIR_SUFFIX="database"
SPIDER_PROCESSED_DATA_OUTPUT_DIR="./preprocess_data/spider/dk"
SPIDER_DB_SCHEMAS_OUTPUT_DIR="${SPIDER_PROCESSED_DATA_OUTPUT_DIR}/db_schemas"
DB_CONTENT_INDEX_PATH="${SPIDER_PROCESSED_DATA_OUTPUT_DIR}/db_content_index"
JSON_DATASET_PATH="${SPIDER_DATASET_ROOT}/Spider-DK.json"
DB_PATH="${SPIDER_DATASET_ROOT}/${SPIDER_DB_DIR_SUFFIX}"

# 确保输出目录存在
mkdir -p "${SPIDER_DB_SCHEMAS_OUTPUT_DIR}"
mkdir -p "${SPIDER_PROCESSED_DATA_OUTPUT_DIR}"
mkdir -p "${DB_CONTENT_INDEX_PATH}"

echo "Running ./data_procession/generate_spider_db_desc.py"
python ./data_procession/generate_spider_db_desc.py \
    --spider_dir "${SPIDER_DATASET_ROOT}" \
    --db_dir_suffix "${SPIDER_DB_DIR_SUFFIX}" \
    --output_dir "${SPIDER_DB_SCHEMAS_OUTPUT_DIR}"

echo "Running process_dataset.py"
python ./data_procession/process_dataset.py \
    --json_dataset "${JSON_DATASET_PATH}" \
    --db_dir "${DB_PATH}" \
    --db_schema_dir "${SPIDER_DB_SCHEMAS_OUTPUT_DIR}" \
    --db_content_index_path "${DB_CONTENT_INDEX_PATH}" \
    --output_dir "${SPIDER_PROCESSED_DATA_OUTPUT_DIR}"

echo "SPIDER train data processing complete."
