#!/bin/bash

# 定义根路径
BIRD_DATASET_ROOT="/home/jamtc/paper-code/lifh/Text-to-SQL/datasets/BIRD"
BIRD_MODE="train" 

# --- BIRD 数据集处理 (dev 模式) ---
echo "--- Running BIRD dev dataset processing ---"
BIRD_DB_DIR_SUFFIX="${BIRD_MODE}/${BIRD_MODE}_databases"
BIRD_DB_SCHEMAS_OUTPUT_DIR="./preprocess_data/bird/${BIRD_MODE}/db_schemas"
BIRD_PROCESSED_DATA_OUTPUT_DIR="./preprocess_data/bird/${BIRD_MODE}"
BIRD_DB_CONTENT_INDEX_DIR="./preprocess_data/bird/${BIRD_MODE}/db_content_index"
JSON_DATASET_PATH="${BIRD_DATASET_ROOT}/${BIRD_MODE}/${BIRD_MODE}.json"
DB_PATH="${BIRD_DATASET_ROOT}/${BIRD_MODE}/${BIRD_MODE}_databases"

# 确保输出目录存在
mkdir -p "${BIRD_DB_SCHEMAS_OUTPUT_DIR}"
mkdir -p "${BIRD_PROCESSED_DATA_OUTPUT_DIR}"
mkdir -p "${BIRD_DB_CONTENT_INDEX_DIR}"

echo "Running ./data_procession/generate_bird_db_desc.py"
python ./data_procession/generate_bird_db_desc.py \
    --bird_dir "${BIRD_DATASET_ROOT}" \
    --db_dir_suffix "${BIRD_DB_DIR_SUFFIX}" \
    --output_dir "${BIRD_DB_SCHEMAS_OUTPUT_DIR}"

echo "Running process_dataset.py"
python ./data_procession/process_dataset.py \
    --json_dataset "${JSON_DATASET_PATH}" \
    --db_dir "${DB_PATH}" \
    --db_schema_dir "${BIRD_DB_SCHEMAS_OUTPUT_DIR}" \
    --output_dir "${BIRD_PROCESSED_DATA_OUTPUT_DIR}" \
    --db_content_index_path "${BIRD_DB_CONTENT_INDEX_DIR}" # 新增：传递BM25索引路径

echo "BIRD dev data processing complete."
