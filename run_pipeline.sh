#!/bin/bash

# 进入脚本所在的目录，确保相对路径正确
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR"

OUTPUT_BASE_DIR="outputs"
DATASET_NAME="bird/dev"
DB_SCHEMA_DIR="preprocess_data/bird/dev/db_schemas"
DB_ROOT_DIR="../datasets/BIRD/dev/dev_databases"
CSV_FILE_PATH="preprocess_data/bird/dev/processed_dataset.csv"
PIPELINE_CONFIGS_PATH="config/pipeline_configs.json"
MAX_SCHEMA_TOKEN_LENGTH=8192

# 检查是否存在自定义的PipelineManager配置
if [ ! -f "$PIPELINE_CONFIGS_PATH" ]; then
    echo "警告: 未找到PipelineManager配置文件 $PIPELINE_CONFIGS_PATH，将使用默认配置。"
    PIPELINE_CONFIGS_ARG=""
else
    PIPELINE_CONFIGS_ARG="--pipeline_configs_path $PIPELINE_CONFIGS_PATH"
fi

# 检查是否提供了MAX_SCHEMA_TOKEN_LENGTH参数
if [ -n "$MAX_SCHEMA_TOKEN_LENGTH" ]; then
    MAX_SCHEMA_TOKEN_LENGTH_ARG="--max_schema_token_length $MAX_SCHEMA_TOKEN_LENGTH"
else
    MAX_SCHEMA_TOKEN_LENGTH_ARG=""
fi

# 执行Python脚本
python src/run_pipeline.py \
    --output_base_dir "$OUTPUT_BASE_DIR" \
    --dataset_name "$DATASET_NAME" \
    --db_schema_dir "$DB_SCHEMA_DIR" \
    --db_root_dir "$DB_ROOT_DIR" \
    --csv_file_path "$CSV_FILE_PATH" \
    $PIPELINE_CONFIGS_ARG \
    $MAX_SCHEMA_TOKEN_LENGTH_ARG 
    # --save_additional_data

echo "管道流执行完成。"
