import os
from pathlib import Path
from typing import Any, Dict, List

from ..managers.database_manager import DatabaseManager
from ..utils.schema_utils import quote_field, build_database_schema
from ..utils.prompts import table_extraction_prompt
from ..core.task import Task # 导入 Task 类
from typing import Any, Dict, List
from tqdm import tqdm # 导入 tqdm

def extract_related_table(tasks: List[Task], chat_model: Any) -> List[Dict[str, Any]]:
    """提取相关表格节点"""
    paths = DatabaseManager()
    db_schema_dir = paths.db_schema_dir

    results = []
    for task in tqdm(tasks, desc="提取相关表格"): # 添加进度条
        # 提取相关表
        ans = chat_model.get_ans(
            table_extraction_prompt.format(
                database_schema=task.database_schema, 
                question=task.question
            )
        )
        related_tables = _extract_ans(ans)

        # 读取已有数据
        db_schema_file = Path(db_schema_dir) / f"{task.db_id}_schema.json"
        
        scaled_down_schema = ""
        if os.path.exists(db_schema_file):
            scaled_down_schema = build_database_schema(db_schema_file, related_tables, task.question_id)
        else:
            import logging
            logging.warning(f"数据库schema文件不存在: {db_schema_file}")
        
        response = {
            "question_id": task.question_id, # 确保包含 question_id
            "related_tables": ", ".join(related_tables),
            "scaled_down_db_schema": scaled_down_schema
        }
        results.append(response)
    return results

def _extract_ans(ans):
    ans = ans.split('<answer>\n<table>')[1].split('</table>\n</answer>')[0].strip()
    return [quote_field(t) for t in set(ans.split("</table>\n<table>"))]
