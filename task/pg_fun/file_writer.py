import os
import uuid
from datetime import datetime
from pathlib import Path
import psycopg2
from config.settings import load_config

def insert_file_info(file_path: str, ext: str, file_es_index: str) -> str:
    config = load_config()
    db_cfg = config.wmx_database

    file = Path(file_path)
    original_name = file.name
    file_suffix = file.suffix.lstrip(".").lower()
    file_type = ext
    file_size_kb = round(os.path.getsize(file_path) / 1024, 2)
    now = datetime.now()
    file_name = f"{now:%Y/%m/%d}/{original_name}"
    zhisk_file_id = str(uuid.uuid4())

    sql = """
        INSERT INTO m_zhisk_files (
            zhisk_file_id, file_name, original_name, file_suffix,
            file_type, file_size, tokens, zhisk_rows, state,
            url, create_time, create_by, update_time, update_by,
            wmx_id, category
        ) VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NULL, %s, %s, %s, %s, %s, NULL, %s)
    """

    with psycopg2.connect(
        dbname=db_cfg.DB_NAME,
        user=db_cfg.DB_USER,
        password=db_cfg.DB_PASSWORD,
        host=db_cfg.DB_HOST,
        port=db_cfg.DB_PORT
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (
                zhisk_file_id, file_name, original_name, file_suffix,
                file_type, file_size_kb, file_name, now,
                "system", now, "system", file_es_index
            ))
            conn.commit()

    return zhisk_file_id


def update_zhisk_rows(zhisk_file_id: str, row_count: int):
    config = load_config()
    db_cfg = config.wmx_database

    sql = "UPDATE m_zhisk_files SET zhisk_rows = %s WHERE zhisk_file_id = %s"

    with psycopg2.connect(
        dbname=db_cfg.DB_NAME,
        user=db_cfg.DB_USER,
        password=db_cfg.DB_PASSWORD,
        host=db_cfg.DB_HOST,
        port=db_cfg.DB_PORT
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (row_count, zhisk_file_id))
            conn.commit()