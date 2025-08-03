from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from config.settings import load_config

def insert_chunk_to_pg(zhisk_file_id: str, uu_id: str, chunk_content: str, ori_json=None):
    config = load_config()
    db_cfg = config.wmx_database
    now = datetime.now()

    sql = """
        INSERT INTO m_zhisk_results (
            zhisk_file_id, content, create_time, create_by,
            update_time, update_by, wmx_id, vector,
            ori_json, code, category, uu_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                zhisk_file_id, chunk_content, now, "system",
                now, "system", None, None,
                Json(ori_json) if ori_json else None,
                "default", None, uu_id
            ))
            conn.commit()