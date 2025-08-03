import psycopg2
from psycopg2.extras import execute_batch
from task.celery_app import celery_app
from utils.logger_manager import get_logger
from config.settings import load_config
from utils.vector_utils import vector_to_pgstring

logger = get_logger("insert_ques_batch")

@celery_app.task(name="insert.ques.batch", bind=True, autoretry_for=(Exception,), max_retries=3)
def insert_ques_batch_task(self, vectors, *, questions, uu_id):
    try:
        if not isinstance(vectors[0], (list, tuple)):
            vectors = [vectors]

        config = load_config()
        db_cfg = config.wmx_database

        sql = """
            INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector)
            VALUES (%s, %s, %s)
        """
        data = [
            (uu_id, q, vector_to_pgstring(v))
            for q, v in zip(questions, vectors)
        ]

        with psycopg2.connect(
            dbname=db_cfg.DB_NAME,
            user=db_cfg.DB_USER,
            password=db_cfg.DB_PASSWORD,
            host=db_cfg.DB_HOST,
            port=db_cfg.DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, sql, data)
                conn.commit()

        logger.info(f"[{uu_id}] ✅ 成功写入 {len(data)} 条问题向量")

    except Exception as e:
        logger.exception(f"[{uu_id}] ❌ 向量入库失败: {e}")
        raise self.retry(exc=e)