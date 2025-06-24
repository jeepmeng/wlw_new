import asyncio
import traceback
from .celery_app import celery_app
from db_service.db_interact_service import insert_ques_batch, insert_vectors_to_db
from utils.logger_manager import get_logger  # ✅ 假设你使用统一日志


logger = get_logger("task_db_insteract")

@celery_app.task(
    name="insert.vector.2db",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    autoretry_for=(Exception,)
)
def insert_vectors_to_db_task(
    self,
    zhisk_file_id: str,
    content: str,
    vector: list,
    jsons: dict,
    code: str,
    category: str,
    uu_id: str
):
    try:
        asyncio.run(insert_vectors_to_db(
            zhisk_file_id, content, vector, jsons, code, category, uu_id
        ))
        logger.info(f"[vector2db] ✅ uu_id: {uu_id} 写入成功")

    except Exception as e:
        err_trace = traceback.format_exc()
        logger.error(f"[vector2db] ❌ uu_id: {uu_id} 写入失败: {e}\n{err_trace}")
        raise self.retry(exc=e)


@celery_app.task(
    name="insert.ques.batch",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    autoretry_for=(Exception,)
)
def insert_ques_batch_task(self, uu_id: str, sentences: list, vectors: list):
    try:
        asyncio.run(insert_ques_batch(uu_id, sentences, vectors))
        logger.info(f"[ques.batch] ✅ uu_id: {uu_id} 共插入 {len(sentences)} 条问题")

    except Exception as e:
        err_trace = traceback.format_exc()
        logger.error(f"[ques.batch] ❌ uu_id: {uu_id} 问题批量写入失败: {e}\n{err_trace}")
        raise self.retry(exc=e)