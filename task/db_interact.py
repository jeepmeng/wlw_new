import asyncio
import traceback
from .celery_app import celery_app
from db_service.db_interact_service import insert_ques_batch, insert_vectors_to_db
from utils.logger_manager import get_logger  # ✅ 假设你使用统一日志
import requests
from db_service.pg_pool import pg_conn
import json
from utils.vector_utils import vector_to_pgstring
import asyncio
from asgiref.sync import async_to_sync


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


# @celery_app.task(
#     name="insert.ques.batch",
#     bind=True,
#     max_retries=3,
#     default_retry_delay=5,
#     autoretry_for=(Exception,)
# )
# def insert_ques_batch_task(self, vectors: list, sentences: list, uu_id: str):
#     # try:
#     #     # asyncio.run(insert_ques_batch(uu_id, sentences, vectors))
#     #     # logger.info(f"[ques.batch] ✅ uu_id: {uu_id} 共插入 {len(sentences)} 条问题")
#     #
#     #
#     #     loop = asyncio.new_event_loop()
#     #     asyncio.set_event_loop(loop)
#     #     loop.run_until_complete(insert_ques_batch(uu_id, sentences, vectors))
#     #     loop.close()
#     #
#     #     logger.info(f"[ques.batch] ✅ uu_id: {uu_id} 共插入 {len(sentences)} 条问题")
#     # except Exception as e:
#     #     err_trace = traceback.format_exc()
#     #     logger.error(f"[ques.batch] ❌ uu_id: {uu_id} 问题批量写入失败: {e}\n{err_trace}")
#     #     raise self.retry(exc=e)
#
#
# # def insert_ques_batch_task(vectors: list, questions: list, uu_id: str):
# #     try:
# #         if len(vectors) != len(questions):
# #             raise ValueError("向量和问题数量不一致")
# #
# #         insert_ques_batch.delay(
# #             uu_id=uu_id,
# #             sentences=questions,
# #             vectors=vectors
# #         )
# #     except Exception as e:
# #         return {"error": str(e)}
#
#     try:
#         url = "http://127.0.0.1:8000/db/write-ques-batch"  # 视部署情况修改
#         payload = {
#             "uu_id": uu_id,
#             "sentences": sentences,
#             "vectors": vectors
#         }
#         resp = requests.post(url, json=payload, timeout=10)
#         resp.raise_for_status()
#         return resp.json()
#
#     except Exception as e:
#         logger.exception(f"[ques.batch] ❌ uu_id: {uu_id} 写入接口失败: {e}")
#         raise








# @celery_app.task(
#     name="insert.ques.batch",
#     bind=True,
#     autoretry_for=(Exception,),
#     retry_backoff=True,
#     retry_kwargs={"max_retries": 3},
# )
# def insert_ques_batch_task(self, vectors: list, sentences: list, uu_id: str):
#     """
#     在 Celery worker 内直接用 asyncpg 写库
#     """
#     async def _run():
#         if len(vectors) != len(sentences):
#             raise ValueError("向量和句子数量不一致")
#
#         try:
#             sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
#             data = [
#                 (uu_id, sent, json.dumps(vec))
#                 for sent, vec in zip(sentences, vectors)
#             ]
#
#             async with pg_conn() as conn:
#                 await conn.executemany(sql, data)
#
#         except Exception as e:
#             # 抛出异常给外层 retry
#             raise e
#
#     try:
#         asyncio.run(_run())
#         return {"status": "ok", "inserted": len(sentences)}
#     except Exception as e:
#         err_trace = traceback.format_exc()
#         logger.error(f"[ques.batch] ❌ uu_id: {uu_id} 批量写入失败: {e}\n{err_trace}")
#         raise self.retry(exc=e)




@celery_app.task(name="insert.ques.batch")
def insert_ques_batch_task(uu_id: str, sentences: list, vectors: list):
    async def _run():
        from db_service.pg_pool import pg_conn
        async with pg_conn() as conn:
            sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
            data = [
                # (uu_id, sent, json.dumps(vec))
                (uu_id, sent, vector_to_pgstring(vec))
                # (uu_id, sent, "[" + ",".join(map(str, vec)) + "]")
                for sent, vec in zip(sentences, vectors)
            ]

            async with pg_conn() as conn:
                async with conn.transaction():
                    await conn.executemany(sql, data)

        async_to_sync(_run)()  # ✅ 安全地在已有同步上下文中调用 async 函数
    #         await conn.executemany(sql, data)
    # import asyncio
    # asyncio.run(_run())
