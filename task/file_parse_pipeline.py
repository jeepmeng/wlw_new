import os
from task.celery_app import celery_app
from task.splitter_loader import LOADER_MAP, SPLITTER_MAP
from task.gen_ques import generate_questions_task
from task.gen_vector_chain import encode_text_task
from celery import chain, group
from utils.logger_manager import get_logger
import asyncio
from db_service.pg_pool import pg_conn
from utils.vector_utils import vector_to_pgstring  # 假设你封装了转换函数
from celery import chord
import psycopg2
from psycopg2.extras import execute_batch
from config.settings import load_config

@celery_app.task(
    name="parse.file",
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,)
)
def parse_file_and_enqueue_chunks(self, file_path: str, ext: str, file_id: str):
    logger = get_logger("parse_file_and_enqueue_chunks")
    try:
        if ext not in LOADER_MAP or ext not in SPLITTER_MAP:
            msg = f"暂不支持的文件类型: .{ext}"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}

        # ✅ 加载器 & 分段器
        loader = LOADER_MAP[ext]
        splitter = SPLITTER_MAP[ext]

        docs = loader(file_path)
        if not docs or all(not d.page_content.strip() for d in docs):
            msg = "文档内容为空，无法处理"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}

        chunks = splitter(docs)
        if not chunks:
            msg = "文档分段为空，跳过处理"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}


        for chunk in chunks:
            # ✅ 每个 chunk 单独调度执行，无嵌套
            build_chunk_chain(chunk.page_content, file_id).apply_async()


        return {"status": "dispatched", "chunks": len(chunks)}


    except Exception as e:
        logger.exception(f"[{file_id}] 文档处理异常: {e}")
        raise self.retry(exc=e)

    finally:
        try:
            os.remove(file_path)
            logger.info(f"[{file_id}] 已删除临时文件: {file_path}")
        except Exception as e:
            logger.warning(f"[{file_id}] 临时文件删除失败: {e}")

def build_chunk_chain(text: str, file_id: str):
    return chain(
        generate_questions_task.s(text),
        encode_questions_and_store.s(file_id=file_id)
    )


# @celery_app.task(name="encode_and_insert", bind=True, autoretry_for=(Exception,), max_retries=3)
# def encode_questions_and_store(self, questions: list, file_id: str):
#     logger = get_logger("encode_and_insert")
#     try:
#         if not questions:
#             logger.warning(f"[{file_id}] 无问题生成，跳过编码与入库")
#             return "skipped"
#
#         # 为每个问题创建 encode 向量化任务
#         encode_jobs = [encode_text_task.s(q) for q in questions]
#
#
#         logger.info(f"[{file_id}] ✉️ 已生成向量任务 chord，问题数: {len(questions)}")
#
#         return chord(encode_jobs)(
#             insert_ques_batch_task.s(questions=questions, uu_id=file_id)
#         )
#     except Exception as e:
#         logger.exception(f"[{file_id}] 问题向量化任务失败: {e}")
#         raise self.retry(exc=e)




@celery_app.task(name="encode_and_insert_each", bind=True, autoretry_for=(Exception,), max_retries=3)
def encode_questions_and_store(self, questions: list, file_id: str):
    logger = get_logger("encode_and_insert_each")
    try:
        if not questions:
            logger.warning(f"[{file_id}] 无问题生成，跳过编码与入库")
            return "skipped"

        for question in questions:
            chain(
                encode_text_task.s(question),
                wrap_vector_as_list.s(),
                insert_ques_batch_task.s(questions=[question], uu_id=file_id)
            ).apply_async()

        logger.info(f"[{file_id}] ✅ 共调度 {len(questions)} 条 encode→insert 子任务链")
        return f"dispatched {len(questions)} tasks"

    except Exception as e:
        logger.exception(f"[{file_id}] encode+insert 任务链调度失败: {e}")
        raise self.retry(exc=e)




@celery_app.task(name="insert.ques.batch", bind=True, autoretry_for=(Exception,), max_retries=3)
def insert_ques_batch_task(self, vectors, *, questions, uu_id):
    # questions = kwargs.get("questions")
    # uu_id = kwargs.get("uu_id")
    logger = get_logger("insert_ques_batch")
    # logger.info(f"[{uu_id}] ✅ insert task 被调用")
    # logger.info(f"[{uu_id}] 👀 vectors: {vectors}")
    # logger.info(f"[{uu_id}] 👀 questions: {questions}")

    try:
        # ✅ 修复核心 bug：包装单个向量为二维 list
        if not isinstance(vectors[0], (list, tuple)):
            vectors = [vectors]

        config = load_config()
        db_cfg = config.wmx_database

        conn = psycopg2.connect(
            dbname=db_cfg.DB_NAME,
            user=db_cfg.DB_USER,
            password=db_cfg.DB_PASSWORD,
            host=db_cfg.DB_HOST,
            port=db_cfg.DB_PORT
        )
        cursor = conn.cursor()

        sql = """
            INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector)
            VALUES (%s, %s, %s)
        """
        data = [
            (uu_id, q, vector_to_pgstring(v))
            for q, v in zip(questions, vectors)
        ]

        execute_batch(cursor, sql, data)
        conn.commit()
        logger.info(f"[{uu_id}] ✅ 成功写入 {len(data)} 条问题向量")

    except Exception as e:
        logger.exception(f"[{uu_id}] ❌ 向量入库失败: {e}")
        raise self.retry(exc=e)

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@celery_app.task(name="wrap.vector")
def wrap_vector_as_list(vec):
    return [vec]

