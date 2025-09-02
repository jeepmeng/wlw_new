import os
import uuid
import json
from celery import chain
from config.settings import settings
from task.celery_app import celery_app
from task.splitter_loader import LOADER_MAP, SPLITTER_MAP
from task.gen_ques import generate_questions_task
from task.gen_vector_chain import encode_text_task
from utils.logger_manager import get_logger
from task.pg_fun.file_writer import insert_file_info, update_zhisk_rows
from task.pg_fun.chunk_writer import insert_chunk_to_pg
from task.pg_fun.vector_writer import insert_ques_batch_task
from task.common.wrap_utils import wrap_vector_as_list
# from task.milvus_fun.writer import insert_to_milvus  # 暂不使用

from task.es_fun.writer import (
    insert_file_meta_to_es,
    insert_chunk_to_es,
    insert_question_vector_to_es
)
from utils.es_meta_build import (
    build_file_meta,
    build_chunk_doc,
    build_question_vector_doc
)

logger = get_logger("parse_file_and_enqueue_chunks")
es_cfg = settings.elasticsearch
index_file = es_cfg.indexes.file_index
index_chunk = es_cfg.indexes.chunk_index
index_ques = es_cfg.indexes.ques_index


@celery_app.task(
    name="parse.file",
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,)
)
def parse_file_and_enqueue_chunks(self, file_path: str, ext: str, create_by: str, original_name: str):
    logger = get_logger("parse_file_and_enqueue_chunks")
    try:
        if ext not in LOADER_MAP or ext not in SPLITTER_MAP:
            msg = f"暂不支持的文件类型: .{ext}"
            logger.warning(msg)
            return {"status": "skipped", "reason": msg}

        # ✅ 从配置中读取控制参数
        store_flags = settings.task_defaults.store_flags
        use_pg = "pg" in store_flags and settings.task_defaults.pg_enable
        use_es = "es" in store_flags and settings.task_defaults.es_enable

        # ✅ 索引名从配置中读取
        es_cfg = settings.elasticsearch
        index_file = es_cfg.indexes.file_index
        index_chunk = es_cfg.indexes.chunk_index
        logger.info(f"[DEBUG] 当前 ES 写入配置：use_es={use_es}, file_index={index_file}, chunk_index={index_chunk}")
        # ✅ PG 写入返回 uuid，否则新建 uuid
        zhisk_file_id = str(uuid.uuid4()) if not use_pg else insert_file_info(file_path, ext, index_file)

        loader = LOADER_MAP[ext]
        splitter = SPLITTER_MAP[ext]

        docs = loader(file_path)
        if not docs or all(not d.page_content.strip() for d in docs):
            msg = "文档内容为空，无法处理"
            logger.warning(msg)
            return {"status": "skipped", "reason": msg}

        chunks = splitter(docs)
        if not chunks:
            msg = "文档分段为空，跳过处理"
            logger.warning(msg)
            return {"status": "skipped", "reason": msg}

        if use_es:
            logger.info(f"[ES] 正在写入文件元信息到索引: {index_file}")
            file_meta = build_file_meta(zhisk_file_id, file_path, ext, len(chunks), create_by, original_name)
            insert_file_meta_to_es(index_file, file_meta)

        for chunk in chunks:
            uu_id = str(uuid.uuid4())

            if use_pg:
                insert_chunk_to_pg(
                    zhisk_file_id,
                    uu_id,
                    chunk.page_content,
                    ori_json=getattr(chunk, "metadata", {}) or {}
                )

            if use_es:
                logger.info(f"[ES] 正在写入段落 chunk 到索引: {index_chunk}")
                chunk_doc = build_chunk_doc(zhisk_file_id, chunk.page_content, uu_id=uu_id, create_by=create_by)
                insert_chunk_to_es(index_chunk, chunk_doc)

            build_chunk_chain(
                chunk.page_content,
                uu_id,
                zhisk_file_id,
                use_pg=use_pg,
                use_es=use_es
            ).apply_async()

        if use_pg:
            update_zhisk_rows(zhisk_file_id, len(chunks))

        return {"status": "dispatched", "chunks": len(chunks)}

    except Exception as e:
        logger.exception(f"文档处理异常: {e}")
        raise self.retry(exc=e)

    finally:
        try:
            os.remove(file_path)
            logger.info(f"已删除临时文件: {file_path}")
        except Exception as e:
            logger.warning(f"临时文件删除失败: {e}")


def build_chunk_chain(text: str, uu_id: str, file_id: str, use_pg=False, use_es=True):
    return chain(
        generate_questions_task.s(text),
        encode_questions_and_store.s(file_id=file_id, chunk_id=uu_id, use_pg=use_pg, use_es=use_es)
    )


@celery_app.task(name="encode_and_store", bind=True, autoretry_for=(Exception,), max_retries=3)
def encode_questions_and_store(self, questions: list, file_id: str, chunk_id: str, use_pg=True, use_es=True):
    logger = get_logger("encode_and_store")
    try:
        if not questions:
            logger.warning(f"[{file_id}] 无问题生成，跳过编码与入库")
            return "skipped"

        for question in questions:
            q_id = str(uuid.uuid4())

            task_chain = chain(
                encode_text_task.s(question),
                wrap_vector_as_list.s()
            )

            if use_pg:
                task_chain |= insert_ques_batch_task.s(questions=[question], uu_id=file_id)

            if use_es:
                task_chain |= insert_question_vector_to_es_task.s(
                    chunk_id=chunk_id,
                    question=question,
                    q_id=q_id
                )

            task_chain.apply_async()

        logger.info(f"[{file_id}] ✅ 共调度 {len(questions)} 条 encode→insert 子任务链")
        return f"dispatched {len(questions)} tasks"

    except Exception as e:
        logger.exception(f"[{file_id}] encode+insert 任务链调度失败: {e}")
        raise self.retry(exc=e)


@celery_app.task(name="insert.qvec.to.es")
def insert_question_vector_to_es_task(vector: list, chunk_id: str, question: str, q_id: str = None):
    logger = get_logger("insert_question_vector_to_es_task")
    try:
        # ✅ 修复嵌套结构
        if isinstance(vector, list) and isinstance(vector[0], list):
            vector = vector[0]

        doc = build_question_vector_doc(
            ori_sent_id=chunk_id,
            ori_ques_sent=question,
            vector=vector
        )
        if q_id:
            doc["id"] = q_id

        logger.warning(f"[DEBUG] doc.keys(): {list(doc.keys())}")
        logger.warning(f"[DEBUG] vector preview: {vector[:5]}")

        insert_question_vector_to_es(index_ques, doc)
    except Exception as e:
        logger.exception(f"向量写入失败: {e}")
        raise