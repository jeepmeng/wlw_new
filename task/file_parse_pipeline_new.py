# task/file_parse_pipeline_new.py
# -*- coding: utf-8 -*-

import os
import uuid
from typing import List
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

# ES 写入函数
from task.es_fun.writer import (
    insert_file_meta_to_es,
    insert_chunk_to_es,
    insert_question_vector_to_es,
)

# ES 文档构建
from utils.es_meta_build import (
    build_file_meta,
    build_chunk_doc,
    build_question_vector_doc,
)

# ===== 新增：OCR 客户端（外部服务） =====
# 默认提供 pdf/image 两种入口；也可引入 call_ocr_by_ext 自动分流
from pdf_ocr.ocr_client import (
    ocr_pdf_bytes,
    ocr_image_bytes,
    OCRClientError,
)

try:
    # langchain-core 的 Document（如果没有，下面兜底一个最简替身）
    from langchain_core.documents import Document
except Exception:
    Document = None  # 兜底

logger = get_logger("parse_file_and_enqueue_chunks")

# ES 索引配置
es_cfg = settings.elasticsearch
index_file = es_cfg.indexes.file_index
index_chunk = es_cfg.indexes.chunk_index
index_ques = es_cfg.indexes.ques_index

_OCR_BASE = settings.ocr_service.base_url
logger.info(f"[CONFIG] OCR base_url = {_OCR_BASE}")

# 可识别为“图片 OCR”的扩展名集合
_IMAGE_EXTS = {"jpg", "jpeg", "png", "bmp", "tif", "tiff"}


def _default_split_text(text: str, chunk_size: int = 800, overlap: int = 120) -> List[str]:
    """当没有可用的 splitter 时的安全兜底（当前未使用，保留以备不时之需）"""
    if not text:
        return []
    res, start = [], 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        res.append(text[start:end])
        if end == n:
            break
        start = max(end - overlap, 0)
    return res


@celery_app.task(
    name="parse.file",
    bind=True,
    max_retries=3,
    autoretry_for=(Exception,),
)
def parse_file_and_enqueue_chunks(
    self,
    file_path: str,
    ext: str,
    create_by: str,
    original_name: str,
    *,
    # ==== 新增：OCR 开关与参数（从上传接口透传） ====
    enable_pdf_ocr: bool = False,
    ocr_lang: str = "ch",
    ocr_dpi: int = 200,  # 仅对 PDF 生效；图片不需要 DPI
):
    """
    主处理任务：
    1) 若 enable_pdf_ocr=True 且扩展名为 pdf 或图片，则优先调用外部 OCR 服务获取全文文本；
       - 成功：将全文文本包成单个 Document 进入后续分段；
       - 失败/为空：回退到原本 LOADER_MAP 流程。
    2) 未启用 OCR 或非支持类型：直接按原流程 loader → splitter。
    3) 下游：写入 ES/PG，派发问题生成与向量写入。
    """
    _log = get_logger("parse_file_and_enqueue_chunks")
    try:
        # ===== 读取运行控制（开关/目标存储） =====
        store_flags = settings.task_defaults.store_flags
        use_pg = "pg" in store_flags and settings.task_defaults.pg_enable
        use_es = "es" in store_flags and settings.task_defaults.es_enable

        es_cfg = settings.elasticsearch
        index_file = es_cfg.indexes.file_index
        index_chunk = es_cfg.indexes.chunk_index
        _log.info(f"[DEBUG] use_es={use_es}, file_index={index_file}, chunk_index={index_chunk}")

        # ===== PG：先拿 zhisk_file_id（你的原逻辑）=====
        zhisk_file_id = str(uuid.uuid4()) if not use_pg else insert_file_info(file_path, ext, index_file)

        # ===== A) 若启用 OCR：优先尝试外部服务 =====
        docs = None
        used_pdf_ocr = False
        ext_l = (ext or "").lower().strip(".")

        if enable_pdf_ocr and ext_l in ({"pdf"} | _IMAGE_EXTS):
            try:
                with open(file_path, "rb") as f:
                    raw = f.read()

                if ext_l == "pdf":
                    # 返回全文本（默认 join_pages=True）
                    full_text: str = ocr_pdf_bytes(
                        _OCR_BASE,
                        raw,
                        lang=ocr_lang,
                        dpi=int(ocr_dpi or 200),
                        timeout=120,
                    )
                else:
                    # 图片 OCR：返回全文本（join_lines=True）
                    full_text: str = ocr_image_bytes(
                        _OCR_BASE,
                        raw,
                        lang=ocr_lang,
                        timeout=120,
                        filename_hint=f"image.{ext_l}",
                        mime_hint="application/octet-stream",  # 无需严格 image/*，服务端会判断
                    )

                if full_text and full_text.strip():
                    # 将全文文本包装为一个 Document，以便走统一的 splitter
                    if Document is None:
                        class _Doc:
                            def __init__(self, page_content: str, metadata=None):
                                self.page_content = page_content
                                self.metadata = metadata or {}
                        docs = [_Doc(full_text, {"source": original_name})]
                    else:
                        docs = [Document(page_content=full_text, metadata={"source": original_name})]

                    used_pdf_ocr = True
                else:
                    _log.warning("[OCR] 结果为空，回退原逻辑")

            except OCRClientError as e:
                _log.error(f"[OCR] 外部服务调用失败：{e}")
                used_pdf_ocr = False
            except Exception as e:
                _log.exception(f"[OCR] 未知异常：{e}")
                used_pdf_ocr = False

        # ===== B) 未启用或 OCR 失败：回到原有 loader/splitter =====
        if docs is None:
            if ext_l not in LOADER_MAP or ext_l not in SPLITTER_MAP:
                msg = f"暂不支持的文件类型: .{ext_l}"
                _log.warning(msg)
                return {"status": "skipped", "reason": msg}

            loader = LOADER_MAP[ext_l]
            docs = loader(file_path)
            if not docs or all(not getattr(d, "page_content", "").strip() for d in docs):
                msg = "文档内容为空，无法处理"
                _log.warning(msg)
                return {"status": "skipped", "reason": msg}

        # ===== C) 分段：对 OCR 文本优先使用 'pdf' 的分段策略（通常更稳）=====
        splitter_fn = SPLITTER_MAP.get("pdf" if used_pdf_ocr else ext_l)
        chunks = splitter_fn(docs) if splitter_fn else []
        if not chunks:
            msg = "文档分段为空，跳过处理"
            _log.warning(msg)
            return {"status": "skipped", "reason": msg}

        # ===== D) 写文件级元信息到 ES（可审计 used_pdf_ocr）=====
        if use_es:
            _log.info(f"[ES] 写入文件元信息: {index_file}")
            file_meta = build_file_meta(
                zhisk_file_id,
                file_path,
                ext_l,
                len(chunks),
                create_by,
                original_name,
            )
            file_meta["used_pdf_ocr"] = used_pdf_ocr  # 标注是否用了 OCR
            insert_file_meta_to_es(index_file, file_meta)

        # ===== E) 逐 chunk 入库 + 下游链路 =====
        for c in chunks:
            uu_id = str(uuid.uuid4())

            if use_pg:
                insert_chunk_to_pg(
                    zhisk_file_id,
                    uu_id,
                    c.page_content,
                    ori_json=getattr(c, "metadata", {}) or {},
                )

            if use_es:
                _log.info(f"[ES] 写入段落: {index_chunk}")
                chunk_doc = build_chunk_doc(
                    zhisk_file_id,
                    c.page_content,
                    uu_id=uu_id,
                    create_by=create_by,
                )
                chunk_doc["used_pdf_ocr"] = used_pdf_ocr
                insert_chunk_to_es(index_chunk, chunk_doc)

            build_chunk_chain(
                c.page_content,
                uu_id,
                zhisk_file_id,
                use_pg=use_pg,
                use_es=use_es,
            ).apply_async()

        if use_pg:
            update_zhisk_rows(zhisk_file_id, len(chunks))

        return {
            "status": "dispatched",
            "chunks": len(chunks),
            "used_pdf_ocr": used_pdf_ocr,
        }

    except Exception as e:
        _log.exception(f"文档处理异常: {e}")
        raise self.retry(exc=e)

    finally:
        # 清理临时文件
        try:
            os.remove(file_path)
            logger.info(f"已删除临时文件: {file_path}")
        except Exception as e:
            logger.warning(f"临时文件删除失败: {e}")


def build_chunk_chain(text: str, uu_id: str, file_id: str, use_pg: bool = False, use_es: bool = True):
    """保持你现有的问题生成 → 向量编码 → 入库链路不变"""
    return chain(
        generate_questions_task.s(text),
        encode_questions_and_store.s(
            file_id=file_id,
            chunk_id=uu_id,
            use_pg=use_pg,
            use_es=use_es,
        ),
    )


@celery_app.task(name="encode_and_store", bind=True, autoretry_for=(Exception,), max_retries=3)
def encode_questions_and_store(self, questions: list, file_id: str, chunk_id: str, use_pg: bool = True, use_es: bool = True):
    _log = get_logger("encode_and_store")
    try:
        if not questions:
            _log.warning(f"[{file_id}] 无问题生成，跳过编码与入库")
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
                    q_id=q_id,
                )

            task_chain.apply_async()

        _log.info(f"[{file_id}] ✅ 共调度 {len(questions)} 条 encode→insert 子任务链")
        return f"dispatched {len(questions)} tasks"

    except Exception as e:
        _log.exception(f"[{file_id}] encode+insert 任务链调度失败: {e}")
        raise self.retry(exc=e)


@celery_app.task(name="insert.qvec.to.es")
def insert_question_vector_to_es_task(vector: list, chunk_id: str, question: str, q_id: str = None):
    _log = get_logger("insert_question_vector_to_es_task")
    try:
        # 修复嵌套结构：[[...]] → [...]
        if isinstance(vector, list) and vector and isinstance(vector[0], list):
            vector = vector[0]

        doc = build_question_vector_doc(
            ori_sent_id=chunk_id,
            ori_ques_sent=question,
            vector=vector,
        )
        if q_id:
            doc["id"] = q_id

        _log.warning(f"[DEBUG] doc.keys(): {list(doc.keys())}")
        _log.warning(f"[DEBUG] vector preview: {vector[:5] if isinstance(vector, list) else 'N/A'}")

        insert_question_vector_to_es(index_ques, doc)

    except Exception as e:
        _log.exception(f"向量写入失败: {e}")
        raise