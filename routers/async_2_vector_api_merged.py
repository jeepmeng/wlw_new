import asyncio
import os
from typing import List
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
# from db_service.session import get_async_db
# from db_service.db_search_service import async_query_similar_sentences, async_hybrid_search
from elasticsearch import AsyncElasticsearch
from task.es_fun.search_engine import search_bm25, search_vector, merge_results
from utils.logger_manager import get_logger
from redis import Redis
import json
# from task.vector_tasks import encode_text_task
from task.gen_vector_chain import encode_text_task
from config.settings import settings
import time
from db_service.db_interact_service import (
    insert_vectors_to_db,
    insert_ques_batch,
    update_by_id,
    update_field_by_id
)
from routers.schema import (
    VectorItem,
    ResponseModel,
    InsertVectorItem,
    InsertQuesBatchItem,
    UpdateByIdItem,
    FileBatchRequest,
    WriteQuesBatch,
    SearchRequest,
    SearchResult
)
import aiohttp
import tempfile
from task.file_parse_pipeline_new import parse_file_and_enqueue_chunks
from utils.task_utils import submit_vector_task_with_option
from task.es_fun.es_delete import delete_doc, delete_by_term, delete_by_terms
from celery import chain
from db_service.pg_pool import pg_conn
from celery.result import AsyncResult
from task.celery_app import celery_app
router = APIRouter()
# logger = setup_logger("async_vector_api")
logger = get_logger("router_async_vector_api")
# logger.info("任务开始")


# Redis client
redis_client = Redis.from_url(settings.vector_service.redis_backend)


es = AsyncElasticsearch(
    hosts=[settings.elasticsearch.host],
    http_auth=(settings.elasticsearch.username, settings.elasticsearch.password)
)

# # ✅ 提交异步向量计算任务
# @router.post("/search/gen_vector", response_model=ResponseModel)
# def submit_vector_task(item: VectorItem):
#     try:
#         task = encode_text_task.delay(item.text)
#         task_id = task.id
#         redis_client.set(f"text:{task_id}", item.text, ex=3600)
#         return {"msg": "任务创建成功", "data": {"task_id": task_id}}
#     except Exception as e:
#         logger.exception("提交向量计算任务失败")
#         return {"msg": "处理失败", "data": {"error": str(e)}}





es_cfg = settings.elasticsearch
FILE_INDEX = es_cfg.indexes.file_index
CHUNK_INDEX = es_cfg.indexes.chunk_index
QUES_INDEX = es_cfg.indexes.ques_index

@router.post("/search/gen_vector", response_model=ResponseModel)
def submit_vector_task(item: VectorItem):
    try:
        task = submit_vector_task_with_option(item.text, write_to_redis=True)
        redis_client.set(f"text:{task.id}", item.text, ex=3600)
        return {"msg": "任务创建成功", "data": {"task_id": task.id}}
    except Exception as e:
        logger.exception("提交向量计算任务失败")
        return {"msg": "处理失败", "data": {"error": str(e)}}






@router.post("/db/insert_vector", response_model=ResponseModel)
async def insert_vector(item: InsertVectorItem):
    try:
        await insert_vectors_to_db(
            zhisk_file_id=item.zhisk_file_id,
            content=item.content,
            vector=item.vector,
            jsons=item.jsons,
            code=item.code,
            category=item.category,
            uu_id=item.uu_id
        )
        return {"msg": "插入成功", "data": None}
    except Exception as e:
        logger.exception("插入向量记录失败")
        return {"msg": "失败", "data": {"error": str(e)}}


@router.post("/db/insert_ques_batch", response_model=ResponseModel)
async def insert_batch(item: InsertQuesBatchItem):
    try:
        await insert_ques_batch(item.uu_id, item.sentences, item.vectors)
        return {"msg": "批量插入成功", "data": None}
    except Exception as e:
        logger.exception("批量插入问题失败")
        return {"msg": "失败", "data": {"error": str(e)}}


@router.post("/db/update_by_id", response_model=ResponseModel)
async def update_by_id_route(item: UpdateByIdItem):
    try:
        await update_by_id(item.update_data, item.record_id, item.updata_ques, item.up_ques_vect)
        return {"msg": "更新成功", "data": None}
    except Exception as e:
        logger.exception("更新记录失败")
        return {"msg": "失败", "data": {"error": str(e)}}


@router.post("/db/update_field", response_model=ResponseModel)
async def update_field_route(
    table_name: str = Query(...),
    field_name: str = Query(...),
    new_value: str = Query(...),
    record_id: int = Query(...)
):
    try:
        await update_field_by_id(table_name, field_name, new_value, record_id)
        return {"msg": "字段更新成功", "data": None}
    except Exception as e:
        logger.exception("字段更新失败")
        return {"msg": "失败", "data": {"error": str(e)}}




# @router.post("/upload")
# async def upload_and_dispatch_files(data: FileBatchRequest):
#     task_ids = []
#
#     for file in data.files:
#         ext = file.filename.split(".")[-1].lower()
#
#         async with aiohttp.ClientSession() as session:
#             async with session.get(file.url) as resp:
#                 if resp.status != 200:
#                     raise HTTPException(status_code=400, detail=f"下载失败: {file.filename}")
#                 with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
#                     tmp.write(await resp.read())
#                     tmp.flush()
#                     os.fsync(tmp.fileno())
#                     tmp_path = tmp.name
#
#         # 构建任务链（这里只使用了一个任务,可继续添加）
#         task_chain = chain(
#             parse_file_and_enqueue_chunks.s(tmp_path, ext, file.file_id)
#             # 如果你有其它收尾任务，比如状态更新、入库记录等，可以继续加到这里
#         )
#
#         result = task_chain.apply_async()
#         task_ids.append({
#             "file_id": file.file_id,
#             "task_id": result.id,   # chain 的 root id
#             "filename": file.filename
#         })
#
#     return {"msg": "上传任务已提交", "tasks": task_ids}


@router.post("/upload")
async def upload_and_dispatch_files(data: FileBatchRequest):
    task_ids = []

    async with aiohttp.ClientSession() as session:
        for file in data.files:
            ext = file.filename.split(".")[-1].lower()

            async with session.get(file.url) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=400, detail=f"下载失败: {file.filename}")

                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
                    tmp.write(await resp.read())
                    tmp.flush()
                    os.fsync(tmp.fileno())
                    tmp_path = tmp.name

            # ✅ 不再构造 store_target，任务内部使用 settings.task_defaults 控制
            task_chain = parse_file_and_enqueue_chunks.s(
                tmp_path,
                ext,
                create_by=file.user_id  # ✅ 保留 user_id 字段用于审计
            )

            result = task_chain.apply_async()
            task_ids.append({
                "file_id": file.file_id,
                "task_id": result.id,
                "filename": file.filename
            })

    return {"msg": "上传任务已提交", "tasks": task_ids}

# @router.get("/task_status/{task_id}")
# def get_task_status(task_id: str):
#     result = AsyncResult(task_id, app=celery_app)
#     return {
#         "task_id": task_id,
#         "status": result.status,  # PENDING, STARTED, SUCCESS, FAILURE, RETRY
#         "result": result.result if result.successful() else None,
#         "error": str(result.result) if result.failed() else None
#     }
# @router.post("/db/write-ques-batch")
# async def write_ques_batch(payload: WriteQuesBatch):
#     if len(payload.sentences) != len(payload.vectors):
#         return {"error": "数量不一致"}
#
#     data = [
#         (payload.uu_id, sent, json.dumps(vec))
#         for sent, vec in zip(payload.sentences, payload.vectors)
#     ]
#     sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
#     async with pg_conn() as conn:
#         async with conn.transaction():
#             await conn.executemany(sql, data)
#
#     return {"status": "ok", "inserted": len(data)}


@router.post("/es_hybrid_search", response_model=List[SearchResult])
async def hybrid_search_api(request: SearchRequest):
    # ✅ 校验：至少要启用一种检索方式
    if not (request.use_bm25 or request.use_vector):
        raise HTTPException(status_code=400, detail="必须启用至少一种检索方式（use_bm25 或 use_vector）")

    tasks = []
    if request.use_bm25:
        tasks.append(search_bm25(request.query))
    else:
        tasks.append(asyncio.sleep(0, result=[]))

    if request.use_vector:
        tasks.append(search_vector(request.query))
    else:
        tasks.append(asyncio.sleep(0, result=[]))

    bm25_results, vector_results = await asyncio.gather(*tasks)
    print("🎯 vector 原始得分:")
    for r in vector_results:
        print(f"{r['id']} -> {r['score']}")
    merged = merge_results(bm25_results, vector_results, alpha=request.alpha)
    # return merged
    return [SearchResult(**item) for item in merged]  # ✅ 保证结构



# ========== 删除问题 ==========
@router.delete("/delete/question")
async def delete_questions(question_ids: List[str]):
    for qid in question_ids:
        await delete_doc(QUES_INDEX, qid)
    return {"deleted_questions": question_ids}


# ========== 删除 chunk（及其问题） ==========
@router.delete("/delete/chunk")
async def delete_chunks(chunk_uuids: List[str]):
    for uuid in chunk_uuids:
        # 删除下属问题
        await delete_by_term(QUES_INDEX, "ori_sent_id", uuid)
    await delete_by_terms(CHUNK_INDEX, "uu_id", chunk_uuids)
    return {"deleted_chunks": chunk_uuids}


# ========== 删除文件（及其 chunk 和问题） ==========
@router.delete("/delete/file")
async def delete_files(zhisk_file_ids: List[str]):
    for file_id in zhisk_file_ids:
        # 查出该文件所有 chunk 的 uuid（用于删除对应问题）
        resp = await es.search(index=CHUNK_INDEX, query={
            "term": {"zhisk_file_id": file_id}
        }, size=1000, _source=["uu_id"])
        chunk_uuids = [hit["_source"]["uu_id"] for hit in resp["hits"]["hits"]]

        # 删除问题
        if chunk_uuids:
            await delete_by_terms(QUES_INDEX, "ori_sent_id", chunk_uuids)

        # 删除 chunk
        await delete_by_term(CHUNK_INDEX, "zhisk_file_id", file_id)

        # 删除文件元信息
        await delete_doc(FILE_INDEX, file_id)

    return {"deleted_files": zhisk_file_ids}


