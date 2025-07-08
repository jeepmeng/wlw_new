import asyncio
import os
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
# from db_service.session import get_async_db
from db_service.pg_pool import pg_conn
from db_service.db_search_service import async_query_similar_sentences, async_hybrid_search
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
    WriteQuesBatch
)
import aiohttp
import tempfile
from task.file_parse_pipeline import parse_file_and_enqueue_chunks
from utils.task_utils import submit_vector_task_with_option
from celery import chain
from db_service.pg_pool import pg_conn

router = APIRouter()
# logger = setup_logger("async_vector_api")
logger = get_logger("router_async_vector_api")
# logger.info("任务开始")


# Redis client
redis_client = Redis.from_url(settings.vector_service.redis_backend)




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




@router.post("/search/gen_vector", response_model=ResponseModel)
def submit_vector_task(item: VectorItem):
    try:
        task = submit_vector_task_with_option(item.text, write_to_redis=True)
        redis_client.set(f"text:{task.id}", item.text, ex=3600)
        return {"msg": "任务创建成功", "data": {"task_id": task.id}}
    except Exception as e:
        logger.exception("提交向量计算任务失败")
        return {"msg": "处理失败", "data": {"error": str(e)}}


# ✅ 轮询获取任务计算结果并执行向量查询
# @router.get("/search/vector_search/{task_id}", response_model=ResponseModel)
# async def get_vector_result(task_id: str, db: AsyncSession = Depends(get_async_db)):
#     try:
#         redis_key = f"vec_result:{task_id}"
#         vec_json = redis_client.get(redis_key)
#         if vec_json:
#             text_vec = json.loads(vec_json)
#             results = await async_query_similar_sentences(text_vec, db)
#             return {"msg": "top-10 vector search", "data": {"results": results}}
#         else:
#             return {"msg": "任务未完成", "data": {"status": "PENDING"}}
#     except Exception as e:
#         logger.exception(f"获取向量结果失败，task_id={task_id}")
#         return {"msg": "处理失败", "data": {"error": str(e)}}

@router.get("/search/vector_search/{task_id}", response_model=ResponseModel)
async def get_vector_result(task_id: str):
    try:
        redis_key = f"vec_result:{task_id}"
        vec_json = redis_client.get(redis_key)
        if vec_json:
            text_vec = json.loads(vec_json)
            async with pg_conn() as conn:
                async with conn.transaction():
                    results = await async_query_similar_sentences(text_vec, conn)
            return {"msg": "top-10 vector search", "data": {"results": results}}
        else:
            return {"msg": "任务未完成", "data": {"status": "PENDING"}}
    except Exception as e:
        logger.exception(f"获取向量结果失败，task_id={task_id}")
        return {"msg": "处理失败", "data": {"error": str(e)}}


# # ✅ 提交混合搜索任务（保存 query_text）
# @router.post("/vector/mix_task", response_model=ResponseModel)
# def submit_mix_task(item: VectorItem):
#     task = encode_text_task.delay(item.text)
#     redis_client.set(f"text:{task.id}", item.text, ex=3600)
#     return {"msg": "任务创建成功", "data": {"task_id": task.id}}


# @router.post("/vector/hybrid_search", response_model=ResponseModel)
# async def submit_mix_task_blocking(
#         item: VectorItem,
#         db: AsyncSession = Depends(get_async_db),
#         top_k: int = Query(15, ge=1),
#         lambda_: float = Query(0.8, ge=0.0, le=1.0),
#         timeout: int = Query(30, ge=1, le=60)
# ):
#     try:
#         task = encode_text_task.delay(item.text)
#         task_id = task.id
#         redis_client.set(f"text:{task_id}", item.text, ex=3600)
#
#         start = time.time()
#         while time.time() - start < timeout:
#             vec_json = redis_client.get(f"vec_result:{task_id}")
#             if vec_json:
#                 vector = json.loads(vec_json)
#                 results = await async_hybrid_search(item.text, vector, db, top_k=top_k, lambda_=lambda_)
#                 return {"msg": "hybrid search result", "data": {"results": results}}
#             await asyncio.sleep(1)
#
#         return {"msg": "等待超时，任务未完成", "data": {"task_id": task_id, "status": "TIMEOUT"}}
#     except Exception as e:
#         logger.exception("阻塞式混合搜索失败")
#         return {"msg": "处理失败", "data": {"error": str(e)}}


@router.post("/vector/hybrid_search", response_model=ResponseModel)
async def submit_mix_task_blocking(item: VectorItem, top_k: int = Query(15, ge=1), lambda_: float = Query(0.8, ge=0.0, le=1.0), timeout: int = Query(30, ge=1, le=60)):
    try:
        task = encode_text_task.delay(item.text)
        task_id = task.id
        redis_client.set(f"text:{task_id}", item.text, ex=3600)

        start = time.time()
        while time.time() - start < timeout:
            vec_json = redis_client.get(f"vec_result:{task_id}")
            if vec_json:
                vector = json.loads(vec_json)
                async with pg_conn() as conn:
                    async with conn.transaction():
                        results = await async_hybrid_search(item.text, vector, conn, top_k=top_k, lambda_=lambda_)
                return {"msg": "hybrid search result", "data": {"results": results}}
            await asyncio.sleep(1)

        return {"msg": "等待超时，任务未完成", "data": {"task_id": task_id, "status": "TIMEOUT"}}
    except Exception as e:
        logger.exception("阻塞式混合搜索失败")
        return {"msg": "处理失败", "data": {"error": str(e)}}



# ✅ 查询混合搜索结果（支持可选参数）
# @router.get("/vector/hybrid_search/{task_id}", response_model=ResponseModel)
# async def get_hybrid_search_result(
#         task_id: str,
#         db: AsyncSession = Depends(get_async_db),
#         top_k: int = Query(15, ge=1),
#         lambda_: float = Query(0.8, ge=0.0, le=1.0)
# ):
#     try:
#         vec_json = redis_client.get(f"vec_result:{task_id}")
#         query_text = redis_client.get(f"text:{task_id}")
#
#         if not vec_json or not query_text:
#             return {"msg": "任务未完成", "data": {"status": "PENDING"}}
#
#         vector = json.loads(vec_json)
#         text = query_text.decode()
#         results = await async_hybrid_search(text, vector, db, top_k=top_k, lambda_=lambda_)
#         return {"msg": "hybrid search result", "data": {"results": results}}
#     except Exception as e:
#         logger.exception(f"获取混合搜索结果失败，task_id={task_id}")
#         return {"msg": "处理失败", "data": {"error": str(e)}}



@router.get("/vector/hybrid_search/{task_id}", response_model=ResponseModel)
async def get_hybrid_search_result(task_id: str, top_k: int = Query(15, ge=1), lambda_: float = Query(0.8, ge=0.0, le=1.0)):
    try:
        vec_json = redis_client.get(f"vec_result:{task_id}")
        query_text = redis_client.get(f"text:{task_id}")

        if not vec_json or not query_text:
            return {"msg": "任务未完成", "data": {"status": "PENDING"}}

        vector = json.loads(vec_json)
        text = query_text.decode()

        async with pg_conn() as conn:
            async with conn.transaction():
                results = await async_hybrid_search(text, vector, conn, top_k=top_k, lambda_=lambda_)
        return {"msg": "hybrid search result", "data": {"results": results}}
    except Exception as e:
        logger.exception(f"获取混合搜索结果失败，task_id={task_id}")
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

            # ✅ 构建任务链（此处 chain 最后执行）
            task_chain = parse_file_and_enqueue_chunks.s(tmp_path, ext, file.file_id)

            result = task_chain.apply_async()
            task_ids.append({
                "file_id": file.file_id,
                "task_id": result.id,
                "filename": file.filename
            })

    return {"msg": "上传任务已提交", "tasks": task_ids}


@router.post("/db/write-ques-batch")
async def write_ques_batch(payload: WriteQuesBatch):
    if len(payload.sentences) != len(payload.vectors):
        return {"error": "数量不一致"}

    data = [
        (payload.uu_id, sent, json.dumps(vec))
        for sent, vec in zip(payload.sentences, payload.vectors)
    ]
    sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
    async with pg_conn() as conn:
        async with conn.transaction():
            await conn.executemany(sql, data)

    return {"status": "ok", "inserted": len(data)}


