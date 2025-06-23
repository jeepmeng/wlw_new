# ✅ async_vector_api.py
# FastAPI 路由接口：提交向量任务 + 查询向量结果 + 检索数据库内容
import asyncio
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from db_service.session import get_async_db
from db_service.db_search_service import async_query_similar_sentences, async_hybrid_search
# from utils.logger import setup_logger
from utils.logger_manager import get_logger
from redis import Redis
import json
# from task.vector_tasks import encode_text_task
from task.gen_vector import encode_text_task
from config.settings import settings
import time
from typing import List, Dict, Any
from db_service.db_interact_service import (
    insert_vectors_to_db,
    insert_ques_batch,
    update_by_id,
    update_field_by_id
)

router = APIRouter()
# logger = setup_logger("async_vector_api")
logger = get_logger("router_async_vector_api")
# logger.info("任务开始")


# Redis client
redis_client = Redis.from_url(settings.vector_service.redis_backend)


# ✅ 请求数据模型
class VectorItem(BaseModel):
    id: Optional[str] = None
    text: str


class ResponseModel(BaseModel):
    msg: str
    data: Optional[dict] = None


# 插入向量记录
class InsertVectorItem(BaseModel):
    zhisk_file_id: int
    content: str
    vector: List[float]
    jsons: Dict[str, Any]
    code: str
    category: str
    uu_id: str


# 批量插入问题
class InsertQuesBatchItem(BaseModel):
    uu_id: str
    sentences: List[str]
    vectors: List[List[float]]


# 更新记录字段
class UpdateByIdItem(BaseModel):
    update_data: Dict[str, Any]
    record_id: int
    updata_ques: List[str]
    up_ques_vect: List[List[float]]


# ✅ 提交异步向量计算任务
@router.post("/search/gen_vector", response_model=ResponseModel)
def submit_vector_task(item: VectorItem):
    try:
        task = encode_text_task.delay(item.text)
        task_id = task.id
        redis_client.set(f"text:{task_id}", item.text, ex=3600)
        return {"msg": "任务创建成功", "data": {"task_id": task_id}}
    except Exception as e:
        logger.exception("提交向量计算任务失败")
        return {"msg": "处理失败", "data": {"error": str(e)}}


# ✅ 轮询获取任务计算结果并执行向量查询
@router.get("/search/vector_search/{task_id}", response_model=ResponseModel)
async def get_vector_result(task_id: str, db: AsyncSession = Depends(get_async_db)):
    try:
        redis_key = f"vec_result:{task_id}"
        vec_json = redis_client.get(redis_key)
        if vec_json:
            text_vec = json.loads(vec_json)
            results = await async_query_similar_sentences(text_vec, db)
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


@router.post("/vector/hybrid_search", response_model=ResponseModel)
async def submit_mix_task_blocking(
        item: VectorItem,
        db: AsyncSession = Depends(get_async_db),
        top_k: int = Query(15, ge=1),
        lambda_: float = Query(0.8, ge=0.0, le=1.0),
        timeout: int = Query(30, ge=1, le=60)
):
    try:
        task = encode_text_task.delay(item.text)
        task_id = task.id
        redis_client.set(f"text:{task_id}", item.text, ex=3600)

        start = time.time()
        while time.time() - start < timeout:
            vec_json = redis_client.get(f"vec_result:{task_id}")
            if vec_json:
                vector = json.loads(vec_json)
                results = await async_hybrid_search(item.text, vector, db, top_k=top_k, lambda_=lambda_)
                return {"msg": "hybrid search result", "data": {"results": results}}
            await asyncio.sleep(1)

        return {"msg": "等待超时，任务未完成", "data": {"task_id": task_id, "status": "TIMEOUT"}}
    except Exception as e:
        logger.exception("阻塞式混合搜索失败")
        return {"msg": "处理失败", "data": {"error": str(e)}}


# ✅ 查询混合搜索结果（支持可选参数）
@router.get("/vector/hybrid_search/{task_id}", response_model=ResponseModel)
async def get_hybrid_search_result(
        task_id: str,
        db: AsyncSession = Depends(get_async_db),
        top_k: int = Query(15, ge=1),
        lambda_: float = Query(0.8, ge=0.0, le=1.0)
):
    try:
        vec_json = redis_client.get(f"vec_result:{task_id}")
        query_text = redis_client.get(f"text:{task_id}")

        if not vec_json or not query_text:
            return {"msg": "任务未完成", "data": {"status": "PENDING"}}

        vector = json.loads(vec_json)
        text = query_text.decode()
        results = await async_hybrid_search(text, vector, db, top_k=top_k, lambda_=lambda_)
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
