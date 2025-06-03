# ✅ async_vector_api.py
# FastAPI 路由接口：提交向量任务 + 查询向量结果 + 检索数据库内容
import asyncio
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from db_service.session import get_async_db
from db_service.vector_service import async_query_similar_sentences, async_hybrid_search
from utils.logger import setup_logger
from redis import Redis
import json
from vector_service.vector_tasks import encode_text_task
from config.settings import settings

router = APIRouter()
logger = setup_logger("async_vector_api")

# Redis client
redis_client = Redis.from_url(settings.vector_service.redis_backend)

# ✅ 请求数据模型
class VectorItem(BaseModel):
    id: Optional[str] = None
    text: str

class ResponseModel(BaseModel):
    msg: str
    data: Optional[dict] = None

# ✅ 提交异步向量计算任务
@router.post("/vector/task", response_model=ResponseModel)
def submit_vector_task(item: VectorItem):
    task = encode_text_task.delay(item.text)
    return {"msg": "任务创建成功", "data": {"task_id": task.id}}

# ✅ 轮询获取任务计算结果并执行向量查询
@router.get("/vector/task_result/{task_id}", response_model=ResponseModel)
async def get_vector_result(task_id: str, db: AsyncSession = Depends(get_async_db)):
    redis_key = f"vec_result:{task_id}"
    vec_json = redis_client.get(redis_key)
    if vec_json:
        text_vec = json.loads(vec_json)
        results = await async_query_similar_sentences(text_vec, db)
        return {"msg": "top-10 vector search", "data": {"results": results}}
    else:
        return {"msg": "任务未完成", "data": {"status": "PENDING"}}


# # ✅ 提交混合搜索任务（保存 query_text）
# @router.post("/vector/mix_task", response_model=ResponseModel)
# def submit_mix_task(item: VectorItem):
#     task = encode_text_task.delay(item.text)
#     redis_client.set(f"text:{task.id}", item.text, ex=3600)
#     return {"msg": "任务创建成功", "data": {"task_id": task.id}}


@router.post("/vector/mix_task", response_model=ResponseModel)
async def submit_mix_task_blocking(
    item: VectorItem,
    db: AsyncSession = Depends(get_async_db),
    top_k: int = Query(15, ge=1),
    lambda_: float = Query(0.8, ge=0.0, le=1.0),
    timeout: int = Query(30, ge=1, le=60)
):
    task = encode_text_task.delay(item.text)
    task_id = task.id

    # 存储原始文本供后续使用
    redis_client.set(f"text:{task_id}", item.text, ex=3600)

    import time
    start = time.time()

    while time.time() - start < timeout:
        vec_json = redis_client.get(f"vec_result:{task_id}")
        if vec_json:
            vector = json.loads(vec_json)
            results = await async_hybrid_search(item.text, vector, db, top_k=top_k, lambda_=lambda_)
            return {"msg": "hybrid search result", "data": {"results": results}}
        await asyncio.sleep(1)

    return {"msg": "等待超时，任务未完成", "data": {"task_id": task_id, "status": "TIMEOUT"}}

# ✅ 查询混合搜索结果（支持可选参数）
@router.get("/vector/mix_result/{task_id}", response_model=ResponseModel)
async def get_hybrid_search_result(
    task_id: str,
    db: AsyncSession = Depends(get_async_db),
    top_k: int = Query(15, ge=1),
    lambda_: float = Query(0.8, ge=0.0, le=1.0)
):
    vec_json = redis_client.get(f"vec_result:{task_id}")
    query_text = redis_client.get(f"text:{task_id}")

    if not vec_json or not query_text:
        return {"msg": "任务未完成", "data": {"status": "PENDING"}}

    vector = json.loads(vec_json)
    text = query_text.decode()
    results = await async_hybrid_search(text, vector, db, top_k=top_k, lambda_=lambda_)

    return {"msg": "hybrid search result", "data": {"results": results}}