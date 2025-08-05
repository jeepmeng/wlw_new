from fastapi import APIRouter, HTTPException
from elasticsearch import AsyncElasticsearch
from utils.logger_manager import get_logger
from redis import Redis
import json
from config.settings import settings
import time
from task.es_fun.es_query import query_es_index,query_results_by_file_id,query_by_uuids
from routers.schema import (
    ResponseModel,
    QueryZhiskFilesRequest,
    QueryByFileIdRequest,
    UUIDQueryRequest
)
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


es_cfg = settings.elasticsearch
FILE_INDEX = es_cfg.indexes.file_index
CHUNK_INDEX = es_cfg.indexes.chunk_index
QUES_INDEX = es_cfg.indexes.ques_index

@router.get("/health")
async def health_check():
    info=await es.info()
    print(info)
    return {"status": "ok", "info": info}


@router.post("/es/query_zhisk_files", response_model=ResponseModel)
async def query_zhisk_files(request: QueryZhiskFilesRequest):
    """
    查询zhisk_file索引（使用封装函数）
    """
    try:
        result = await query_es_index(
            es=es,
            index_name="zhisk_files",
            page=request.page,
            page_size=request.page_size,
            sort_field=request.sort_field,
            sort_order=request.sort_order
        )
        
        return {
            "msg": "查询成功",
            "data": {
                "items": result["items"],
                "pagination": {
                    "total": result["total"],
                    "page": result["page"],
                    "page_size": result["page_size"],
                    "total_pages": result["total_pages"]
                }
            }
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception("查询处理失败")
        return {
            "msg": "处理失败",
            "data": {"error": str(e)}
        }
    

@router.post("/es/query_zhisk_results", response_model=ResponseModel)
async def query_results_api(request: QueryByFileIdRequest):
    """
    根据zhisk_file_id查询文档（简洁版）
    请求示例：
    {
        "zhisk_file_id": 12345
    }
    """
    try: 
        # 调用业务逻辑函数
        response = await query_results_by_file_id(es, request.zhisk_file_id)
        
        return {
            'msg':"查询成功",
            'data':{
                "total": response["total"],
                "items": response["results"]
            }
        }
        
    except Exception as e:
        logger.error(f"查询失败: {str(e)}")
        return {
            'msg':"查询失败",
            'data':{"error": str(e)}
        }   
    
@router.post("/es/query_wmx_ques_by_uuids", response_model=ResponseModel)
async def query_wmx_ques(request: UUIDQueryRequest):
    """
    根据多个UUID批量查询wmx_ques索引
    请求示例：
    {
        "uuids": ["550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"],
    }
    """
    try:
        # 调用业务逻辑
        result = await query_by_uuids(
            es,request.uuids
        )
        
        return {
            "msg":"查询成功",
            "data":{'details': result}
        }
        
    except Exception as e:
        logger.error(f"批量查询失败: {str(e)}")

        return {
            "msg":"查询失败",
            "data":{"error": str(e), "uuids": request.uuids}
        }
