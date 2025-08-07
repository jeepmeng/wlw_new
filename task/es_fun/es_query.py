from fastapi import HTTPException
from elasticsearch import AsyncElasticsearch
from typing import List, Optional 
async def query_es_index(
    es: AsyncElasticsearch,
    index_name: str,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    sort_field: str = "_doc",
    sort_order: str = "asc"
):
    """
    通用Elasticsearch分页查询函数
    :param es: AsyncElasticsearch实例
    :param index_name: 索引名称
    :param page: 页码
    :param page_size: 每页大小
    :param sort_field: 排序字段
    :param sort_order: 排序方向(asc/desc)
    :param query: 自定义查询条件，默认match_all
    :return: 包含分页结果和元数据的字典
    :raises: HTTPException
    """
    try:
        # 构建基础查询体
        query_body = {
            "query": {"match_all": {}},
            "sort": [{sort_field: {"order": sort_order}}]
        }
        
        # 处理分页逻辑
        if page is not None and page_size is not None:
            query_body.update({
                "from": (page - 1) * page_size,
                "size": page_size
            })
        else:
            # 不传分页参数时，设置超大size获取全部数据
            query_body["size"] = 10000  # 可根据实际调整最大限制
        # 执行查询
        response = await es.search(
            index=index_name,
            body=query_body
        )
        
        # 解析结果
        hits = response["hits"]["hits"]
        total = response["hits"]["total"]["value"]
        
        # 格式化文档列表
        results = []
        for hit in hits:
            item = hit["_source"].copy()
            item["_id"] = hit["_id"]  # 注入文档ID
            results.append(item)
        
        return {
            "items": results,
            "total": total,
            "page": page or 1,
            "page_size": page_size or total,
            "total_pages": 1 if page_size is None else (total + page_size - 1) // page_size
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"ES查询失败: {str(e)}"
        )


async def query_results_by_file_id(es: AsyncElasticsearch, file_id: str):
    """
    独立业务逻辑：根据file_id查询ES结果
    :param es: ES客户端实例
    :param file_id: 要查询的文件ID
    :return: 原始ES响应数据
    """
    query_body = {
        "query": {
            "term": {
                "zhisk_file_id": file_id  # 精确匹配
            }
        },
        "size": 1000,  # 默认最大获取1000条
        "sort": [
            {"_doc": {"order": "asc"}}  # 默认按存储顺序
        ]
    }
    
    response = await es.search(
        index="zhisk_results",
        body=query_body
    )
    return {
        "total": response["hits"]["total"]["value"],
        "results": [
            {"_id": hit["_id"], "_source": hit["_source"]} 
            for hit in response["hits"]["hits"]
        ]
    }


async def query_by_uuids(es: AsyncElasticsearch, uuids: List[str]):
    """
    根据多个UUID查询ES (核心业务逻辑)
    :param es: ES客户端实例
    :param uuids: UUID列表
    :return: 格式化后的结果 {uuid1: [doc1, doc2], uuid2: [...]}
    """
    results = []
    
    for uuid in uuids:
        query = {
            "query": {
                "term": {
                    "ori_sent_id": uuid  # 精确匹配UUID字段
                }
            },
            "sort": [{"_doc": {"order": "asc"}}]
        }
        
        response = await es.search(index="wmx_ques", body=query)
        hits = response["hits"]["hits"]

        results.append({
            "uuid":uuid,
            "ques": [hit["_source"] for hit in hits]
        })
    
    return results