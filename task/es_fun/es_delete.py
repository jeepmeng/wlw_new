from elasticsearch import AsyncElasticsearch
from config.settings import settings

es = AsyncElasticsearch(
    hosts=[settings.elasticsearch.host],
    http_auth=(settings.elasticsearch.username, settings.elasticsearch.password)
)

# 单条删除
async def delete_doc(index: str, doc_id: str):
    try:
        await es.delete(index=index, id=doc_id, ignore=[404])
    except Exception as e:
        print(f"Delete failed: {e}")

# 条件删除（term 查询）
async def delete_by_term(index: str, field: str, value: str):
    await es.delete_by_query(index=index, body={
        "query": {
            "term": {field: value}
        }
    }, refresh=True)

# 多值批量删除
async def delete_by_terms(index: str, field: str, values: list[str]):
    await es.delete_by_query(index=index, body={
        "query": {
            "terms": {field: values}
        }
    }, refresh=True)