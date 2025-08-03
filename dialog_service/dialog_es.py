from elasticsearch import AsyncElasticsearch
from datetime import datetime
from typing import Optional
import time
from config.settings import settings
# es = AsyncElasticsearch("http://localhost:9200")

es = AsyncElasticsearch(
    hosts=[settings.elasticsearch.host],
    http_auth=(settings.elasticsearch.username, settings.elasticsearch.password)
)

INDEX_TALK = "m_talk"
INDEX_RECORD = "m_talk_record"

# ✅ 使用时间戳生成 long 型 ID（或你可传入 int）
def gen_long_id():
    return int(time.time() * 1000)

async def create_session_es(user_id: int, title: Optional[str] = None) -> int:
    talk_id = gen_long_id()
    doc = {
        "id": talk_id,
        "user_id": user_id,
        "title": title,
        "create_time": datetime.utcnow().isoformat(),
        "create_by": user_id
    }
    await es.index(index=INDEX_TALK, id=talk_id, document=doc)
    return talk_id



async def insert_message_es(talk_id: int, user_id: int, input_content: str, output_content: str):
    record_id = gen_long_id()
    doc = {
        "id": record_id,
        "talk_id": talk_id,
        "create_by": user_id,
        "input_content": input_content,
        "output_content": output_content,
        "create_time": datetime.utcnow().isoformat()
    }
    await es.index(index=INDEX_RECORD, id=record_id, document=doc)



from typing import List, Dict

async def get_history_by_session_es(talk_id: int) -> List[Dict]:
    resp = await es.search(
        index=INDEX_RECORD,
        query={"term": {"talk_id": talk_id}},
        sort=[{"create_time": {"order": "desc"}}],
        size=3
    )
    history = []
    for hit in reversed(resp["hits"]["hits"]):
        source = hit["_source"]
        history.append({"role": "user", "content": source["input_content"]})
        history.append({"role": "assistant", "content": source["output_content"]})
    return history



# async def get_session_user_es(talk_id: int) -> Optional[int]:
#     resp = await es.search(
#         index=INDEX_RECORD,
#         query={"term": {"talk_id": talk_id}},
#         size=1
#     )
#     hits = resp["hits"]["hits"]
#     if hits:
#         return hits[0]["_source"]["create_by"]
#     return None


async def get_session_user_es(talk_id: int) -> Optional[int]:
    try:
        doc = await es.get(index="m_talk", id=talk_id)
        return doc["_source"]["user_id"]
    except:
        return None