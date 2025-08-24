# from elasticsearch import AsyncElasticsearch
# from datetime import datetime
# from typing import Optional
# import time
# from config.settings import settings
# # es = AsyncElasticsearch("http://localhost:9200")
#
# es = AsyncElasticsearch(
#     hosts=[settings.elasticsearch.host],
#     http_auth=(settings.elasticsearch.username, settings.elasticsearch.password)
# )
#
# INDEX_TALK = "m_talk"
# INDEX_RECORD = "m_talk_record"
#
# # ✅ 使用时间戳生成 long 型 ID（或你可传入 int）
# def gen_long_id():
#     return int(time.time() * 1000)
#
# async def create_session_es(user_id: int, title: Optional[str] = None, provider: str = "deepseek") -> int:
#     talk_id = gen_long_id()
#     doc = {
#         "id": talk_id,
#         "user_id": user_id,
#         "title": title,
#         "provider": provider,
#         "create_time": datetime.utcnow().isoformat(),
#         "create_by": user_id
#     }
#     await es.index(index=INDEX_TALK, id=talk_id, document=doc)
#     return talk_id
#
#
#
# async def insert_message_es(talk_id: int, user_id: int, input_content: str, output_content: str):
#     record_id = gen_long_id()
#     doc = {
#         "id": record_id,
#         "talk_id": talk_id,
#         "create_by": user_id,
#         "input_content": input_content,
#         "output_content": output_content,
#         "create_time": datetime.utcnow().isoformat()
#     }
#     await es.index(index=INDEX_RECORD, id=record_id, document=doc)
#
#
#
# from typing import List, Dict
#
# async def get_history_by_session_es(talk_id: int) -> List[Dict]:
#     resp = await es.search(
#         index=INDEX_RECORD,
#         query={"term": {"talk_id": talk_id}},
#         sort=[{"create_time": {"order": "desc"}}],
#         size=3
#     )
#     history = []
#     for hit in reversed(resp["hits"]["hits"]):
#         source = hit["_source"]
#         history.append({"role": "user", "content": source["input_content"]})
#         history.append({"role": "assistant", "content": source["output_content"]})
#     return history
#
#
#
# # async def get_session_user_es(talk_id: int) -> Optional[int]:
# #     resp = await es.search(
# #         index=INDEX_RECORD,
# #         query={"term": {"talk_id": talk_id}},
# #         size=1
# #     )
# #     hits = resp["hits"]["hits"]
# #     if hits:
# #         return hits[0]["_source"]["create_by"]
# #     return None
#
#
# async def get_session_user_es(talk_id: int) -> Optional[int]:
#     try:
#         doc = await es.get(index="m_talk", id=talk_id)
#         return doc["_source"]["user_id"]
#     except:
#         return None



# dialog_service/dialog_es.py
from elasticsearch import AsyncElasticsearch
from datetime import datetime
from typing import Optional, List, Dict
import time
from config.settings import settings

# ======================
# ES 客户端
# ======================
es = AsyncElasticsearch(
    hosts=[settings.elasticsearch.host],
    http_auth=(settings.elasticsearch.username, settings.elasticsearch.password),
)

INDEX_TALK = "m_talk"
INDEX_RECORD = "m_talk_record"

def gen_long_id() -> int:
    """使用时间戳生成 long 型 ID"""
    return int(time.time() * 1000)

# ======================
# 会话表
# ======================
async def create_session_es(user_id: int, title: Optional[str] = None, provider: str = "deepseek") -> int:
    talk_id = gen_long_id()
    doc = {
        "id": talk_id,
        "user_id": user_id,
        "title": title,
        "provider": provider,
        "create_time": datetime.utcnow().isoformat(),
        "create_by": user_id,
    }
    await es.index(index=INDEX_TALK, id=talk_id, document=doc)
    return talk_id

async def get_session_user_es(talk_id: int) -> Optional[int]:
    try:
        doc = await es.get(index=INDEX_TALK, id=talk_id)
        return doc["_source"]["user_id"]
    except Exception:
        return None

# ======================
# 记录表（成对写入：单条记录包含问与答）
# ======================
async def insert_message_es(
    talk_id: int,
    user_id: int,
    input_content: str,
    output_content: str,
) -> int:
    """
    成对写入（单条记录）：
    - input_content  : 用户问题
    - output_content : 模型最终回答（可在路由层已拼接参考来源）
    """
    record_id = gen_long_id()
    doc = {
        "id": record_id,
        "talk_id": talk_id,
        "create_by": user_id,
        "input_content": input_content,
        "output_content": output_content,
        "create_time": datetime.utcnow().isoformat(),
    }
    await es.index(index=INDEX_RECORD, id=record_id, document=doc)
    return record_id

async def get_history_by_session_es(talk_id: int, limit: int = 3) -> List[Dict[str, str]]:
    """
    读取最近 N 条“成对记录”，还原为对话历史 messages：
    返回示例：
      [{"role":"user","content":"..."},{"role":"assistant","content":"..."}, ...]
    兼容旧数据（role/content 或单边为空）自动转换。
    """
    resp = await es.search(
        index=INDEX_RECORD,
        query={"term": {"talk_id": talk_id}},
        sort=[{"create_time": {"order": "desc"}}],
        size=limit,
    )

    messages: List[Dict[str, str]] = []

    # ES 返回倒序，这里翻转为时间正序
    for hit in reversed(resp["hits"]["hits"]):
        src = hit["_source"]

        # 新结构（优先）
        ic = (src.get("input_content") or "").strip()
        oc = (src.get("output_content") or "").strip()
        if ic or oc:
            if ic:
                messages.append({"role": "user", "content": ic})
            if oc:
                messages.append({"role": "assistant", "content": oc})
            continue

        # 兼容旧结构（role/content 一条条消息）
        role = (src.get("role") or "").strip()
        content = (src.get("content") or "").strip()
        if role and content:
            # 直接塞进历史
            messages.append({"role": role, "content": content})

        # 兼容更旧的成对字段命名（如果存在）
        old_ic = (src.get("question") or "").strip()
        old_oc = (src.get("answer") or "").strip()
        if old_ic:
            messages.append({"role": "user", "content": old_ic})
        if old_oc:
            messages.append({"role": "assistant", "content": old_oc})

    return messages