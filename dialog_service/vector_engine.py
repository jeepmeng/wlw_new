# vector_engine.py
from redis import Redis
from config.settings import settings
# from task.vector_tasks import encode_text_task
from task.gen_vector_chain import encode_text_task
# from task.vector_tasks import redis_client
from db_service.db_search_service import async_query_similar_sentences
import json


# ✅ 初始化 Redis client
redis_client = Redis.from_url(settings.vector_service.redis_backend)


def submit_vector_task_sync(text: str) -> str:
    task = encode_text_task.delay(text)
    return task.id

async def get_vector_result_by_task(task_id: str, db):
    redis_key = f"vec_result:{task_id}"
    vec_json = redis_client.get(redis_key)
    if vec_json:
        vector = json.loads(vec_json)
        return await async_query_similar_sentences(vector, db)
    return None

def format_results_into_prompt(results: list) -> str:
    if not results:
        return ""
    prompt = "以下是与你提问相关的参考资料：\\n\\n"
    for i, r in enumerate(results[:3]):
        prompt += f"{i+1}. {r['content']} (score: {r['score']})\\n"
    return prompt + "\n\n"