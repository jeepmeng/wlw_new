# vector_engine.py

from vector_service.vector_tasks import encode_text_task
from vector_service.vector_tasks import redis_client
from db_service.vector_service import async_query_similar_sentences
import json

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