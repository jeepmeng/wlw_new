from datetime import datetime
from typing import List
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from config.settings import settings
from task.gen_vector_chain import encode_text_task
from task.gen_ques import generate_questions_task
from celery import group


es = AsyncElasticsearch(
    hosts=[settings.elasticsearch.host],
    basic_auth=(settings.elasticsearch.username, settings.elasticsearch.password)
)


async def update_chunk_in_es(chunk_id: str, new_content: str, update_by: str):
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await es.update(
        index=settings.elasticsearch.indexes.chunk_index,
        id=chunk_id,
        doc={"doc": {
            "content": new_content,
            "update_by": update_by,
            "update_time": update_time
        }}
    )


async def delete_questions_by_chunk(chunk_id: str):
    await es.delete_by_query(
        index=settings.elasticsearch.indexes.ques_index,
        body={"query": {"term": {"ori_sent_id": chunk_id}}}
    )


async def generate_questions_by_llm(content: str) -> List[str]:
    task = generate_questions_task.delay(content)
    return task.get(timeout=30)


async def encode_questions_to_vectors(questions: List[str]) -> List[list]:
    task_group = group(encode_text_task.s(q) for q in questions)()
    return task_group.get(timeout=30)


async def bulk_insert_questions(chunk_id: str, questions: List[str], vectors: List[list]):
    actions = []
    for question, vector in zip(questions, vectors):
        qid = f"{chunk_id}_{abs(hash(question))}"
        actions.append({
            "_index": settings.elasticsearch.indexes.ques_index,
            "_id": qid,
            "_source": {
                "ori_sent_id": chunk_id,
                "ori_ques_sent": question,
                "ques_vector": vector
            }
        })

    await async_bulk(es, actions)


async def encode_single_question(question: str) -> list:
    task = encode_text_task.delay(question)
    return task.get(timeout=10)


async def update_question_in_es(question_id: str, new_question: str, vector: list):
    await es.update(
        index=settings.elasticsearch.indexes.ques_index,
        id=question_id,
        doc={"doc": {
            "ori_ques_sent": new_question,
            "ques_vector": vector
        }}
    )