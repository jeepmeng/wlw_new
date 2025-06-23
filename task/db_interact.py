from .celery_app import celery_app
import asyncio

@celery_app.task
def insert_vectors_to_db(vector: list, meta: dict):
    async def _run():
        from db_service.pg_pool import pg_conn
        import json

        vector_str = '[' + ','.join(map(str, vector)) + ']'
        json_str = json.dumps(meta["jsons"])

        sql = """
        INSERT INTO m_zhisk_results (zhisk_file_id, content, create_time, vector, ori_json, code, category, uu_id)
        VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
        """
        async with pg_conn() as conn:
            await conn.execute(
                sql,
                meta["zhisk_file_id"],
                meta["content"],
                vector_str,
                json_str,
                meta["code"],
                meta["category"],
                str(meta["uu_id"])
            )

    asyncio.run(_run())

@celery_app.task
def insert_ques_batch_task(uu_id: str, sentences: list, vectors: list):
    async def _run():
        from db_service.pg_pool import pg_conn
        data = [
            (uu_id, sent, '[' + ','.join(map(str, vec)) + ']')
            for sent, vec in zip(sentences, vectors)
        ]
        sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
        async with pg_conn() as conn:
            await conn.executemany(sql, data)

    asyncio.run(_run())