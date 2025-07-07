import json
from typing import List, Tuple
from db_service.pg_pool import pg_conn


async def insert_vectors_to_db(
    zhisk_file_id: str, content: str, vector: List[float], jsons: dict,
    code: str, category: str, uu_id: str
):
    vector_str = '[' + ','.join(map(str, vector)) + ']'
    json_str = json.dumps(jsons)

    sql = """
    INSERT INTO m_zhisk_results (zhisk_file_id, content, create_time, vector, ori_json, code, category, uu_id)
    VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
    """

    async with pg_conn() as conn:
        async with conn.transaction():
            await conn.execute(sql, zhisk_file_id, content, vector_str, json_str, code, category, str(uu_id))


async def insert_ques_batch(
    uu_id: str, sentences: List[str], vectors: List[List[float]]
):
    data: List[Tuple[str, str, str]] = [
        (uu_id, sent, '[' + ','.join(map(str, vec)) + ']')
        for sent, vec in zip(sentences, vectors)
    ]
    sql = "INSERT INTO wmx_ques (ori_sent_id, ori_ques_sent, ques_vector) VALUES ($1, $2, $3)"
    async with pg_conn() as conn:
        async with conn.transaction():
            await conn.executemany(sql, data)


async def update_by_id(
    update_data: dict, record_id: int, updata_ques: List[str],
    up_ques_vect: List[List[float]]
):
    for key, value in update_data.items():
        if "vector" in key and isinstance(value, (list, tuple)):
            update_data[key] = '[' + ','.join(map(str, value)) + ']'

    set_clause = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(update_data)])
    values = list(update_data.values()) + [record_id]
    update_sql = f"UPDATE m_zhisk_results SET {set_clause} WHERE id = ${len(values)}"

    async with pg_conn() as conn:
        async with conn.transaction():
            await conn.execute(update_sql, *values)

        # 获取 uu_id
        row = await conn.fetchrow("SELECT uu_id FROM m_zhisk_results WHERE id = $1", record_id)
        uu_id = row["uu_id"]

        rows = await conn.fetch("SELECT id FROM wmx_ques WHERE ori_sent_id = $1", uu_id)
        for k, r in enumerate(rows):
            ques_sql = "UPDATE wmx_ques SET ori_ques_sent=$1, ques_vector=$2 WHERE id=$3"
            vector_str = '[' + ','.join(map(str, up_ques_vect[k])) + ']'
            await conn.execute(ques_sql, updata_ques[k], vector_str, r["id"])


async def update_field_by_id(
    table_name: str, field_name: str, new_value, record_id: int
):
    sql = f"UPDATE {table_name} SET {field_name} = $1 WHERE id = $2"
    async with pg_conn() as conn:
        async with conn.transaction():
            await conn.execute(sql, new_value, record_id)