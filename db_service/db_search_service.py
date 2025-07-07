# 使用 SQLAlchemy Session 执行原始 SQL
from sqlalchemy.orm import Session
from sqlalchemy import text
import requests
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import bindparam
import jieba
import asyncpg
from typing import List





# async def async_query_similar_sentences(target_vector: list[float], db: AsyncSession) -> list[dict]:
#     # 向量转 PostgreSQL 兼容格式（数组写法）
#     vector_str = "[" + ",".join(map(str, target_vector)) + "]"
#
#     # 第一步：向量相似度查询
#     sql1 = """
#     SELECT ori_sent_id, ori_ques_sent, 1 - (ques_vector <=> :vec) AS similarity
#     FROM wmx_ques
#     WHERE (1 - (ques_vector <=> :vec)) > 0.6
#     ORDER BY similarity DESC
#     LIMIT 10;
#     """
#     result1 = await db.execute(text(sql1), {"vec": vector_str})
#     rows1 = result1.fetchall()
#
#     if not rows1:
#         return []
#
#     sent_ids = [row[0] for row in rows1]
#
#     # 第二步：根据 ori_sent_id 查内容
#     sql2 = text("""
#     SELECT content
#     FROM m_zhisk_results
#     WHERE uu_id IN :uids;
#     """).bindparams(bindparam("uids", expanding=True))
#
#     result2 = await db.execute(sql2, {"uids": sent_ids})
#     rows2 = result2.fetchall()
#
#     res = [{"关联结果": i + 1, "text": row[0]} for i, row in enumerate(rows2)]
#     # print("✅ 最终返回内容：", res)
#     return res



async def async_query_similar_sentences(target_vector: List[float], conn: asyncpg.Connection) -> List[dict]:
    # ✅ 将 Python list[float] 转换为 PostgreSQL 向量格式字符串
    vector_str = "[" + ",".join(map(str, target_vector)) + "]"

    # 第一步：向量相似度查询
    sql1 = """
        SELECT ori_sent_id, ori_ques_sent, 1 - (ques_vector <=> CAST($1 AS vector)) AS similarity
        FROM wmx_ques
        WHERE (1 - (ques_vector <=> CAST($1 AS vector))) > 0.6
        ORDER BY similarity DESC
        LIMIT 10;
        """
    rows1 = await conn.fetch(sql1, vector_str)

    if not rows1:
        return []

    sent_ids = [r["ori_sent_id"] for r in rows1]

    # 第二步：查匹配内容
    sql2 = """
        SELECT content 
        FROM m_zhisk_results 
        WHERE uu_id = ANY($1);
        """
    rows2 = await conn.fetch(sql2, sent_ids)

    return [{"关联结果": i + 1, "text": r["content"]} for i, r in enumerate(rows2)]



async def query_similar_sentences(target_vector: list[float], db: Session) -> list[str]:
    # 向量转 PostgreSQL 兼容格式
    vector_str = "[" + ",".join(map(str, target_vector)) + "]"

    # 第一步：相似句子查询
    sql1 = """
    SELECT ori_sent_id, ori_ques_sent, 1 - (ques_vector <=> :vec) AS similarity
    FROM wmx_ques
    WHERE (1 - (ques_vector <=> :vec)) > 0.6
    ORDER BY similarity DESC
    LIMIT 10;
    """
    result1 = db.execute(text(sql1), {"vec": vector_str}).fetchall()

    if not result1:
        return []

    sent_ids = [row[0] for row in result1]

    # 第二步：根据 ori_sent_id 查内容
    sql2 = """
    SELECT content 
    FROM m_zhisk_results 
    WHERE uu_id IN :uids;
    """
    result2 = db.execute(text(sql2), {"uids": tuple(sent_ids)}).fetchall()


    # return [row[0] for row in result2]
    res = [{"关联结果": i + 1, "text": row[0]} for i, row in enumerate(result2)]
    # print(res)
    return res



# async def async_hybrid_search(query_text: str, vector: list[float], db: AsyncSession, top_k=15, lambda_=0.8):
#
#
#     # 分词获取关键词用于 BM25 检索
#     terms = jieba.lcut(query_text)
#     keyword_query = ' '.join(set(terms))
#     vector_str = f"[{','.join(map(str, vector))}]"
#
#     # 混合查询 SQL
#     sql = text("""
#         WITH vector_hits AS (
#             SELECT ori_sent_id, 1 - (ques_vector <#> CAST(:vec AS vector)) AS vector_score
#             FROM wmx_ques
#             WHERE ques_vector IS NOT NULL
#             ORDER BY ques_vector <#> CAST(:vec AS vector)
#             LIMIT 100
#         ),
#         vector_agg AS (
#             SELECT r.id, MAX(v.vector_score) AS vector_score
#             FROM vector_hits v
#             JOIN m_zhisk_results r ON r.uu_id = v.ori_sent_id
#             GROUP BY r.id
#         ),
#         bm25_hits AS (
#             SELECT id, pgroonga_score(tableoid, ctid) AS bm25_score
#             FROM m_zhisk_results
#             WHERE content &@~ :kw
#         )
#         SELECT r.id, r.content,
#                COALESCE(b.bm25_score, 0) AS bm25_score,
#                COALESCE(v.vector_score, 0) AS vector_score,
#                (1 - :lambda_) * COALESCE(b.bm25_score, 0) + :lambda_ * COALESCE(v.vector_score, 0) AS final_score
#         FROM m_zhisk_results r
#         LEFT JOIN bm25_hits b ON r.id = b.id
#         LEFT JOIN vector_agg v ON r.id = v.id
#         WHERE b.id IS NOT NULL OR v.id IS NOT NULL
#         ORDER BY final_score DESC
#         LIMIT :top_k
#     """)
#
#     # 执行 SQL 查询
#     result = await db.execute(sql, {
#         "vec": vector_str,
#         "kw": keyword_query,
#         "lambda_": lambda_,
#         "top_k": top_k
#     })
#     rows = result.fetchall()
#
#     # 结构化结果输出
#     return [
#         {
#             "id": r[0],
#             "content": r[1],
#             "bm25Score": float(r[2]),
#             "vectorScore": float(r[3]),
#             "finalScore": float(r[4])
#         } for r in rows
#     ]


async def async_hybrid_search(query_text: str, vector: List[float], conn: asyncpg.Connection, top_k=15, lambda_=0.8):
    # 分词获取关键词用于 BM25 检索
    terms = jieba.lcut(query_text)
    keyword_query = ' '.join(set(terms))

    # sql = """
    #     WITH vector_hits AS (
    #         SELECT ori_sent_id, 1 - (ques_vector <#> $1) AS vector_score
    #         FROM wmx_ques
    #         WHERE ques_vector IS NOT NULL
    #         ORDER BY ques_vector <#> $1
    #         LIMIT 100
    #     ),
    #     vector_agg AS (
    #         SELECT r.id, MAX(v.vector_score) AS vector_score
    #         FROM vector_hits v
    #         JOIN m_zhisk_results r ON r.uu_id = v.ori_sent_id
    #         GROUP BY r.id
    #     ),
    #     bm25_hits AS (
    #         SELECT id, pgroonga_score(tableoid, ctid) AS bm25_score
    #         FROM m_zhisk_results
    #         WHERE content &@~ $2
    #     )
    #     SELECT r.id, r.content,
    #            COALESCE(b.bm25_score, 0) AS bm25_score,
    #            COALESCE(v.vector_score, 0) AS vector_score,
    #            (1 - $3) * COALESCE(b.bm25_score, 0) + $3 * COALESCE(v.vector_score, 0) AS final_score
    #     FROM m_zhisk_results r
    #     LEFT JOIN bm25_hits b ON r.id = b.id
    #     LEFT JOIN vector_agg v ON r.id = v.id
    #     WHERE b.id IS NOT NULL OR v.id IS NOT NULL
    #     ORDER BY final_score DESC
    #     LIMIT $4
    # """

    vector_str = "[" + ",".join(map(str, vector)) + "]"  # 转为 pgvector 兼容格式

    sql = """
        WITH vector_hits AS (
            SELECT ori_sent_id, 1 - (ques_vector <#> CAST($1 AS vector)) AS vector_score
            FROM wmx_ques
            WHERE ques_vector IS NOT NULL
            ORDER BY ques_vector <#> CAST($1 AS vector)
            LIMIT 100
        ),
        vector_agg AS (
            SELECT r.id, MAX(v.vector_score) AS vector_score
            FROM vector_hits v
            JOIN m_zhisk_results r ON r.uu_id = v.ori_sent_id
            GROUP BY r.id
        ),
        bm25_hits AS (
            SELECT id, pgroonga_score(tableoid, ctid) AS bm25_score
            FROM m_zhisk_results
            WHERE content &@~ $2
        )
        SELECT r.id, r.content,
               COALESCE(b.bm25_score, 0) AS bm25_score,
               COALESCE(v.vector_score, 0) AS vector_score,
               (1 - $3) * COALESCE(b.bm25_score, 0) + $3 * COALESCE(v.vector_score, 0) AS final_score
        FROM m_zhisk_results r
        LEFT JOIN bm25_hits b ON r.id = b.id
        LEFT JOIN vector_agg v ON r.id = v.id
        WHERE b.id IS NOT NULL OR v.id IS NOT NULL
        ORDER BY final_score DESC
        LIMIT $4
    """



    # rows = await conn.fetch(sql, vector, keyword_query, lambda_, top_k)
    rows = await conn.fetch(sql, vector_str, keyword_query, lambda_, top_k)
    return [
        {
            "id": r["id"],
            "content": r["content"],
            "bm25Score": float(r["bm25_score"]),
            "vectorScore": float(r["vector_score"]),
            "finalScore": float(r["final_score"])
        } for r in rows
    ]


def get_text_vector(text: str) -> list[float]:
    resp = requests.post("http://localhost:8001/vector/encode", json={"text": text})
    resp.raise_for_status()
    return resp.json()["vector"]


