# 使用 SQLAlchemy Session 执行原始 SQL
from sqlalchemy.orm import Session
from sqlalchemy import text
import requests



def query_similar_sentences(target_vector: list[float], db: Session) -> list[str]:
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
    print(res)
    return res


def get_text_vector(text: str) -> list[float]:
    resp = requests.post("http://localhost:8001/vector/encode", json={"text": text})
    resp.raise_for_status()
    return resp.json()["vector"]