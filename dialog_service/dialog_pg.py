from uuid import UUID
from typing import Optional, List
from db_service.pg_pool import pg_conn  # ✅ 使用你已有的连接管理器

# ✅ 创建对话会话
async def create_session(user_id: str, title: Optional[str] = None) -> int:
    async with pg_conn() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO m_talk (user_id, title, created_at)
            VALUES ($1, $2, NOW())
            """,
            user_id, title
        )
    return row["talk_id"]
# ✅ 插入对话消息
async def insert_message(talk_id: int, user_id: int, input_content: str, output_content: str):
    async with pg_conn() as conn:
        await conn.execute(
            """
            INSERT INTO m_talk_record (talk_id, create_by, input_content, output_content, create_time)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            talk_id, user_id, input_content, output_content
        )

# ✅ 查询对话历史
async def get_history_by_session(talk_id: int) -> List[dict]:
    async with pg_conn() as conn:
        rows = await conn.fetch(
            """
            SELECT create_by, input_content, output_content
            FROM m_talk_record
            WHERE talk_id = $1
            ORDER BY create_by DESC
            LIMIT 3
            """,
            talk_id
        )
        history = []
        # print(type(rows))
        for row in rows:
            # print(row)
            history.append({"role": "user", "content": row["input_content"]})
            history.append({"role": "assistant", "content": row["output_content"]})
        print('history-------------------',history)
        return history
        # return [dict(row) for row in reversed(rows)]

# ✅ 查询该 talk_id 所属的 user_id
async def get_session_user(talk_id: int):
    async with pg_conn() as conn:
        row = await conn.fetchrow(
            """
            SELECT create_by
            FROM m_talk_record
            WHERE talk_id = $1
            """,
            talk_id
        )
        return dict(row) if row else None


