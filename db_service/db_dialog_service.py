# talk_insert_pg.py

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from session import get_async_db  # 如果需要，可引用异步 session 创建器



# ✅ 异步插入函数
async def insert_talk_vectors_to_db_async(
    db_session: AsyncSession,
    talk_id: str,
    create_by: int,
    message: str,
    response: str
):
    """
    使用异步 SQLAlchemy 执行插入到 m_talk_record 表。
    参数：传入 FastAPI 注入的 AsyncSession。
    """
    try:
        sql = text("""
            INSERT INTO m_talk_record (talk_id, create_by, input_content, output_content, create_time)
            VALUES (:talk_id, :create_by, :message, :response, NOW()::TIMESTAMP(0))
        """)
        await db_session.execute(sql, {
            "talk_id": talk_id,
            "create_by": create_by,
            "message": message,
            "response": response
        })
        await db_session.commit()
    except Exception as e:
        await db_session.rollback()
        print(f"[insert_talk_vectors_to_db_async] 插入失败: {e}")
        raise
