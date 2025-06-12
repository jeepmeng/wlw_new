import asyncpg
from asyncpg import Pool, Connection
from config.settings import settings
from contextlib import asynccontextmanager

# 全局连接池对象
_pg_pool: Pool = None


# ✅ 初始化连接池（在 FastAPI startup 事件中调用）
async def init_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        db = settings.wmx_database
        _pg_pool = await asyncpg.create_pool(
            host=db.DB_HOST,
            port=int(db.DB_PORT),
            user=db.DB_USER,
            password=db.DB_PASSWORD,
            database=db.DB_NAME,
            min_size=5,   # 可根据项目调整
            max_size=20,  # 可根据并发量调整
            timeout=60
        )
        print("✅ PostgreSQL 连接池创建成功")


# ✅ 获取连接（手动）
async def get_pg_conn() -> Connection:
    if _pg_pool is None:
        raise RuntimeError("PostgreSQL连接池未初始化，请先调用 init_pg_pool()")
    return await _pg_pool.acquire()


# ✅ 释放连接（手动）
async def release_pg_conn(conn: Connection):
    if _pg_pool and conn:
        await _pg_pool.release(conn)


# ✅ 清理连接池（在 FastAPI shutdown 事件中调用）
async def close_pg_pool():
    global _pg_pool
    if _pg_pool:
        await _pg_pool.close()
        _pg_pool = None
        print("🧹 PostgreSQL 连接池已关闭")


# ✅ 异步上下文管理器（推荐使用）
@asynccontextmanager
async def pg_conn():
    conn = await get_pg_conn()
    try:
        yield conn
    finally:
        await release_pg_conn(conn)