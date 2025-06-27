# import asyncpg
# from asyncpg import Pool, Connection
# from config.settings import settings
# from contextlib import asynccontextmanager
# import asyncio
#
# _pg_pool: Pool = None
# _pg_pool_lock = asyncio.Lock()  # 🔐 避免并发初始化
#
# # ✅ 幂等初始化连接池：FastAPI 或 Celery 启动时调用
# async def init_pg_pool():
#     global _pg_pool
#     async with _pg_pool_lock:
#         if _pg_pool is None:
#             db = settings.wmx_database
#             _pg_pool = await asyncpg.create_pool(
#                 host=db.DB_HOST,
#                 port=int(db.DB_PORT),
#                 user=db.DB_USER,
#                 password=db.DB_PASSWORD,
#                 database=db.DB_NAME,
#                 min_size=5,
#                 max_size=20,
#                 timeout=60
#             )
#             print("✅ PostgreSQL 连接池创建成功")
#
# # ✅ 获取连接
# async def get_pg_conn() -> Connection:
#     if _pg_pool is None:
#         raise RuntimeError("PostgreSQL连接池未初始化，请先调用 init_pg_pool()")
#     return await _pg_pool.acquire()
#
# # ✅ 释放连接
# async def release_pg_conn(conn: Connection):
#     if _pg_pool and conn:
#         await _pg_pool.release(conn)
#
# # ✅ 关闭连接池（用于 FastAPI shutdown）
# async def close_pg_pool():
#     global _pg_pool
#     if _pg_pool:
#         await _pg_pool.close()
#         _pg_pool = None
#         print("🧹 PostgreSQL 连接池已关闭")
#
# # ✅ 异步上下文管理器：async with pg_conn() 使用
# @asynccontextmanager
# async def pg_conn():
#     conn = await get_pg_conn()
#     try:
#         yield conn
#     finally:
#         await release_pg_conn(conn)



import asyncpg
from asyncpg import Pool, Connection
from config.settings import settings
from contextlib import asynccontextmanager
import asyncio

_pg_pool: Pool = None
_pg_pool_lock = asyncio.Lock()

# ✅ 初始化连接池（幂等）
async def init_pg_pool():
    global _pg_pool
    async with _pg_pool_lock:
        if _pg_pool is None:
            db = settings.wmx_database
            _pg_pool = await asyncpg.create_pool(
                host=db.DB_HOST,
                port=int(db.DB_PORT),
                user=db.DB_USER,
                password=db.DB_PASSWORD,
                database=db.DB_NAME,
                min_size=5,
                max_size=20,
                timeout=60
            )
            print("✅ PostgreSQL 连接池创建成功")

# ✅ 获取连接：首次调用自动初始化
async def get_pg_conn() -> Connection:
    global _pg_pool
    if _pg_pool is None:
        await init_pg_pool()
    return await _pg_pool.acquire()

async def release_pg_conn(conn: Connection):
    if _pg_pool and conn:
        await _pg_pool.release(conn)

async def close_pg_pool():
    global _pg_pool
    if _pg_pool:
        await _pg_pool.close()
        _pg_pool = None
        print("🧹 PostgreSQL 连接池已关闭")

# ✅ 异步上下文管理器
@asynccontextmanager
async def pg_conn():
    conn = await get_pg_conn()
    try:
        yield conn
    finally:
        await release_pg_conn(conn)