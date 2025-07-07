# import asyncpg
# from asyncpg import Pool, Connection
# from config.settings import settings
# from contextlib import asynccontextmanager
# import asyncio
#
# _pg_pool: Pool = None
# _pg_pool_lock = asyncio.Lock()
#
# # ✅ 初始化连接池（幂等）
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
# # ✅ 获取连接：首次调用自动初始化
# async def get_pg_conn() -> Connection:
#     global _pg_pool
#     if _pg_pool is None:
#         await init_pg_pool()
#     return await _pg_pool.acquire()
#
# async def release_pg_conn(conn: Connection):
#     if _pg_pool and conn:
#         await _pg_pool.release(conn)
#
# async def close_pg_pool():
#     global _pg_pool
#     if _pg_pool:
#         await _pg_pool.close()
#         _pg_pool = None
#         print("🧹 PostgreSQL 连接池已关闭")
#
# # ✅ 异步上下文管理器
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

# ✅ 获取连接池（懒加载 + 锁保护）
async def get_pg_pool() -> Pool:
    global _pg_pool
    if _pg_pool is None:
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
    return _pg_pool

async def get_pg_conn() -> Connection:
    pool = await get_pg_pool()
    return await pool.acquire()

async def release_pg_conn(conn: Connection):
    if _pg_pool and conn:
        await _pg_pool.release(conn)

async def close_pg_pool():
    global _pg_pool
    if _pg_pool:
        await _pg_pool.close()
        _pg_pool = None
        print("🧹 PostgreSQL 连接池已关闭")

@asynccontextmanager
async def pg_conn():
    conn = await get_pg_conn()
    try:
        yield conn
    finally:
        await release_pg_conn(conn)