from sqlalchemy import create_engine, text  # ✅ 加了 text
from sqlalchemy.orm import sessionmaker
from config.settings import settings  # ✅ 使用 Settings 实例
from urllib.parse import quote_plus
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


# ✅ 安全编码用户名和密码
db_cfg = settings.wmx_database
user = quote_plus(db_cfg.DB_USER)
password = quote_plus(db_cfg.DB_PASSWORD)
host = db_cfg.DB_HOST
port = db_cfg.DB_PORT
db = db_cfg.DB_NAME

# ✅ 拼接连接字符串
# DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"
DATABASE_URL = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"

# engine = create_engine(DATABASE_URL, pool_pre_ping=True)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

async_engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)

# # ✅ 依赖注入函数
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()


async def get_async_db():
    async with AsyncSessionLocal() as session:
        yield session



# ✅ 测试数据库连接
def test_db_connection():
    db_gen = get_async_db()
    db = next(db_gen)

    try:
        result = db.execute(text("SELECT version();"))  # ✅ 用 text 包裹
        version = result.fetchone()
        print("✅ 数据库连接成功，版本信息：", version[0])
    except Exception as e:
        print("❌ 数据库连接失败：", str(e))
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


import asyncio
from sqlalchemy import text
# from db_service.session import get_async_db

async def test_async_db_connection():
    # 使用 get_async_db() 获取一个异步数据库连接
    async for db in get_async_db():
        try:
            # 执行一条简单的 SQL 语句
            result = await db.execute(text("SELECT version();"))
            version = result.scalar()
            print(f"✅ 数据库连接成功，PostgreSQL 版本：{version}")
        except Exception as e:
            print(f"❌ 数据库连接或执行失败：{e}")


if __name__ == "__main__":
    # test_db_connection()
    asyncio.run(test_async_db_connection())