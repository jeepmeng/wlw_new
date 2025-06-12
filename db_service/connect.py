import asyncpg
from config.settings import settings  # 你自己的 settings.py

async def connect_db():
    db = settings.wmx_database
    return await asyncpg.connect(
        host=db.DB_HOST,
        port=int(db.DB_PORT),
        user=db.DB_USER,
        password=db.DB_PASSWORD,
        database=db.DB_NAME
    )



