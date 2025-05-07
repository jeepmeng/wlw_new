from sqlalchemy import create_engine, text  # ✅ 加了 text
from sqlalchemy.orm import sessionmaker
from config.config import settings  # ✅ 使用 Settings 实例
from urllib.parse import quote_plus
# # ✅ 拼接 DATABASE_URL
# db_cfg = settings.wmx_database
# DATABASE_URL = f"postgresql://{db_cfg.DB_USER}:{db_cfg.DB_PASSWORD}@{db_cfg.DB_HOST}:{db_cfg.DB_PORT}/{db_cfg.DB_NAME}"


# ✅ 安全编码用户名和密码
db_cfg = settings.wmx_database
user = quote_plus(db_cfg.DB_USER)
password = quote_plus(db_cfg.DB_PASSWORD)
host = db_cfg.DB_HOST
port = db_cfg.DB_PORT
db = db_cfg.DB_NAME

# ✅ 拼接连接字符串
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ✅ 依赖注入函数
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ 测试数据库连接
def test_db_connection():
    db_gen = get_db()
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

if __name__ == "__main__":
    test_db_connection()