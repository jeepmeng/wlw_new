from celery import Celery
from config.settings import load_config

# ✅ 加载配置
config = load_config()
vector_config = config.vector_service

# ✅ 创建 Celery 实例
celery_app = Celery(
    "my_tasks",
    broker=vector_config.redis_broker,
    backend=vector_config.redis_backend,
    include=["task.gen_ques", "task.gen_vector_chain", "task.db_interact"]  # ✅ 可以追加其他模块
)

# ✅ 可选 Celery 配置项（如序列化器）
celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='Asia/Shanghai',
    enable_utc=False
)

# ✅ Worker 启动后自动初始化 PostgreSQL 连接池
from db_service.pg_pool import init_pg_pool

@celery_app.on_after_configure.connect
def setup_pg_pool_after_worker_config(sender, **kwargs):
    import asyncio
    asyncio.run(init_pg_pool())