# from celery import Celery
# from config.settings import load_config
# import multiprocessing
# multiprocessing.set_start_method("spawn", force=True)
# # ✅ 加载配置
# config = load_config()
# vector_config = config.vector_service
#
# # ✅ 创建 Celery 实例
# celery_app = Celery(
#     "my_tasks",
#     broker=vector_config.redis_broker,
#     backend=vector_config.redis_backend,
#     include=["task.gen_ques", "task.gen_vector_chain", "task.db_interact","task.file_parse_pipeline"]  # ✅ 可以追加其他模块
# )
#
# # ✅ 可选 Celery 配置项（如序列化器）
# celery_app.conf.update(
#     task_serializer='json',
#     result_serializer='json',
#     accept_content=['json'],
#     timezone='Asia/Shanghai',
#     enable_utc=False
# )
#
# # ✅ Worker 启动后自动初始化 PostgreSQL 连接池
# from db_service.pg_pool import init_pg_pool
#
# @celery_app.on_after_configure.connect
# def setup_pg_pool_after_worker_config(sender, **kwargs):
#     import asyncio
#     asyncio.run(init_pg_pool())
#






from celery import Celery
from config.settings import load_config
import multiprocessing

# ✅ macOS 下防止 CoreFoundation fork 报错
multiprocessing.set_start_method("spawn", force=True)

# ✅ 加载配置
config = load_config()
vector_config = config.vector_service

# ✅ 创建 Celery 实例
celery_app = Celery(
    "my_tasks",
    broker=vector_config.redis_broker,
    backend=vector_config.redis_backend,
    include=[
        "task.gen_ques",
        "task.gen_vector_chain",
        "task.db_interact",
        "task.file_parse_pipeline"
    ]
)

# ✅ 配置
celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='Asia/Shanghai',
    enable_utc=False
)

# # ✅ 推荐初始化 PG 的方式：异步线程防止事件循环冲突
# from db_service.pg_pool import init_pg_pool
#
# @celery_app.on_after_finalize.connect
# def setup_pg_pool_after_worker_start(sender, **kwargs):
#     import threading
#     import asyncio
#
#     def runner():
#         try:
#             asyncio.run(init_pg_pool())
#         except Exception as e:
#             print(f"⚠️ PG初始化失败: {e}")
#
#     threading.Thread(target=runner).start()

# ✅ 可选: 支持命令行直接运行调试
if __name__ == "__main__":
    celery_app.worker_main()