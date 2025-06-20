from celery import Celery
from config.settings import load_config

config = load_config()
vector_config = config.vector_service

celery_app = Celery(
    "my_tasks",
    broker=vector_config.redis_broker,
    backend=vector_config.redis_backend,
    include=["task.gen_ques", "task.gen_vector"]
)

# ✅ 自动发现 task.ques / task.vector 中的 @shared_task
# celery_app.autodiscover_tasks(["task"])
