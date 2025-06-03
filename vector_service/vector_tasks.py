# ✅ vector_tasks.py
# Celery 异步任务：执行文本向量化并写入 Redis

from celery import Celery
from sentence_transformers import SentenceTransformer
from redis import Redis
from utils.logger import setup_logger
from config.settings import load_config
import os
import json


os.environ["TOKENIZERS_PARALLELISM"] = "false"


# ✅ 日志与配置
logger = setup_logger("celery_worker")
config = load_config()
vector_config = config.vector_service

# ✅ 初始化 Redis client
redis_client = Redis.from_url(vector_config.redis_backend)

# ✅ 创建 Celery 应用
celery_app = Celery(
    "vector_tasks",
    broker=vector_config.redis_broker,
    backend=vector_config.redis_backend
)

# ✅ 加载模型（每个 worker 启动时执行一次）
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(base_dir, "..", vector_config.model_path)
logger.info(f"加载向量模型路径: {model_path}")
model = SentenceTransformer(model_path, device="cpu")
model.encode("warmup", normalize_embeddings=True)  # ✅ 预热模型

# ✅ 定义异步任务
@celery_app.task(name="vector.encode")
def encode_text_task(text: str):
    logger.info(f"处理文本向量任务：{text}")
    vec = model.encode(text, normalize_embeddings=True).tolist()
    task_id = encode_text_task.request.id
    redis_key = f"vec_result:{task_id}"
    redis_client.set(redis_key, json.dumps(vec), ex=3600)
    return "OK"
