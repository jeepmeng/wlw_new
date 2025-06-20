# ✅ 这是拆分后的结构：
# - 保留 Celery encode_text_task 异步执行向量计算
# - 任务结果不直接 return，而是存储到 Redis
# - 主流程通过 task_id 查询 Redis 获取结果

from celery import Celery
from sentence_transformers import SentenceTransformer
from redis import Redis
import json
import os

# ✅ 日志、配置、模型路径复用你原来的逻辑
from utils.logger import setup_logger
from config.settings import load_config

# ✅ 初始化
config = load_config()
vector_config = config.vector_service
logger = setup_logger("celery_worker")

# ✅ 创建 Celery 应用
celery_app = Celery(
    "vector_tasks",
    broker=vector_config.redis_broker,   # e.g. redis://localhost:6379/0
    backend=vector_config.redis_backend  # e.g. redis://localhost:6379/1
)

# ✅ 连接 Redis 手动存储结果（防止 Celery backend 序列化崩溃）
redis_client = Redis.from_url(vector_config.redis_backend)

# ✅ 加载模型
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(base_dir, "..", "bge-large-zh-v1.5")
logger.info(f"加载向量模型路径: {model_path}")
model = SentenceTransformer(model_path, device="cpu")

# ✅ 定义任务（仅存储 task_id → 向量 映射）
@celery_app.task(name="vector.encode")
def encode_text_task(text: str):
    logger.info(f"处理文本向量任务：{text}")
    vec = model.encode(text, normalize_embeddings=True).tolist()

    # ✅ 自定义存储结果（以 task_id 为 key）
    task_id = encode_text_task.request.id
    redis_key = f"vec_result:{task_id}"
    redis_client.set(redis_key, json.dumps(vec), ex=3600)  # 1 小时过期

    return "OK"  # ✅ 只返回状态，实际数据从 Redis 查

# ✅ 配套接口函数（可用于 FastAPI 路由中）
def submit_vector_task(text: str):
    task = encode_text_task.delay(text)
    return {"msg": "任务已提交", "task_id": task.id}


def get_vector_result(task_id: str):
    redis_key = f"vec_result:{task_id}"
    vec_json = redis_client.get(redis_key)
    if vec_json:
        vec = json.loads(vec_json)
        return {"status": "SUCCESS", "vector": vec}
    else:
        return {"status": "PENDING", "vector": None}
