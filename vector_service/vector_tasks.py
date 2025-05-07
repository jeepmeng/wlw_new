from celery import Celery
from sentence_transformers import SentenceTransformer
import os

# ✅ 日志、配置、模型路径复用你原来的逻辑
from utils.logger import setup_logger
from config.config import load_config

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

# ✅ 加载模型
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(base_dir, "..", "bge-large-zh-v1.5")
logger.info(f"加载向量模型路径: {model_path}")
model = SentenceTransformer(model_path, device="cpu")

# ✅ 定义任务
@celery_app.task(name="vector.encode")
def encode_text_task(text: str):
    logger.info(f"处理文本向量任务：{text}")
    vec = model.encode(text, normalize_embeddings=True).tolist()
    return vec