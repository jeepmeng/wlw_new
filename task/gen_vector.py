from .celery_app import celery_app
# from celery import shared_task
from config.settings import load_config
from utils.logger_manager import get_logger
from sentence_transformers import SentenceTransformer
from redis import Redis
import os
import json

os.environ["TOKENIZERS_PARALLELISM"] = "false"
logger = get_logger("gen_vector")
config = load_config()
vector_config = config.vector_service

# ✅ 初始化 Redis 和模型
redis_client = Redis.from_url(vector_config.redis_backend)
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(base_dir, "..", vector_config.model_path)
logger.info(f"加载向量模型路径: {model_path}")
model = SentenceTransformer(model_path, device="cpu")
model.encode("warmup", normalize_embeddings=True)

@celery_app.task(name="vector.encode")
def encode_text_task(text: str):
    try:
        logger.info(f"处理文本向量任务：{text}")
        vec = model.encode(text, normalize_embeddings=True).tolist()
        task_id = encode_text_task.request.id
        redis_key = f"vec_result:{task_id}"
        redis_client.set(redis_key, json.dumps(vec), ex=3600)
        return "job is done"

    except Exception as e:
        logger.exception(f"向量化任务失败，文本: {text}，错误: {str(e)}")
        return "ERROR"
