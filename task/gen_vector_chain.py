
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




@celery_app.task(bind=True, name="encode_text_task")
def encode_text_task(self, text: str, use_redis: bool = False) -> list:
    """
    通用向量计算任务：
    - 支持 chain（直接返回向量）
    - 支持异步轮询（写入 Redis）
    """
    try:
        logger.info(f"处理文本向量任务：{text}")
        vec = model.encode(text, normalize_embeddings=True).tolist()

        if use_redis:
            task_id = self.request.id
            redis_key = f"vec_result:{task_id}"
            redis_client.set(redis_key, json.dumps(vec), ex=3600)
            logger.info(f"写入 Redis 结果: {redis_key}")

        return vec

    except Exception as e:
        logger.exception(f"向量化任务失败，文本: {text}，错误: {str(e)}")
        raise e