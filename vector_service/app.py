from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import uvicorn
import os
from utils.logger import setup_logger  # ✅ 导入通用 logger
from config.settings import load_config  # ✅ 读取配置


# ✅ 加载配置
config = load_config()
vector_config = config.vector_service

# ✅ 初始化 logger
logger = setup_logger("vector_service")
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(base_dir, "..", "bge-large-zh-v1.5")
# ✅ 加载模型
model = SentenceTransformer(model_path, device="cpu")

# ✅ 初始化 FastAPI app
app = FastAPI()

class TextInput(BaseModel):
    text: str

@app.post("/vector/encode")
def encode_vector(input: TextInput):
    logger.info(f"收到文本：{input.text}")
    try:
        vec = model.encode(input.text, normalize_embeddings=True).tolist()
        return {"vector": vec}
    except Exception as e:
        logger.error("向量化失败", exc_info=True)
        raise

if __name__ == "__main__":
    # ✅ 启动 uvicorn（注意直接传 app 对象，而不是字符串）
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=vector_config.port,
        log_level="debug",  # ✅ 防止 uvicorn 抢日志，只输出 warning+
        # access_log=True,   # ✅ 关闭访问日志，避免干扰控制台
        reload=True
    )