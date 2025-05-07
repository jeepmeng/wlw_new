from pydantic import BaseModel
import yaml
import os

# ✅ 数据库配置
class DBConfig(BaseModel):
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

# ✅ 向量服务配置，包括模型路径、端口、Redis 连接地址
class VectorConfig(BaseModel):
    model_path: str
    port: int
    redis_broker: str
    redis_backend: str

# ✅ 顶层配置结构
class Settings(BaseModel):
    env: str
    wmx_database: DBConfig
    vector_service: VectorConfig

# ✅ 配置加载函数
def load_config() -> Settings:
    env = os.getenv("ENV", "dev")  # 默认环境为 dev
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, f"config.{env}.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在：{config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return Settings(**raw)

# ✅ 全局配置对象（只加载一次）
settings = load_config()