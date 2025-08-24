from typing import List, Optional
from pydantic import BaseModel
import yaml
import os

# config/settings.py
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())  # 自动向上查找最近的 .env


ENV = os.getenv("ENV", "dev")



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

class SessionCacheConfig(BaseModel):
    redis_url: str
    expire_seconds: int

# Elasticsearch 索引名配置
class ESIndexConfig(BaseModel):
    file_index: str
    chunk_index: str
    ques_index: str

# ✅ Elasticsearch 主配置
class ESConfig(BaseModel):
    host: str
    knowledge_base: str
    question_base: str
    username: str = "elastic"
    password: str = "wlw123456"
    indexes: ESIndexConfig


class TaskDefaults(BaseModel):
    store_flags: List[str] = ["pg", "es"]
    pg_enable: bool = True
    es_enable: bool = True


class Deepseek(BaseModel):
    api_key: str
    base_url: str

class QwenConfig(BaseModel):
    api_key: str
    model: str = "qwen-plus"


# ✅ 顶层配置结构
class Settings(BaseModel):
    env: str
    wmx_database: DBConfig
    vector_service: VectorConfig
    elasticsearch: ESConfig
    task_defaults: TaskDefaults
    deepseek: Deepseek
    session_cache: SessionCacheConfig
    qwen: QwenConfig


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