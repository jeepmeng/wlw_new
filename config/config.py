# config/config.py

# from pydantic import BaseModel
# import yaml
# import os
#
# # ✅ 数据模型
# class DBConfig(BaseModel):
#     DB_HOST: str
#     DB_PORT: str
#     DB_NAME: str
#     DB_USER: str
#     DB_PASSWORD: str
#
# # class APIConfig(BaseModel):
# #     vector_api: str
#
# class Settings(BaseModel):
#     env: str
#     wmx_database: DBConfig
#     # api_urls: APIConfig
#
# # ✅ 加载配置函数
# def load_config() -> Settings:
#     env = os.getenv("ENV", "dev")  # 从环境变量读取环境名
#     config_file = f"config.{env}.yaml"
#     with open(config_file, "r", encoding="utf-8") as f:
#         raw = yaml.safe_load(f)
#     return Settings(**raw)
#
# # ✅ 全局配置对象（只加载一次）
# settings = load_config()



# config/config.py

from pydantic import BaseModel
import yaml
import os

class DBConfig(BaseModel):
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

class VectorConfig(BaseModel):
    model_path: str
    port: int

class Settings(BaseModel):
    env: str
    wmx_database: DBConfig
    vector_service: VectorConfig

def load_config() -> Settings:
    env = os.getenv("ENV", "dev")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, f"config.{env}.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在：{config_path}")
    # config_file = f"./config.{env}.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Settings(**raw)

# ✅ 全局配置对象（只加载一次）
settings = load_config()