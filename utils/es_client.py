from elasticsearch import Elasticsearch
from config.settings import settings

def get_es_client() -> Elasticsearch:
    """
    获取配置化的 Elasticsearch 客户端实例

    读取 config.settings.elasticsearch 中的 host/username/password。

    :return: Elasticsearch 实例
    """
    es_cfg = settings.elasticsearch

    return Elasticsearch(
        hosts=[es_cfg.host],
        basic_auth=(es_cfg.username, es_cfg.password) if es_cfg.username else None
    )