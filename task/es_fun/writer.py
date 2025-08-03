from utils.es_client import get_es_client
from utils.logger_manager import get_logger
es = get_es_client()

def insert_file_meta_to_es(index_name: str, file_meta: dict):
    logger = get_logger("insert_file_meta_to_es")
    logger.info(f"[ES] PUT {index_name} doc_id={file_meta.get('zhisk_file_id')}")
    """
    将文件源信息写入 Elasticsearch 索引

    :param index_name: ES 索引名
    :param file_meta: 文件元信息字典，需包含 zhisk_file_id 字段
    """
    if "zhisk_file_id" not in file_meta:
        raise ValueError("file_meta must contain 'zhisk_file_id'")

    es.index(index=index_name, id=file_meta["zhisk_file_id"], document=file_meta)

def insert_chunk_to_es(index_name: str, chunk_doc: dict):
    logger = get_logger("insert_chunk_to_es")
    logger.info(f"[ES] PUT {index_name} doc_id={chunk_doc.get('uu_id')}")
    """
    将段落信息写入 Elasticsearch

    :param index_name: ES 索引名
    :param chunk_doc: 段落内容文档，需包含 uu_id 字段
    """
    if "uu_id" not in chunk_doc:
        raise ValueError("chunk_doc must contain 'uu_id'")

    es.index(index=index_name, id=chunk_doc["uu_id"], document=chunk_doc)

def insert_question_vector_to_es(index_name: str, doc: dict):
    """
    向 ES 写入一条符合 wmx_ques 的问题向量文档

    :param index_name: 索引名，如 "wmx_ques"
    :param doc: 字段需包含 id、ori_sent_id、ori_ques_sent、ques_vector
    """
    if "id" not in doc:
        raise ValueError("doc must contain 'id'")

    es.index(index=index_name, id=doc["id"], document=doc)