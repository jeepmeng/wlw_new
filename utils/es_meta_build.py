from datetime import datetime
import os
from pathlib import Path
import uuid


def build_file_meta(
    zhisk_file_id: str,
    file_path: str,
    file_type: str,
    zhisk_rows: int,
    create_by: str
) -> dict:
    """
    构造用于 Elasticsearch 写入的文件元信息结构

    :param zhisk_file_id: 文件唯一 ID（UUID 字符串）
    :param file_path: 本地路径（用于提取原始名与大小）
    :param file_type: 文件类型，如 'pdf' / 'docx'
    :param zhisk_rows: 分段数量
    :param create_by: 创建人（用户 ID）
    :return: 符合 ES 写入格式的 dict
    """
    file = Path(file_path)
    return {
        "zhisk_file_id": zhisk_file_id,
        "file_name": f"{datetime.now():%Y/%m/%d}/{file.name}",
        "original_name": file.name,
        "file_type": file_type,
        "file_size": round(os.path.getsize(file_path) / 1024, 2),  # 单位：KB
        "zhisk_rows": zhisk_rows,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "create_by": create_by
    }



def build_chunk_doc(
    zhisk_file_id: str,
    content: str,
    uu_id: str,
    create_by: str = None
) -> dict:
    """
    构造段落写入 ES 的文档结构（符合 zhisk_results mapping）

    :param zhisk_file_id: 所属文件 ID
    :param content: 段落文本
    :param uu_id: 段落唯一 ID（由外部生成）
    :param create_by: 创建人 ID，可选
    :return: 用于 ES 写入的文档 dict
    """
    doc = {
        "zhisk_file_id": zhisk_file_id,
        "uu_id": uu_id,
        "content": content,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    if create_by:
        doc["create_by"] = create_by
    return doc


def build_question_vector_doc(
    ori_sent_id: str,
    ori_ques_sent: str,
    vector: list
) -> dict:
    """
    构造符合 wmx_ques mapping 的 ES 问题向量文档结构

    :param ori_sent_id: 原始段落 ID，用于反查
    :param ori_ques_sent: 原始生成的问题文本
    :param vector: 问题向量（长度应为 1024）
    :return: 可用于 insert_question_vector_to_es 的 dict
    """
    return {
        "id": str(uuid.uuid4()),
        "ori_sent_id": ori_sent_id,
        "ori_ques_sent": ori_ques_sent,
        "ques_vector": vector
    }