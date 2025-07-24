from elasticsearch import Elasticsearch, helpers
from psycopg2 import connect
import logging
import json
import ast

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_index(es, index_name, index_mapping):
    """创建Elasticsearch索引（如果不存在）"""
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_mapping)
        logger.info(f"索引 {index_name} 创建成功")
    else:
        logger.warning(f"索引 {index_name} 已存在，跳过创建")

def parse_vector_field(value):
    """解析向量字段，支持字符串形式的数组或PostgreSQL数组"""
    if isinstance(value, str):
        try:
            # 尝试解析JSON格式的字符串
            return json.loads(value)
        except json.JSONDecodeError:
            try:
                # 尝试解析Python字面量格式的字符串
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                logger.error(f"无法解析向量字段: {value}")
                return None
    elif isinstance(value, (list, tuple)):
        return list(value)
    return None

def generate_docs(cursor, field_names, index_name, id_field=None, vector_fields=None):
    """
    生成Elasticsearch文档
    :param cursor: 数据库游标
    :param field_names: 查询返回的字段名列表（按查询顺序）
    :param index_name: ES索引名称
    :param id_field: 作为文档ID的字段名（可选）
    :param vector_fields: 需要特殊处理的向量字段名列表（可选）
    """
    if vector_fields is None:
        vector_fields = []
    
    for row in cursor:
        try:
            # 构建文档
            doc_source = {}
            for field, value in zip(field_names, row):
                if field in vector_fields:
                    # 特殊处理向量字段
                    parsed_vector = parse_vector_field(value)
                    if parsed_vector is not None:
                        doc_source[field] = parsed_vector
                    else:
                        logger.warning(f"向量字段 {field} 解析失败，跳过此字段")
                else:
                    doc_source[field] = value
            
            doc = {
                "_index": index_name,
                "_source": doc_source
            }
            
            # 如果指定了ID字段
            if id_field and id_field in doc["_source"]:
                doc["_id"] = doc["_source"][id_field]
                
            yield doc
            
        except Exception as e:
            logger.error(f"处理行失败: {row} | 错误: {str(e)}")

def import_table_to_es(pg_config, es_host, index_name, index_mapping, sql_query, id_field=None, vector_fields=None):
    """
    将PostgreSQL表导入Elasticsearch
    :param pg_config: PostgreSQL连接配置
    :param es_host: Elasticsearch主机地址
    :param index_name: ES索引名称
    :param index_mapping: ES索引映射配置
    :param sql_query: SQL查询语句
    :param id_field: 作为文档ID的字段名（可选）
    :param vector_fields: 需要特殊处理的向量字段名列表（可选）
    """
    # 连接Elasticsearch
    es = Elasticsearch(es_host, verify_certs=False)
    if not es.ping():
        raise ConnectionError("无法连接到Elasticsearch")
    
    create_index(es, index_name, index_mapping)
    
    conn = None
    cursor = None
    try:
        # 连接PostgreSQL
        conn = connect(**pg_config)
        cursor = conn.cursor()

        cursor.execute(sql_query)
        
        # 获取查询字段名
        field_names = [desc[0] for desc in cursor.description]
        logger.info(f"查询字段: {field_names}")
        
        # 批量导入ES
        docs_generator = generate_docs(cursor, field_names, index_name, id_field, vector_fields)
        success, errors = helpers.bulk(es, docs_generator, stats_only=False)
        
        logger.info(f"导入完成! 成功: {success}, 失败: {len(errors)}")
        for i, error in enumerate(errors[:3]):  # 最多打印前3个错误
            logger.error(f"错误 #{i+1}: {error}")
            
    except Exception as e:
        logger.exception("导入过程中发生严重错误")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# 使用示例
if __name__ == "__main__":
    # PostgreSQL配置
    PG_CONFIG = {
        "dbname": "wmx",
        "user": "root",
        "password": "zeus@CC1234!",
        "host": "172.16.19.61",
        "port": 5432
    }
    
    # Elasticsearch主机
    ES_HOST = "http://172.16.19.242:9200"
    
    # 索引名称
    INDEX_NAME = "m_talk_record"  # 替换为实际的索引名称
    
    # 索引映射
    # INDEX_MAPPING = {
    #     "mappings": {
    #         "properties": {
    #             "id": {"type": "long"},
    #             "ori_sent_id": {"type": "keyword"},
    #             "ori_ques_sent": {
    #                 "type": "text",
    #                 "fields": {
    #                     "keyword": {
    #                         "type": "keyword",
    #                         "ignore_above": 256
    #                     }
    #                 }
    #             },
    #             "ques_vector": {
    #                 "type": "dense_vector",
    #                 "dims": 1024,
    #                 "index": True,
    #                 "similarity": "cosine"
    #             }
    #         }
    #     }
    # }
    # INDEX_MAPPING = {
    #     "mappings": {
    #         "properties": {
    #             "id": {"type": "long"},
    #             "zhisk_file_id": {"type": "long"},
    #             "content": {
    #                 "type": "text",
    #                 "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
    #             },
    #             "create_time": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
    #             "category": {"type": "keyword"},
    #             "uu_id": {"type": "keyword"}
    #         }
    #     }
    # }
    INDEX_MAPPING = {
  "mappings": {
    "properties": {
      "id": {
        "type": "long"
      },
      "talk_id": {
        "type": "long"
      },
      "input_content": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "output_content": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "create_time": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      "create_by": {
        "type": "long"
      }
    }
  }
}
    # SQL查询
    # SQL_QUERY = """
    #     SELECT 
    #         id,          
    #         ori_sent_id,       
    #         ori_ques_sent,      
    #         ques_vector              
    #     FROM wmx_ques
    # """
    # SQL_QUERY = """
    # SELECT 
    #     id, 
    #     zhisk_file_id,
    #     content,
    #     create_time,
    #     category,
    #     uu_id
    # FROM m_zhisk_results
    # """    
    SQL_QUERY = """
    SELECT 
        id, 
        talk_id,
        input_content,
        output_content,
        create_time
    FROM m_talk_record
    """    
    # 执行导入
    # import_table_to_es(
    #     pg_config=PG_CONFIG,
    #     es_host=ES_HOST,
    #     index_name=INDEX_NAME,
    #     index_mapping=INDEX_MAPPING,
    #     sql_query=SQL_QUERY,
    #     id_field="id",  # 使用id字段作为ES文档ID
    #     vector_fields=["ques_vector"]  # 指定需要特殊处理的向量字段
    # )

    import_table_to_es(
        pg_config=PG_CONFIG,
        es_host=ES_HOST,
        index_name=INDEX_NAME,
        index_mapping=INDEX_MAPPING,
        sql_query=SQL_QUERY,
        id_field="id" # 使用id字段作为ES文档ID
    )