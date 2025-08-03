from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
import json

# 直接在这里配置参数
ES_HOST = "http://172.16.19.242:9200"
CONFIG_FILE = "indices_config.json"  # 索引配置 JSON 文件路径
DELETE_EXISTING = False    # 是否删除已存在的索引

def create_indices(es_client, index_configs, delete_existing=False):
    """
    批量创建 Elasticsearch 索引
    """
    results = {}
    
    for index_name, config in index_configs.items():
        try:
            # 检查索引是否存在
            if es_client.indices.exists(index=index_name):
                if delete_existing:
                    es_client.indices.delete(index=index_name)
                else:
                    results[index_name] = {"status": "skipped", "message": "Index already exists"}
                    continue
            
            # 创建索引
            body = {}
            if "mappings" in config:
                body["mappings"] = config["mappings"]      
            response = es_client.indices.create(index=index_name, body=body)
            results[index_name] = {"status": "success", "response": response}
            
        except RequestError as e:
            error_info = json.loads(e.info)
            results[index_name] = {
                "status": "error",
                "error": error_info.get("error", {}).get("reason", str(e)),
                "type": error_info.get("error", {}).get("type", "unknown_error")
            }
        except Exception as e:
            results[index_name] = {"status": "error", "error": str(e)}
    
    return results

def load_config_from_file(file_path):
    """从 JSON 文件加载索引配置"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    # 初始化 ES 客户端
    es = Elasticsearch(ES_HOST, verify_certs=False)
    if not es.ping():
        raise ConnectionError("无法连接到Elasticsearch")
    # 加载配置
    try:
        index_configs = load_config_from_file(CONFIG_FILE)
    except Exception as e:
        print(f"加载配置文件失败: {str(e)}")
        return
    # 创建索引
    results = create_indices(es, index_configs, DELETE_EXISTING)

    # 打印结果
    print("\n操作结果:")
    for index, result in results.items():
        status = result["status"]
        if status == "success":
            print(f"[✓] {index}: 创建成功")
        elif status == "skipped":
            print(f"[ ] {index}: 已跳过 (索引已存在)")
        else:
            print(f"[×] {index}: 失败 - {result.get('error', '未知错误')}")

if __name__ == "__main__":
    main()