from minio import Minio
from minio.error import S3Error
from datetime import timedelta
import base64
import json

# ✅ MinIO连接配置
minio_client = Minio(
    endpoint="localhost:9000",         # 替换为你的 MinIO 服务地址
    access_key="admin",               # 替换为你的 Access Key
    secret_key="jeepmeng2",           # 替换为你的 Secret Key
    secure=False                      # 如果是 https 则设为 True
)

bucket_name = "wlw-test"

def generate_file_list(user_id="123"):
    files = []
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        for idx, obj in enumerate(objects, start=1):
            filename = obj.object_name

            # ✅ 生成 presigned 下载链接（有效期12小时）
            presigned_url = minio_client.presigned_get_object(
                bucket_name, filename, expires=timedelta(hours=12)
            )

            files.append({
                "file_id": f"file-test-{idx:04d}",
                "filename": filename,
                "url": presigned_url,
                "user_id": user_id    # ✅ 增加 user_id 字段
            })

    except S3Error as e:
        print(f"MinIO 发生错误: {e}")

    return {"files": files}


if __name__ == "__main__":
    result = generate_file_list(user_id="123")
    print(json.dumps(result, indent=2, ensure_ascii=False))