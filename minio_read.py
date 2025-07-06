from minio import Minio
from minio.error import S3Error
from datetime import timedelta
import json

# ✅ MinIO 配置
minio_client = Minio(
    endpoint="127.0.0.1:9000",        # 修改为你的 MinIO 地址
    access_key="admin",              # 修改为你的 access_key
    secret_key="jeepmeng2",           # 修改为你的 secret_key
    secure=False                     # http 用 False，https 用 True
)

# ✅ Bucket 和子路径配置
bucket_name = "wlw"                  # ✅ 替换为你的 bucket 名
prefix = ""                          # ✅ 如果 bucket 下面有子目录如 "docs/"，这里填；没有就留空
expiry_seconds = timedelta(hours=24)  # ✅ 有效期：24 小时

# ✅ 输出文件路径
output_json_path = "minio_signed_urls.json"

# ✅ 开始处理
files = []
file_idx = 1

try:
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        if obj.is_dir:
            continue  # 跳过文件夹

        filename = obj.object_name.split("/")[-1]  # 取最后的文件名
        signed_url = minio_client.presigned_get_object(
            bucket_name=bucket_name,
            object_name=obj.object_name,
            expires=expiry_seconds
        )
        files.append({
            "file_id": f"file-test-{file_idx:04d}",
            "filename": filename,
            "url": signed_url
        })
        file_idx += 1

except S3Error as err:
    print(f"❌ MinIO 错误: {err}")
    exit(1)

# ✅ 写入 JSON 文件
with open(output_json_path, "w", encoding="utf-8") as f:
    json.dump({"files": files}, f, ensure_ascii=False, indent=2)

print(f"✅ 成功写入 {len(files)} 个文件，输出路径: {output_json_path}")