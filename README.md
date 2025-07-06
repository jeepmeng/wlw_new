# wlw_new

启动命令：

celery -A task.celery_app worker --loglevel=info --concurrency=4

uvicorn app:app --reload --host 0.0.0.0 --port 8000

celery -A task.celery_app flower

导入镜像: docker load -i Downloads/iot-images-61.tar

sql文件拷贝至docker中: docker cp Downloads/all.sql  wlw-mac-mini:/tmp/all.sql

容器中导入sql: psql -U root -f /tmp/all.sql

