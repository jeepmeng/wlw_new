# wlw_new

启动命令：

`celery -A task.celery_app worker --loglevel=info --concurrency=4  `

`uvicorn app:app --reload --host 0.0.0.0 --port 8000`

`celery -A task.celery_app flower`
