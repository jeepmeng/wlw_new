from task.celery_app import celery_app

@celery_app.task(name="wrap.vector")
def wrap_vector_as_list(vec):
    return [vec]