from task.celery_app import celery_app  # ✅ 明确导入你的 Celery 实例
from splitter_loader import LOADER_MAP, SPLITTER_MAP
from task.gen_ques import generate_5_questions
from task.gen_vector import generate_embedding
from task.db_write import insert_vectors_to_db  # 你要新建的异步入库任务模块

@celery_app.task
def parse_file_and_enqueue_chunks(file_path: str, ext: str, file_id: str):
    try:
        loader = LOADER_MAP[ext]
        splitter = SPLITTER_MAP[ext]

        docs = loader(file_path)
        chunks = splitter(docs)

        for idx, chunk in enumerate(chunks):
            process_single_chunk.delay(chunk.page_content, file_id, idx)

        return f"{file_id} 分段任务已提交，共 {len(chunks)} 段"
    except Exception as e:
        return {"error": str(e)}

@celery_app.task
def process_single_chunk(text: str, file_id: str, chunk_index: int):
    try:
        questions = generate_5_questions(text)
        for i, desc in enumerate(questions):
            vector = generate_embedding(desc)
            insert_vectors_to_db.delay(
                zhisk_file_id=file_id,
                content=text,
                vector=vector,
                jsons={"desc": desc},
                code=f"{chunk_index}-{i}",
                category="auto",
                uu_id=f"{file_id}_{chunk_index}_{i}"
            )
    except Exception as e:
        return {"error": str(e)}