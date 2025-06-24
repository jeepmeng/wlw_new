# task/file_parse_pipeline.py
from task.celery_app import celery_app
from task.splitter_loader import LOADER_MAP, SPLITTER_MAP
from task.gen_ques import generate_questions_task
from task.gen_vector_chain import encode_text_task
from task.db_interact import insert_ques_batch_task
from celery import chain
from utils.logger_manager import get_logger

logger = get_logger("task_parse_file")

@celery_app.task(
    name="parse.file",
    bind=True,
    max_retries=3,
    default_retry_delay=5,  # 每次重试间隔（秒）
    autoretry_for=(Exception,)
)
def parse_file_and_enqueue_chunks(self, file_path: str, ext: str, file_id: str):
    try:
        if ext not in LOADER_MAP or ext not in SPLITTER_MAP:
            msg = f"暂不支持的文件类型: .{ext}"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}

        loader = LOADER_MAP[ext]
        splitter = SPLITTER_MAP[ext]

        docs = loader(file_path)
        if not docs or all(not d.page_content.strip() for d in docs):
            msg = "文档内容为空，无法处理"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}

        chunks = splitter(docs)
        if not chunks:
            msg = "文档分段为空，跳过处理"
            logger.warning(f"[{file_id}] {msg}")
            return {"status": "skipped", "reason": msg}

        for idx, chunk in enumerate(chunks):
            process_single_chunk_batch.delay(chunk.page_content, file_id, idx)

        logger.info(f"[{file_id}] 成功提交 {len(chunks)} 个子任务")
        return {"status": "ok", "chunks": len(chunks)}

    except Exception as e:
        logger.exception(f"[{file_id}] 文档处理异常: {e}")
        raise self.retry(exc=e)  # 显式触发重试

@celery_app.task(name="process.chunk.batch")
def process_single_chunk_batch(text: str, file_id: str, chunk_index: int):
    try:
        questions = generate_questions_task(text)
        if not questions:
            return "无生成问题，跳过该段"

        uu_id_base = f"{file_id}_{chunk_index}"
        task_chain = chain(
            encode_questions_and_store.s(questions, uu_id_base)
        )
        task_chain.apply_async()
    except Exception as e:
        return {"error": str(e)}

@celery_app.task(name="encode_and_insert")
def encode_questions_and_store(questions: list, uu_id: str):
    try:
        sentences = []
        vectors = []
        for question in questions:
            vec = encode_text_task.apply(args=[question]).get()
            sentences.append(question)
            vectors.append(vec)

        insert_ques_batch_task.delay(uu_id=uu_id, sentences=sentences, vectors=vectors)
    except Exception as e:
        return {"error": str(e)}
