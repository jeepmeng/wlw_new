from fastapi import FastAPI, Request, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI
import asyncio
import logging
import threading
import json
from sentence_transformers import SentenceTransformer
from history import his_talk
from pg_interact import read_yaml, connect_db
from talk_insert_pg import insert_talk_vectors_to_db
from dialog_service.vector_engine import submit_vector_task_sync, get_vector_result_by_task, format_results_into_prompt
from sqlalchemy.ext.asyncio import AsyncSession
from db_service.session import get_async_db

# 初始化
app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 日志配置
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [Thread: %(threadName)s] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 加载配置
yaml_file = "config.yaml"
db_params = read_yaml(yaml_file)
chat_sessions = {}

# 请求结构
class ChatRequest(BaseModel):
    session_id: str
    file_text: str
    file_userId: int
    file_talkId: int

class ControlRequest(BaseModel):
    session_id: str
    action: str  # "pause", "resume", "stop"

# 多轮对话主接口
@app.post("/roundTalk")
async def multiple_rounds_chat(request_data: ChatRequest, db: AsyncSession = Depends(get_async_db)):
    client = AsyncOpenAI(api_key="sk-3cd962207189494f872bdec8394a1ffe", base_url="https://api.deepseek.com")
    file_text = request_data.file_text
    file_userId = request_data.file_userId
    file_talkId = request_data.file_talkId
    session_id = request_data.session_id

    logger.info(f"处理请求的线程 ID: {threading.get_ident()}, 线程名称: {threading.current_thread().name}")

    try:
        res = his_talk(file_userId, file_talkId, connect_db, db_params)
        messages = list(res)

        # 提交异步向量计算任务并轮询获取结果
        task_id = submit_vector_task_sync(file_text)
        vector_results = None
        for _ in range(20):
            vector_results = await get_vector_result_by_task(task_id, db)
            if vector_results:
                break
            await asyncio.sleep(0.5)

        zhishi_prompt = format_results_into_prompt(vector_results)

        new_prompt = (
            f"{zhishi_prompt}以上内容为本轮问题知识库相关内容，重点强调**如果内容包含：//标准模版 ,直接将内容格式化输出给用户(格式要编排一下)，不要做任何改动。如果内容没有：//标准模版，可作为参考，如果与问题无关请忽略，并正常作答。上述提示词不要在思考阶段对外展示。\n\n本轮的用户提问是：{file_text}"
        )
        messages.append({"role": "user", "content": new_prompt})

        async def generate_response(file_userId: int, file_talkId: int, session_id: str, prompt: str):
            chat_sessions[session_id] = {"status": "running"}
            reasoning = 0
            full_response = ""

            try:
                response = await client.chat.completions.create(
                    model="deepseek-reasoner",
                    messages=prompt,
                    stream=True
                )
                async for chunk in response:
                    if chat_sessions[session_id]["status"] == "stopped":
                        await client.aclose()
                        break
                    while chat_sessions[session_id]["status"] == "paused":
                        await asyncio.sleep(0.1)
                    if chunk.choices[0].delta.reasoning_content:
                        if reasoning == 0:
                            yield "🤔正在思考... ...  \n"
                            full_response += "🤔正在思考... ...  \n"
                            reasoning = 1
                        content = chunk.choices[0].delta.reasoning_content
                        full_response += content
                        yield content
                    if chunk.choices[0].delta.content:
                        if reasoning in [0, 1]:
                            yield "  \n  \n😶 \n💬开始回答:  \n"
                            full_response += "  \n  \n😶 \n💬开始回答:  \n"
                            reasoning = 2
                        content = chunk.choices[0].delta.content
                        full_response += content
                        yield content
            except Exception as e:
                yield f"data: {str(e)}\n\n"
            finally:
                await client.aclose()

            index = prompt[-1]['content'].find("\n本轮的用户提问是：")
            result = prompt[-1]['content'][index + len("\n本轮的用户提问是："):].strip() if index != -1 else ""
            insert_talk_vectors_to_db(file_talkId, file_userId, result, full_response, connect_db, db_params)

        return StreamingResponse(generate_response(file_userId, file_talkId, session_id, messages), media_type='text/plain; charset=utf-8')

    except Exception as e:
        return JSONResponse(status_code=500, content={"msg": str(e), "code": 500, "data": "操作失败"})

# 控制接口
@app.post("/control")
async def control_chat(request_data: ControlRequest):
    session_id = request_data.session_id
    action = request_data.action

    if session_id not in chat_sessions:
        return JSONResponse(content={"error": "Session not found"}, status_code=404)

    if action in ["pause", "resume", "stop"]:
        chat_sessions[session_id]["status"] = "paused" if action == "pause" else "running" if action == "resume" else "stopped"
    else:
        return JSONResponse(content={"error": "Invalid action"}, status_code=400)

    return JSONResponse(content={"message": f"Session {session_id} {action}d successfully."})

# 本地调试启动
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("stm_r_talk_asyn_xyjx:app", host="0.0.0.0", port=8058, reload=True)
