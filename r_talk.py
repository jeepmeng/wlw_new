from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import asyncio
import time
import json
import re
from sentence_transformers import SentenceTransformer
from search import query_similar_sentences
from history import his_talk
from pg_interact import insert_vectors_to_db, read_yaml, insert_ques_batch, update_by_id, connect_db
from talk_insert_pg import insert_talk_vectors_to_db
from collections import defaultdict
from fastapi.concurrency import run_in_threadpool
from openai import AsyncOpenAI
import logging
import threading

yaml_file = "config.yaml.bak"  # 指定 YAML 文件路径
db_params = read_yaml(yaml_file)
# 配置日志格式，包含线程 ID
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [Thread: %(threadName)s] - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

app = FastAPI()

# 允许所有域跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OpenAI DeepSeek API 配置


ali_client = OpenAI(
    api_key="sk-e8d4973beecd4a43bdce4718b0b2444c",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)
model = SentenceTransformer('/data/xht_test_code/FlagEmbedding-master/BAAI/bge-large-zh-v1.5', device="cpu")

# 用于存储当前会话的状态
chat_sessions = {}


class ChatRequest(BaseModel):
    session_id: str
    file_text: str
    file_userId: int
    file_talkId: int


class ControlRequest(BaseModel):
    session_id: str
    action: str  # "pause", "resume", "stop"


# 记录是否是第一次输出


@app.post("/roundTalk")
async def multiple_rounds_chat(request_data: ChatRequest):
    client = AsyncOpenAI(api_key="sk-3cd962207189494f872bdec8394a1ffe", base_url="https://api.deepseek.com")
    print(request_data)
    result_data = []
    file_text = request_data.file_text
    file_userId = request_data.file_userId
    file_talkId = request_data.file_talkId
    session_id = request_data.session_id
    print(f'file_text:   {file_text}\n',
          f'file_userId:   {file_userId}',
          f'file_talkId:   {file_talkId}',
          f'session_id:   {session_id}', )

    thread_id = threading.get_ident()
    print(f'处理请求的线程:{thread_id}')
    thread_name = threading.current_thread().name
    logger.info(f"处理请求的线程 ID: {thread_id}, 线程名称: {thread_name}")

    try:
        # 生成文本向量
        vector = model.encode([file_text], normalize_embeddings=True).tolist()[0]
        # print(vector)
        # 执行数据库查询
        ques_res = query_similar_sentences(vector, connect_db, db_params)
        # print('ques_res is done')
        res = his_talk(file_userId, file_talkId, connect_db, db_params)

        # 调用阿里云API生成关键词
        prompt = "根据上传内容进行关键词提取，只返回提取后的关键词，用逗号分隔，不要其他文字"
        completion = ali_client.chat.completions.create(model="qwen-long", messages=[
            {'role': 'system', 'content': 'You are a helpful assistant.'}, {'role': 'system', 'content': file_text},
            {'role': 'user', 'content': prompt}], stream=True, stream_options={"include_usage": True})
        # 处理流式响应
        full_content = ""
        for chunk in completion:
            if chunk.choices and chunk.choices[0].delta.content:
                full_content += chunk.choices[0].delta.content
        # 处理关键词查询
        # print("33333")
        a = [i.strip() for i in full_content.split(",") if i.strip()]
        new_a = []
        for i in a:
            encoded_i = model.encode([i], normalize_embeddings=True).tolist()[0]
            new_a.append(query_similar_sentences(encoded_i, connect_db, db_params))
        # 构建提示词逻辑
        new_list = new_a[0] if len(new_a) > 1 else []
        messages = list(res)

        # 无相关结果时的处理逻辑
        zhishi_prompt = "".join(new_list)
        wenti_prompt = "".join([i for i in ques_res]) if len(ques_res) > 1 else str(ques_res)
        # print("4-4-4-4-")
        print(f"wenti_prompt----------:{wenti_prompt}")
        new_prompt = (
                f"{zhishi_prompt} \n{wenti_prompt}  \n  \n  以上内容是物模型的规范文档，生成的结果不要带注释，不要缺少规范中的各种变量、参数、定义，每次返回的物模型json一定要是全部的物模型结果，切不能够带有注释。  \n  \n" + f"\n以下是本轮问题，如果与知识库相关，请结合知识库作答，如果与知识库无关，请直接作答，当前用户提问是：{file_text}")
        # print("4--4--4--4")
        print(f"new_prompt----------:{new_prompt}")
        messages.append({"role": "user", "content": new_prompt})
        # print(messages)
        # response_store = {}  # 用于存储完整的 clean_full_response






    #     async def generate_response(file_userId: int, file_talkId: int, session_id: str, prompt: str):
    #         # full_response = ""  # 用于存储完整响应
    #         chat_sessions[session_id] = {"status": "running"}  # 初始状态
    #         reasoning = 0
    #         full_response = []  # 保存完整的 AI 响应内容
    #
    #         # **每次调用时，重置标记**
    #         first_reasoning_flag = [True]
    #         first_content_flag = [True]
    #         print('开始连接deepseek')
    #
    #         try:
    #             response = await client.chat.completions.create(
    #                 model="deepseek-reasoner",
    #                 # messages=[{"role": "user", "content": prompt}],
    #                 messages = prompt,
    #                 stream=True
    #             )
    #             print('结束连接deepseek')
    #              # ✅ 直接遍历 `response`，因为它是一个流对象
    #
    #             async for chunk in response:
    #
    #                 if chat_sessions[session_id]["status"] == "stopped":
    #                     print(f"Session {session_id} 被终止")
    #                     client.close()
    #                     break
    #                 while chat_sessions[session_id]["status"] == "paused":
    #                     print(f"Session {session_id} 暂停中...")
    #                     await asyncio.sleep(0.1)
    #                 delta = chunk.choices[0].delta
    #                 if delta.reasoning_content is not None:
    #                     if reasoning == 0:
    #                         yield f"🤔正在思考... ...  \n"
    #                         full_response += "🤔正在思考... ...  \n"
    #                         reasoning = 1
    #                     # content = chunk.choices[0].delta.reasoning_content
    #                     full_response.append(delta.reasoning_content)
    #                     yield f"{delta.reasoning_content}"
    #                     # await asyncio.sleep(0.1)
    #
    #                 if delta.content is not None:
    #                     if reasoning == 1 or 0:
    #                         yield f"  \n  \n😶 \n💬开始回答:  \n"
    #                         reasoning = 2
    #                         full_response += "  \n  \n😶 \n💬开始回答:  \n"
    #
    #                     # content = chunk.choices[0].delta.content
    #                     # print(content)
    #                     full_response.append(delta.content)
    #                     yield f"{delta.content}"
    #                     # await asyncio.sleep(0.1)
    #
    #         except Exception as e:
    #             print('error')
    #             yield f"data: {str(e)}\n\n"
    #         finally:
    #             await client.close()  # 确保关闭客户端连接
    #
    #         print('开始入库')
    #         # response_store[session_id] = full_response
    #         # print(f'response_store is {response_store[session_id]}')
    #         keyword = "\n本轮的用户提问是："  # 指定字段
    #         index = prompt[-1]['content'].find(keyword)
    #
    #         if index != -1:  # 找到关键字
    #             result = prompt[-1]['content'][index + len(keyword):].strip()  # 提取后面的内容
    #         else:
    #             result = ""  # 关键字不存在
    #         # full =
    #         print(''.join(full_response))
    #         ## 插入对话记录到数据库
    #         insert_talk_vectors_to_db(file_talkId,file_userId,result,''.join(full_response),connect_db,db_params)
    #         print('结束入库')
    #
    #
    #     return StreamingResponse(generate_response(file_userId, file_talkId, session_id,messages), media_type='text/plain; charset=utf-8')
    #
    # except Exception as e:
    #     return JSONResponse(status_code=500, content={"msg": str(e), "code": 500, "data": "操作失败"})
    # finally :
    #     pass

    except Exception as e:
        pass
    return messages


@app.post("/control")
async def control_chat(request_data: ControlRequest):
    session_id = request_data.session_id
    action = request_data.action

    if session_id not in chat_sessions:
        return JSONResponse(content={"error": "Session not found"}, status_code=404)

    if action == "pause":
        chat_sessions[session_id]["status"] = "paused"
    elif action == "resume":
        chat_sessions[session_id]["status"] = "running"
    elif action == "stop":
        chat_sessions[session_id]["status"] = "stopped"
    else:
        return JSONResponse(content={"error": "Invalid action"}, status_code=400)

    return JSONResponse(content={"message": f"Session {session_id} {action}d successfully."})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("stm_r_talk_api_wmx:app", host="0.0.0.0", port=8053, reload=True)
