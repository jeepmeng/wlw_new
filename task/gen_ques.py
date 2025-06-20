# ✅ tasks/generate.py
from .celery_app import celery_app
# from celery import shared_task
from config.settings import load_config
from utils.logger_manager import get_logger
from openai import OpenAI  # Aliyun Qwen 兼容 OpenAI 接口
import re

logger = get_logger("gen_ques")
config = load_config()

# ✅ 初始化 Aliyun Qwen 客户端（OpenAI 协议）
ali_client = OpenAI(
    api_key="sk-e8d4973beecd4a43bdce4718b0b2444c",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

# ✅ 解析问题列表（按 1. 2. 切分）
def split_questions(text):
    questions = re.split(r'\d+\.\s', text)
    questions = [q.strip() for q in questions if q.strip()]
    return questions

@celery_app.task(name="generate.questions")
def generate_questions_task(text: str) -> list:
    try:
        prompt = "根据内容给我形成针对该段落内容生成五个问题返回给我，其余什么多余信息都不要"
        completion = ali_client.chat.completions.create(
            model="qwen-long",
            messages=[
                {'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': text},
                {'role': 'user', 'content': prompt}
            ],
            stream=True,
            stream_options={"include_usage": True}
        )

        full_content = ""
        for chunk in completion:
            if chunk.choices and chunk.choices[0].delta.content:
                full_content += chunk.choices[0].delta.content

        logger.info(f"大模型返回内容：{full_content}")
        return split_questions(full_content)

    except Exception as e:
        logger.exception(f"生成问题失败：{e}")
        return []
