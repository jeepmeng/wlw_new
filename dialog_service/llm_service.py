# llm_service.py
from openai import OpenAI
from openai import AsyncOpenAI
from typing import Generator
from typing import AsyncGenerator
from config.settings import settings
import asyncio

# client = OpenAI(
#     api_key=settings.deepseek.api_key,
#     base_url=settings.deepseek.base_url
# )
client = AsyncOpenAI(
    api_key=settings.deepseek.api_key,
    base_url=settings.deepseek.base_url
)


async def call_llm(prompt: str) -> AsyncGenerator[str, None]:
        messages = [{"role": "user", "content": prompt}]
        response = await client.chat.completions.create(
            model="deepseek-reasoner",
            messages=messages,
            stream=True
        )
        async for chunk in response:
            delta = chunk.choices[0].delta
            if delta.reasoning_content:
                yield delta.reasoning_content
            elif delta.content:
                yield delta.content

    # loop = asyncio.get_event_loop()
    # for chunk in await loop.run_in_executor(None, lambda: list(generator())):
    #     yield chunk