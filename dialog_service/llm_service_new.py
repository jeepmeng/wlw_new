from typing import AsyncGenerator, Optional, Dict, Any
import asyncio
from contextlib import suppress
from dialog_service.llm_service import call_llm as deepseek_call_llm
from config.settings import settings


# ===== Qwen 同步流式 → 异步封装（支持 prompt / messages）=====
def _qwen_sync_iter(
    *,
    model: str,
    api_key: str,
    prompt: Optional[str] = None,
    messages: Optional[list] = None,
    extra: Optional[Dict[str, Any]] = None
):
    """dashscope 同步流式生成器，在线程中跑。支持 prompt 或 messages。"""
    from dashscope import Generation

    if not (prompt or messages):
        raise ValueError("必须提供 prompt 或 messages 之一")

    # 构建 extra_body / parameters，尽量双保险兼容
    extra_body = dict(extra or {})
    parameters = dict(extra or {})

    # 默认配置：强制联网 + 返回引用 + message 格式
    extra_body.setdefault("enable_search", True)
    extra_body.setdefault("search_options", {
        "forced_search": True,
        "enable_source": True,
        "citation_format": "[ref_<number>]",
        "search_strategy": "turbo",
    })
    extra_body.setdefault("result_format", "message")

    parameters.setdefault("enable_search", extra_body["enable_search"])
    parameters.setdefault("search_options", extra_body["search_options"])
    parameters.setdefault("result_format", extra_body["result_format"])

    # 组装调用参数
    call_kwargs = {
        "api_key": api_key,
        "model": model,
        "stream": True,
        "extra_body": extra_body,   # ✅ Python SDK 推荐
        "parameters": parameters,   # ✅ 兼容一些旧示例
    }
    if messages is not None:
        call_kwargs["messages"] = messages
    else:
        call_kwargs["prompt"] = prompt

    responses = Generation.call(**call_kwargs)

    acc = ""  # 处理偶发的全量 text 去重
    for event in responses:
        out = getattr(event, "output", {}) or {}

        # 1) 增量（最常见）
        delta = out.get("text_delta")
        if delta:
            acc += delta
            yield delta
            continue

        # 2) Chat 风格 choices[].delta.content / text / message?.content
        choices = out.get("choices") or []
        if choices:
            d = (choices[0].get("delta") or {})
            # 新旧两种字段都试一下
            chunk = d.get("content") or d.get("text")
            if not chunk:
                # 有些版本把内容包在 message 里
                msg = d.get("message") or {}
                chunk = msg.get("content")
            if chunk:
                acc += chunk
                yield chunk
                continue

        # 3) 偶发全量 text，做去重截尾
        full = out.get("text")
        if full:
            if len(full) > len(acc):
                tail = full[len(acc):]
                acc = full
                if tail:
                    yield tail
            continue


async def _qwen_stream_async(
    *,
    model: str,
    api_key: str,
    prompt: Optional[str] = None,
    messages: Optional[list] = None,
    extra: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[str, None]:
    """
    把 dashscope 的同步 stream 包装成真正的 async 流。
    """
    import asyncio
    from contextlib import suppress

    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    DONE = object()

    def worker():
        try:
            for piece in _qwen_sync_iter(
                model=model,
                api_key=api_key,
                prompt=prompt,
                messages=messages,
                extra=extra
            ):
                placed = False
                while not placed:
                    try:
                        queue.put_nowait(piece)
                        placed = True
                    except asyncio.QueueFull:
                        import time as _t
                        _t.sleep(0.005)
        except BaseException as e:
            try:
                queue.put_nowait(e)
            except Exception:
                pass
        finally:
            try:
                queue.put_nowait(DONE)
            except Exception:
                pass

    task = asyncio.create_task(asyncio.to_thread(worker))
    try:
        while True:
            item = await queue.get()
            if item is DONE:
                break
            if isinstance(item, BaseException):
                raise item
            yield str(item)
    finally:
        with suppress(Exception):
            task.cancel()


# ===== 工具：非流式先取 search_info（当 with_search_info=True）=====
def _qwen_fetch_search_info(
    *,
    model: str,
    api_key: str,
    prompt: Optional[str] = None,
    messages: Optional[list] = None,
    extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """做一次非流式调用获取 search_info，失败返回空 dict。"""
    try:
        from dashscope import Generation

        extra_body = dict(extra or {})
        parameters = dict(extra or {})

        extra_body.setdefault("enable_search", True)
        extra_body.setdefault("search_options", {
            "forced_search": True,
            "enable_source": True,
            "citation_format": "[ref_<number>]",
            "search_strategy": "turbo",
        })
        extra_body.setdefault("result_format", "message")

        parameters.setdefault("enable_search", extra_body["enable_search"])
        parameters.setdefault("search_options", extra_body["search_options"])
        parameters.setdefault("result_format", extra_body["result_format"])

        call_kwargs = {
            "api_key": api_key,
            "model": model,
            "stream": False,
            "extra_body": extra_body,
            "parameters": parameters,
        }
        if messages is not None:
            call_kwargs["messages"] = messages
        else:
            call_kwargs["prompt"] = prompt

        resp = Generation.call(**call_kwargs)
        out = getattr(resp, "output", {}) or {}
        info = out.get("search_info") or {}
        # 只返回我们关心的字段，避免体积过大
        if "search_results" in info:
            return {
                "search_results": [
                    {
                        "index": it.get("index"),
                        "title": it.get("title"),
                        "url": it.get("url"),
                        "snippet": it.get("snippet"),
                        "source": it.get("source"),
                    }
                    for it in info["search_results"]
                ]
            }
        return {}
    except Exception:
        return {}


def _render_search_block(search_info: Dict[str, Any]) -> str:
    """把 search_info 渲染为一段可读文本，附在流式正文之前。"""
    results = (search_info or {}).get("search_results") or []
    if not results:
        return ""
    lines = ["\n---\n【检索来源】"]
    for it in results:
        idx = it.get("index")
        title = it.get("title") or "(无标题)"
        url = it.get("url") or ""
        lines.append(f"[ref_{idx}] {title} {url}")
    lines.append("\n")
    return "\n".join(lines)


# ===== 统一入口（支持 prompt / messages + 附带 search_info）=====
async def call_llm_stream(
    prompt: Optional[str] = None,
    *,
    messages: Optional[list] = None,          # ✅ 新增：支持多轮对话
    provider: str = "deepseek",               # "deepseek" | "qwen"
    enable_search: Optional[bool] = None,     # 仅对 qwen 有意义
    with_search_info: bool = False,           # ✅ 新增：是否先展示检索来源
    qwen_parameters: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[str, None]:
    """
    统一的流式接口：
      - DeepSeek 走你已有的 async 生成器
      - Qwen 走 dashscope（从 settings.qwen 读取 key/model）
      - Qwen 支持 prompt / messages 两种输入
      - with_search_info=True 时，会先同步取 search_info，再开始流式正文
    """
    if provider == "deepseek":
        # 兼容你原来深度求索的用法（只支持 prompt）
        async for chunk in deepseek_call_llm(prompt or ""):
            yield chunk
        return

    if provider == "qwen":
        api_key = getattr(settings.qwen, "api_key", "") or ""
        model = getattr(settings.qwen, "model", "qwen-plus")

        if not api_key:
            raise ValueError("缺少 Qwen API Key：请在 config.dev.yaml -> qwen.api_key 配置")

        extra = dict(qwen_parameters or {})
        # 顶层开关（优先于默认）
        if enable_search is not None:
            extra["enable_search"] = bool(enable_search)

        # 先拿检索来源（可选）
        if with_search_info:
            info = _qwen_fetch_search_info(
                model=model,
                api_key=api_key,
                prompt=prompt,
                messages=messages,
                extra=extra
            )
            block = _render_search_block(info)
            if block:
                yield block  # 先把来源列表吐出去

        # 再开始流式正文
        async for chunk in _qwen_stream_async(
            model=model,
            api_key=api_key,
            prompt=prompt,
            messages=messages,
            extra=extra
        ):
            yield chunk
        return

    raise ValueError(f"未知 provider: {provider}")