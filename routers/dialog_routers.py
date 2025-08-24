# from fastapi import APIRouter, HTTPException
# from config.settings import settings
# from routers.schema import (
# StartDialogRequest,
# AskRequest,
# ControlRequest
# )
# from dialog_service.dialog_es import (
#     create_session_es,
#     insert_message_es,
#     get_history_by_session_es,
#     get_session_user_es
# )
# from utils.redis_client import get_redis_client
# from typing import List, Dict, Any
# import httpx
# import json
# from dialog_service.llm_service_new_bak import (StreamDeduper,
#                                                 parse_qwen_stream_chunk_once,
#                                                 call_llm_stream,
#                                                 qwen_stream_raw_events)
# # from dialog_service.llm_service import call_llm
# from fastapi.responses import StreamingResponse
# # from openai import AsyncOpenAI
# import asyncio
#
# router = APIRouter()
#
# # Redis 客户端
# ioredis = get_redis_client()
#
# # ali_client = AsyncOpenAI(
# #     api_key=settings.deepseek.api_key,
# #     base_url=settings.deepseek.base_url
# # )
#
# @router.post("/dialog/start")
# async def start_dialog(data: StartDialogRequest):
#     talk_id = await create_session_es(int(data.user_id), data.title, data.provider)
#     return {"talk_id": str(talk_id), "provider": data.provider}
#
# @router.post("/dialog/{talk_id}/ask")
# async def ask_question(talk_id: int, data: AskRequest):
#     # ✅ 检查 talk_id 是否属于 user_id
#     session_user_id = await get_session_user_es(talk_id)
#     if not session_user_id or str(session_user_id) != data.user_id:
#         raise HTTPException(status_code=403, detail="无权限访问该对话")
#
#     # 校验 provider（防止前端传错）
#     allowed = {"deepseek", "qwen"}
#     provider = (data.provider or "deepseek").lower()
#     if provider not in allowed:
#         raise HTTPException(status_code=400, detail=f"provider 必须是 {allowed}")
#
#     # ✅ 查询历史对话（优先 Redis）
#     # redis_key = f"dialog:history:{data.user_id}:{talk_id}"
#     # history = await ioredis.get(redis_key)
#     # if history and all(is_prompt_format(h) for h in history):
#     #     # 缓存是新格式，直接用
#     #     pass
#     # else:
#     #     history = await get_history_by_session_es(talk_id)
#     #     await ioredis.set(redis_key, str(history), ex=3600)
#
#     redis_key = f"dialog:history:{data.user_id}:{talk_id}"
#     history_raw = await ioredis.get(redis_key)
#     if history_raw:
#         # ioredis 通常返回 bytes
#         if isinstance(history_raw, (bytes, bytearray)):
#             history_raw = history_raw.decode("utf-8", errors="ignore")
#         try:
#             history = json.loads(history_raw)
#         except Exception:
#             history = await get_history_by_session_es(talk_id)
#             await ioredis.set(redis_key, json.dumps(history, ensure_ascii=False), ex=3600)
#     else:
#         history = await get_history_by_session_es(talk_id)
#         await ioredis.set(redis_key, json.dumps(history, ensure_ascii=False), ex=3600)
#
#
#
#     # ✅ 调用混合检索
#     retrieved = await es_hybrid_search(data.question)
#
#     # ✅ 构造 prompt 调用大模型
#     prompt = build_prompt(history, retrieved, data.question)
#     messages = build_messages(history, retrieved, data.question)
#
#
#
#     async def sse_stream_generator():
#         # 起始帧
#         yield "event: start\ndata: {}\n\n"
#
#         response_text = ""
#         sent_sources = False
#
#         try:
#             # ====== 调试：告诉前端当前 provider / model ======
#             model_name = getattr(settings.qwen, "model", "qwen-plus") if provider == "qwen" else "<n/a>"
#             yield f"event: debug\ndata: {json.dumps({'provider': provider, 'model': model_name}, ensure_ascii=False)}\n\n"
#
#             if provider == "qwen":
#                 api_key = getattr(settings.qwen, "api_key", "")
#                 model_name = (getattr(data, "model", None) or getattr(settings.qwen, "model", "qwen-plus"))
#                 if not api_key:
#                     yield f"event: error\ndata: {json.dumps({'reason': 'missing_qwen_api_key'}, ensure_ascii=False)}\n\n"
#                     yield "event: end\ndata: {}\n\n"
#                     return
#
#                 # 👇 显式开关：前端没传就默认 False（提速）
#                 enable_search = bool(getattr(data, "enable_search", False))
#
#                 # 👇 最小必要参数（需要来源时再把 search_options 打开）
#                 qwen_extra = {
#                     "enable_search": enable_search,
#                     "result_format": "message",
#                     "search_options": {
#                         "enable_source": True,  # 想要来源必须 True
#                         "citation_format": "[ref_<number>]"
#                         # 不强制搜索，避免无谓等待；确实需要可再加 "forced_search": True
#                     } if enable_search else None
#                 }
#
#                 # —— 去重器 —— #
#                 deduper = StreamDeduper(ngram=16, max_count=3)
#
#                 # —— 原始事件流（首帧/总时限） —— #
#                 FIRST_FRAME_TIMEOUT = 3.0  # 首帧最久 3s
#                 TOTAL_RAW_TIMEOUT = 20.0  # 原始流总时限 20s
#
#                 # 创建原始事件生成器（注意：同时传顶层 kwargs、extra_body、parameters，最大兼容）
#                 raw_iter = qwen_stream_raw_events(
#                     model=model_name,
#                     api_key=api_key,
#                     messages=messages,
#                     extra=qwen_extra  # 在实现里会同时塞到 extra_body/parameters
#                 )
#
#                 async def anext_compat(agen):
#                     return await agen.__anext__()
#
#                 start_ts = asyncio.get_event_loop().time()
#
#                 # 1) 首帧（超时兜底）
#                 try:
#                     raw = await asyncio.wait_for(anext_compat(raw_iter), timeout=FIRST_FRAME_TIMEOUT)
#                 except asyncio.TimeoutError:
#                     # ⛑ 首帧超时：切字符串流兜底（通常更快出字）
#                     yield f"event: debug\ndata: {json.dumps({'fallback': 'first_frame_timeout->string_stream'}, ensure_ascii=False)}\n\n"
#                     async for chunk in call_llm_stream(
#                             provider="qwen",
#                             messages=messages,
#                             enable_search=enable_search,
#                             qwen_parameters=qwen_extra
#                     ):
#                         if chunk:
#                             response_text += chunk
#                             yield f"data: {chunk}\n\n"
#                     # 跳过后续原始流
#                     pass
#                 else:
#                     # —— 处理首帧 —— #
#                     # 调试：第一帧原始事件（可保留 1 帧，定位字段）
#                     try:
#                         to_dict = getattr(raw, "to_dict", None)
#                         dbg = to_dict() if callable(to_dict) else str(raw)
#                         yield f"event: debug\ndata: {json.dumps({'raw_event': dbg, 'frame': 1}, ensure_ascii=False)}\n\n"
#                     except Exception:
#                         pass
#
#                     text_piece, search_info = parse_qwen_stream_chunk_once(raw)
#
#                     # 首次来源（只发一次）
#                     if (not sent_sources) and search_info:
#                         yield f"event: sources\ndata: {json.dumps(search_info, ensure_ascii=False)}\n\n"
#                         sent_sources = True
#
#                     # 正文
#                     if text_piece:
#                         tail = deduper.feed(text_piece)
#                         if tail:
#                             response_text += tail
#                             yield f"data: {tail}\n\n"
#
#                     # 2) 后续帧（带总时限）
#                     frame_idx = 1
#                     while True:
#                         # 总时限保护
#                         if asyncio.get_event_loop().time() - start_ts > TOTAL_RAW_TIMEOUT:
#                             yield f"event: debug\ndata: {json.dumps({'fallback': 'total_raw_timeout->string_stream'}, ensure_ascii=False)}\n\n"
#                             async for chunk in call_llm_stream(
#                                     provider="qwen",
#                                     messages=messages,
#                                     enable_search=enable_search,
#                                     qwen_parameters=qwen_extra
#                             ):
#                                 if chunk:
#                                     response_text += chunk
#                                     yield f"data: {chunk}\n\n"
#                             break
#
#                         try:
#                             raw = await asyncio.wait_for(anext_compat(raw_iter), timeout=5.0)
#                         except (asyncio.TimeoutError, StopAsyncIteration):
#                             break
#
#                         frame_idx += 1
#                         if frame_idx <= 3:  # 再打两帧调试
#                             try:
#                                 to_dict = getattr(raw, "to_dict", None)
#                                 dbg = to_dict() if callable(to_dict) else str(raw)
#                                 yield f"event: debug\ndata: {json.dumps({'raw_event': dbg, 'frame': frame_idx}, ensure_ascii=False)}\n\n"
#                             except Exception:
#                                 pass
#
#                         text_piece, search_info = parse_qwen_stream_chunk_once(raw)
#                         if (not sent_sources) and search_info:
#                             yield f"event: sources\ndata: {json.dumps(search_info, ensure_ascii=False)}\n\n"
#                             sent_sources = True
#                         if text_piece:
#                             tail = deduper.feed(text_piece)
#                             if tail:
#                                 response_text += tail
#                                 yield f"data: {tail}\n\n"
#
#             else:
#                 # DeepSeek 等：老的字符串增量流
#                 async for chunk in call_llm_stream(
#                         provider=provider,
#                         prompt=prompt
#                 ):
#                     response_text += chunk
#                     yield f"data: {chunk}\n\n"
#
#             # ====== 持久化 & 缓存 ======
#             await insert_message_es(talk_id, int(data.user_id), data.question, response_text)
#             history.append({"role": "user", "content": data.question})
#             history.append({"role": "assistant", "content": response_text})
#             await ioredis.set(redis_key, json.dumps(history, ensure_ascii=False), ex=3600)
#
#             # 整段为空，明确告知原因，避免只发 end 看不出问题
#             if not response_text.strip():
#                 yield f"event: error\ndata: {json.dumps({'reason': 'no_text_emitted', 'hint': 'check provider routing / raw event fields / api key'}, ensure_ascii=False)}\n\n"
#
#             yield "event: end\ndata: {}\n\n"
#
#         except Exception as e:
#             err = {"error": str(e)}
#             yield f"event: error\ndata: {json.dumps(err, ensure_ascii=False)}\n\n"
#
#     return StreamingResponse(
#         sse_stream_generator(),
#         media_type="text/event-stream",
#         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
#     )
#
#
#
#
# @router.post("/dialog/{talk_id}/control")
# async def control_dialog(talk_id: int, data: ControlRequest):
#     session_user_id = await get_session_user_es(talk_id)
#     if not session_user_id or str(session_user_id) != data.user_id:
#         raise HTTPException(status_code=403, detail="无权限访问该对话")
#
#     status_key = f"dialog:status:{data.user_id}:{talk_id}"
#     await ioredis.set(status_key, data.action)
#     return {"status": data.action, "msg": f"对话已设置为 {data.action}"}
#
#
#
#
# def build_prompt(history: List[dict], retrieved: List[str], user_input: str):
#     prompt = "以下是历史对话，如果与问题不相关请忽略：\n"
#     for h in history[-5:]:
#         role = "用户" if h["role"] == "user" else "助手"
#         prompt += f"{role}: {h['content']}\n"
#
#     if retrieved:
#         prompt += "\n以下是知识库中可能相关的信息：\n"
#         for i, r in enumerate(retrieved):
#             prompt += f"[信息{i+1}] {r}\n"
#
#     prompt += f"\n用户提问: {user_input}\n请助手结合历史和知识库回答。"
#     return prompt
#
#
#
#
# async def es_hybrid_search(question: str) -> List[str]:
#     async with httpx.AsyncClient(timeout=10) as client:
#         response = await client.post(
#             "http://localhost:8000/es_hybrid_search",
#             json={"query": question}
#         )
#         response.raise_for_status()
#         return [r["content"] for r in response.json()]
#
# def is_prompt_format(h):
#     return isinstance(h, dict) and "role" in h and "content" in h
#
#
# def build_messages(history: List[Dict[str, Any]], retrieved: List[str], question: str) -> List[Dict[str, str]]:
#     """
#     history: 形如 [{"role":"user","content":"..."},{"role":"assistant","content":"..."}]
#     retrieved: 你的混合检索返回的若干片段
#     """
#     msgs: List[Dict[str, str]] = []
#
#     # 1) 系统提示
#     sys_prompt = (
#         "你是专业的检索增强问答助手。"
#         "优先基于【候选资料】回答；若资料不足，再结合常识作答。"
#         "引用外部来源时使用角标 [ref_i]，与来源列表编号一致；"
#         "若无可引用来源请勿编造。"
#     )
#     msgs.append({"role": "system", "content": sys_prompt})
#
#     # 2) 最近历史（防过长）
#     for h in (history or [])[-3:]:
#         role = h.get("role")
#         content = h.get("content") or ""
#         if role in ("user", "assistant", "system") and content:
#             msgs.append({"role": role, "content": content})
#
#     # 3) 候选资料
#     if retrieved:
#         kb = []
#         for i, r in enumerate(retrieved, 1):
#             kb.append(f"[候选{i}] {r}")
#         kb_text = "以下为候选资料（可能部分不相关，请自行甄别）：\n" + "\n".join(kb)
#         msgs.append({"role": "system", "content": kb_text})
#
#     # 4) 当前问题
#     msgs.append({"role": "user", "content": question})
#
#     return msgs





# routers/dialog_routers.py
# 改进点：
# 1) /dialog/start 保持不变（基于你现有的 ES 存储）
# 2) /dialog/{talk_id}/ask 支持 stream=true 的 SSE 输出，且会：
#    - 实时下发正文增量（event: message）
#    - 实时下发来源（event: source）
#    - 结束时下发拼接后的完整文本（event: done，含 sources 汇总）
# 3) 非流式时，直接返回 {"text": "...", "sources": [...]}
# 4) 统一通过 llm_service_new_bak 的 call_llm_stream / call_llm_once，确保拿到 search_info
import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any
import json

from routers.schema import (
    StartDialogRequest,
    AskRequest,
    ControlRequest,
)
from dialog_service.dialog_es import (
    create_session_es,
    insert_message_es,           # 签名: (talk_id, user_id, input_content, output_content)
    get_history_by_session_es,
    get_session_user_es,
)
from dialog_service.llm_service_new_bak2 import (
    build_messages,
    call_llm_stream,
)

router = APIRouter()


@router.post("/dialog/start")
async def start_dialog(data: StartDialogRequest):
    """新建对话，返回 talk_id"""
    talk_id = await create_session_es(int(data.user_id), data.title or "新会话")
    return {"talk_id": str(talk_id)}


def _sse_pack(event: str, payload: Any) -> str:
    """SSE 帧编码"""
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _format_sources_block(sources: List[dict]) -> str:
    """把来源拼接成回答末尾的文本块"""
    if not sources:
        return ""
    lines = []
    for r in sources:
        idx = r.get("index")
        title = (r.get("title") or "").strip()
        url = (r.get("url") or "").strip()
        if idx:
            lines.append(f"[ref_{idx}] {title} {url}".strip())
        else:
            lines.append(f"{title} {url}".strip())
    return "\n\n参考来源：\n" + "\n".join(lines)


@router.post("/dialog/{talk_id}/ask")
async def ask_dialog(talk_id: int, req: AskRequest):
    """
    流式 SSE：
      - event: message -> 正文增量 {"delta": "..."}
      - event: source  -> 最后一条来源列表 [{"index":..,"title":..,"url":..}, ...]
    ES 入库：
      - input_content  = 用户问题
      - output_content = 完整回答 + 来源块
    """

    # 校验会话
    user_id_from_es = await get_session_user_es(talk_id)
    if user_id_from_es is None:
        raise HTTPException(status_code=403, detail="无权访问该对话，或对话不存在")

    # 历史上下文
    hist = await get_history_by_session_es(talk_id)
    # 🔑 调用混合检索
    retrieved: List[str] = await es_hybrid_search(req.question)


    # 构造 messages
    msgs = build_messages(history=hist, retrieved=retrieved, user_input=req.question)

    provider: str = getattr(req, "provider", None) or "qwen"
    enable_search = True if getattr(req, "enable_search", None) is None else bool(req.enable_search)
    qwen_extra: Dict[str, Any] = getattr(req, "qwen_parameters", None) or {}

    async def gen():
        response_text_parts: List[str] = []
        collected_sources: List[Dict[str, Any]] = []
        seen_urls = set()

        # 收集来源（不推流，最后统一发）
        def on_source_cb(r: Dict[str, Any]):
            url = (r.get("url") or "").strip().lower()
            if url and url not in seen_urls:
                seen_urls.add(url)
                collected_sources.append(r)

        # 1. 流式推送正文
        async for chunk in call_llm_stream(
            provider=provider,
            messages=msgs,
            enable_search=enable_search,
            qwen_parameters=qwen_extra,
            on_source=on_source_cb,
        ):
            if chunk:
                response_text_parts.append(chunk)
                yield _sse_pack("message", {"delta": chunk})

        # 2. 最后一条来源
        if collected_sources:
            yield _sse_pack("source", collected_sources)

        # 3. 入库（问题 + 完整回答+来源）
        final_text = "".join(response_text_parts).rstrip()
        if collected_sources:
            final_text += _format_sources_block(collected_sources)

        await insert_message_es(
            talk_id,
            int(req.user_id),
            req.question,   # ✅ 存到 input_content
            final_text,     # ✅ 存到 output_content
        )

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/dialog/{talk_id}/control")
async def control_dialog(talk_id: int, req: ControlRequest):
    """对话控制占位（按需实现 stop/pause/resume 等）"""
    return {"ok": True, "msg": f"received: {getattr(req, 'action', '')}"}


async def es_hybrid_search(question: str) -> List[str]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            "http://localhost:8000/es_hybrid_search",
            json={"query": question}
        )
        response.raise_for_status()
        return [r["content"] for r in response.json()]