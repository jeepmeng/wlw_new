import httpx
from fastapi import APIRouter, HTTPException, Request
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

# NEW: 引入流式遮罩器（你放在 sensitive/streaming_mask.py）
from sensitive.streaming_mask import StreamingMasker
# 可选：如果你希望对“来源标题”也遮罩，导入过滤器类型以便拿到 sf
from sensitive.sensitive_filter_ac import SensitiveFilterAC

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
async def ask_dialog(talk_id: int, req: AskRequest, request: Request):
    """
    流式 SSE：
      - event: message -> 正文增量 {"delta": "..."}（已做敏感词遮罩）
      - event: source  -> 最后一条来源列表 [{"index":..,"title":..,"url":..}, ...]（可选遮罩标题）
    ES 入库：
      - input_content  = 用户问题（输入侧命中会被中间件拦截，能到这就是安全的）
      - output_content = 完整回答(已遮罩) + 来源块(标题可选遮罩)
    """
    # 取全局过滤器（在 app 启动 lifespan 里放到 app.state.sf）
    sf: SensitiveFilterAC = request.app.state.sf  # type: ignore
    sm = StreamingMasker(sf)  # 流式增量遮罩器

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
        # 注意：我们收集的是“已遮罩”的增量，用于最终入库，避免泄露
        masked_parts: List[str] = []
        collected_sources: List[Dict[str, Any]] = []
        seen_urls = set()

        # 收集来源（不推流，最后统一发）；可选：对 title 做遮罩
        def on_source_cb(r: Dict[str, Any]):
            url = (r.get("url") or "").strip().lower()
            if url and url not in seen_urls:
                seen_urls.add(url)
                # 仅对标题进行遮罩（URL 一般不需要改动；若你也想遮罩可自行调用 sf.mask(url)）
                title = (r.get("title") or "")
                masked_title, _ = sf.mask(title)
                r = {**r, "title": masked_title}
                collected_sources.append(r)

        # 1) 流式推送正文（分片遮罩）
        async for chunk in call_llm_stream(
            provider=provider,
            messages=msgs,
            enable_search=enable_search,
            qwen_parameters=qwen_extra,
            on_source=on_source_cb,
        ):
            if not chunk:
                continue
            # 将原始增量喂入遮罩器；可能会返回 0或1 个可安全输出的片段
            for out in sm.feed(chunk):
                masked_parts.append(out)
                yield _sse_pack("message", {"delta": out})

        # 将尾巴 flush 掉（跨分片最大词长-1 的部分）
        for out in sm.flush():
            masked_parts.append(out)
            yield _sse_pack("message", {"delta": out})

        # 2) 最后一条来源（标题已遮罩）
        if collected_sources:
            yield _sse_pack("source", collected_sources)

        # 3) 入库（问题 + 完整回答+来源），使用“已遮罩”的最终文本
        final_text = "".join(masked_parts).rstrip()
        if collected_sources:
            final_text += _format_sources_block(collected_sources)

        await insert_message_es(
            talk_id,
            int(req.user_id),
            req.question,   # ✅ input_content：中间件已保障安全（命中会被拦截，根本到不了这里）
            final_text,     # ✅ output_content：使用已遮罩文本，避免泄漏
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