# dialog_service/llm_service_new_bak.py
# 改进点：
# 1) Qwen 使用 messages + result_format="message"
# 2) extra_body/parameters 均带 enable_search、forced_search、enable_source
# 3) 流式：逐帧解析 output.search_info 与 output.text_delta（兼容 output_text 兜底），通过 on_source 回调实时抛出来源
# 4) 非流式：返回 (text, sources)
# 5) DeepSeek：保持你原有封装（call_llm(messages=...)）完全不变
# 6) 提供 build_messages(history, retrieved, user_input) 构造消息

from typing import AsyncGenerator, Optional, Dict, Any, List, Tuple, Callable
import asyncio
import threading
from queue import Queue, Empty

# 你已有的 DeepSeek 异步流封装（保持不变的调用方式）
from dialog_service.llm_service import call_llm as deepseek_call_llm
from config.settings import settings


# ========== 常用小工具 ==========
def _url_key(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        p = urlparse(url.strip())
        host = (p.netloc or "").lower()
        path = (p.path or "/").rstrip("/").lower()
        return f"{host}{path}"
    except Exception:
        return url.strip().lower() or None


def _safe_get(obj, attr, default=None):
    try:
        return getattr(obj, attr)
    except Exception:
        return default


def _normalize_results(search_info) -> List[Dict[str, Any]]:
    """
    兼容 search_info 的多种形态，统一提取为 [{index,title,url}]
    """
    results: List[Dict[str, Any]] = []
    if not search_info:
        return results
    cand = None
    if isinstance(search_info, dict):
        cand = search_info.get("search_results") or search_info.get("results") or []
    elif isinstance(search_info, list):
        cand = search_info
    else:
        d = getattr(search_info, "__dict__", {}) or {}
        cand = d.get("search_results") or d.get("results") or []
    for it in cand or []:
        if isinstance(it, dict):
            results.append({
                "index": it.get("index"),
                "title": it.get("title") or "",
                "url": it.get("url") or "",
            })
    return results


# ========== Qwen 参数 ==========
def _qwen_params(enable_search: bool = True, qwen_parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    统一的联网与结果格式参数
    """
    search_opts = {"forced_search": True, "enable_source": True}
    base = dict(
        enable_search=bool(enable_search),
        search_options=search_opts,
        result_format="message",
    )
    if qwen_parameters:
        # 不覆盖核心行为
        for k, v in qwen_parameters.items():
            if k not in ["enable_search", "search_options", "result_format"]:
                base[k] = v
    return base


# ========== Qwen 流式 ==========
async def qwen_stream(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    enable_search: bool = True,
    qwen_parameters: Optional[Dict[str, Any]] = None,
    on_source: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> AsyncGenerator[str, None]:
    """
    Qwen 流式：兼容多种返回形态，并做“增量提取”。
      - 抽取位置：output_text / output.text_delta / output.text /
                 output.choices[].delta.content / output.choices[].message.content
      - 仅把【新增后缀】yield 出去，避免重复内容反复返回
    """
    from dashscope import Generation
    import asyncio, threading
    from queue import Queue

    params = _qwen_params(enable_search=enable_search, qwen_parameters=qwen_parameters)
    api_key = getattr(getattr(settings, "qwen", object()), "api_key", "") or ""
    model = model or (getattr(settings, "qwen_model", None) or "qwen-plus")

    seen_sources = set()
    assembled = ""  # 已累计的完整文本

    def _emit_sources(si):
        for r in _normalize_results(si):
            key = _url_key(r.get("url") or "")
            if key and key not in seen_sources:
                seen_sources.add(key)
                if on_source:
                    on_source(r)

    def _extract_candidates(chunk) -> List[str]:
        """把可能的文本字段取出来（顺序越靠前优先级越高）"""
        cands: List[str] = []
        # 1) 旧字段
        piece = _safe_get(chunk, "output_text", None)
        if isinstance(piece, str) and piece:
            cands.append(piece)

        out = _safe_get(chunk, "output", None)
        if not out:
            return cands

        # 2) 标准字段
        if isinstance(out, dict):
            td = out.get("text_delta")
            if isinstance(td, str) and td:
                cands.append(td)
            txt = out.get("text")
            if isinstance(txt, str) and txt:
                cands.append(txt)
            choices = out.get("choices")
            if isinstance(choices, list):
                for ch in choices:
                    if isinstance(ch, dict):
                        d = (ch.get("delta") or {}).get("content") if ch.get("delta") else None
                        if isinstance(d, str) and d:
                            cands.append(d)
                        m = (ch.get("message") or {}).get("content") if ch.get("message") else None
                        if isinstance(m, str) and m:
                            cands.append(m)
            # 同步搜集来源
            si = out.get("search_info")
            if si:
                _emit_sources(si)
        else:
            td = _safe_get(out, "text_delta", None)
            if isinstance(td, str) and td:
                cands.append(td)
            txt = _safe_get(out, "text", None)
            if isinstance(txt, str) and txt:
                cands.append(txt)
            choices = _safe_get(out, "choices", None)
            if isinstance(choices, list):
                for ch in choices:
                    if isinstance(ch, dict):
                        d = (ch.get("delta") or {}).get("content") if ch.get("delta") else None
                        if isinstance(d, str) and d:
                            cands.append(d)
                        m = (ch.get("message") or {}).get("content") if ch.get("message") else None
                        if isinstance(m, str) and m:
                            cands.append(m)
            si = _safe_get(out, "search_info", None)
            if si:
                _emit_sources(si)

        return cands

    def _yield_delta(text_frag: str) -> Optional[str]:
        """
        只返回【新增部分】：
        - 如果是累计文本：frag 以 assembled 为前缀 → 只取后缀
        - 如果是纯增量：直接返回，并把 assembled += frag
        - 其他复杂情况：做最长公共前缀（LCP）后，取后缀
        """
        nonlocal assembled
        if not text_frag:
            return None

        # 累计文本：前缀一致，返回新增后缀
        if len(text_frag) >= len(assembled) and text_frag.startswith(assembled):
            delta = text_frag[len(assembled):]
            assembled = text_frag
            return delta if delta else None

        # 纯增量：直接追加
        if len(text_frag) <= 64 and not assembled.endswith(text_frag):
            assembled += text_frag
            return text_frag

        # LCP 回退（兼容边界情况）
        l = 0
        max_l = min(len(assembled), len(text_frag))
        while l < max_l and assembled[l] == text_frag[l]:
            l += 1
        delta = text_frag[l:]
        if delta:
            # 这里按累计策略更新 assembled：取更长的作为最新“累计”
            if len(text_frag) > len(assembled) and text_frag.startswith(assembled[:l]):
                assembled = text_frag
            else:
                assembled += delta
            return delta
        return None

    # 线程生产，队列消费
    q: Queue = Queue()
    STOP = object()

    def _producer():
        try:
            responses = Generation.call(
                api_key=api_key,
                model=model,
                messages=messages,
                stream=True,
                extra_body=params,
                parameters=params,
                **params,
            )
            for chunk in responses:
                q.put(chunk)
        except Exception as e:
            q.put(e)
        finally:
            q.put(STOP)

    t = threading.Thread(target=_producer, daemon=True)
    t.start()

    loop = asyncio.get_event_loop()
    while True:
        item = await loop.run_in_executor(None, q.get)
        if item is STOP:
            break
        if isinstance(item, Exception):
            break

        for frag in _extract_candidates(item):
            delta = _yield_delta(frag)
            if isinstance(delta, str) and delta:
                yield delta

        # 再补一次来源（冗余调用也会去重）
        out = _safe_get(item, "output", None)
        if out:
            si = _safe_get(out, "search_info", None)
            if si:
                _emit_sources(si)


# ========== Qwen 非流式 ==========
async def qwen_once(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    enable_search: bool = True,
    qwen_parameters: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Qwen 非流式：一次性拿全量文本与来源。
    返回： (full_text, sources_list)
    """
    from dashscope import Generation

    params = _qwen_params(enable_search=enable_search, qwen_parameters=qwen_parameters)
    api_key = settings.qwen.api_key or ""
    model = model or (getattr(settings, "qwen_model", None) or "qwen-plus")

    resp = Generation.call(
        api_key=api_key,
        model=model,
        messages=messages,
        stream=False,
        extra_body=params,
        parameters=params,
        **params,
    )

    # 文本
    full_text = ""
    out = _safe_get(resp, "output", None)
    if isinstance(out, dict):
        full_text = out.get("text", "") \
            or (out.get("choices", [{}])[0].get("message", {}).get("content", "") if out.get("choices") else "") \
            or ""
    else:
        full_text = _safe_get(resp, "output_text", "") or ""

    # 来源
    sources = []
    si = _safe_get(out, "search_info", None) if out else None
    if si:
        sources = _normalize_results(si)

    return full_text, sources


# ========== 统一入口：根据 provider 选择 ==========
async def call_llm_stream(
    *,
    provider: str,
    messages: List[Dict[str, str]],
    enable_search: bool = True,
    qwen_parameters: Optional[Dict[str, Any]] = None,
    on_source: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> AsyncGenerator[str, None]:
    """
    流式统一入口：
    - provider="qwen"：qwen_stream（带来源回调）
    - provider="deepseek"：完全复用你原来的 deepseek_call_llm(messages=...) 行为
    """
    if provider == "qwen":
        async for piece in qwen_stream(
            messages,
            enable_search=enable_search,
            qwen_parameters=qwen_parameters,
            on_source=on_source,
        ):
            if piece:
                yield piece
        return

    if provider == "deepseek":
        # 1) 优先尝试 messages 形态（若你的封装支持）
        try:
            async for piece in deepseek_call_llm(messages=messages):
                if piece:
                    yield piece
            return
        except TypeError:
            pass
        except Exception:
            pass

        # 2) 回退：老封装只吃 prompt → 扁平化所有 messages（保留历史 + retrieved）
        prompt = messages_to_prompt(messages)
        async for piece in deepseek_call_llm(prompt):
            if piece:
                yield piece
        return

    raise ValueError(f"未知 provider: {provider}")


async def call_llm_once(
    *,
    provider: str,
    messages: List[Dict[str, str]],
    enable_search: bool = True,
    qwen_parameters: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    非流式统一入口：
    - provider="qwen"：qwen_once（返回文本+来源）
    - provider="deepseek"：复用原有流式封装，拼接成完整文本（不含来源）
    """
    if provider == "qwen":
        return await qwen_once(
            messages,
            enable_search=enable_search,
            qwen_parameters=qwen_parameters,
        )

    if provider == "deepseek":
        # 1) 先试 messages 形态
        text = ""
        try:
            async for piece in deepseek_call_llm(messages=messages):
                text += piece or ""
            return text, []
        except TypeError:
            pass
        except Exception:
            pass

        # 2) 回退：prompt 形态（扁平化，保留历史 + retrieved）
        prompt = messages_to_prompt(messages)
        async for piece in deepseek_call_llm(prompt):
            text += piece or ""
        return text, []

    raise ValueError(f"未知 provider: {provider}")


# ========== 构造 messages：带历史/候选资料 ==========
def build_messages(
    *,
    history: List[Dict[str, str]],
    retrieved: Optional[List[str]],
    user_input: str,
    keep_last_n: int = 3
) -> List[Dict[str, str]]:
    """
    - history: [{"role":"user"/"assistant"/"system","content":"..."}]
    - retrieved: 召回的候选文段，会做一个 system 提示并列表化
    - user_input: 当前用户问题
    """
    msgs: List[Dict[str, str]] = []

    # 1) 历史（只保留最近 N 轮）
    for h in (history or [])[-keep_last_n:]:
        role = (h.get("role") or "").strip()
        content = (h.get("content") or "").strip()
        if role in ("user", "assistant", "system") and content:
            msgs.append({"role": role, "content": content})

    # 2) 候选资料（可选）
    if retrieved:
        kb_lines = [f"[候选{i}] {r}" for i, r in enumerate(retrieved, 1)]
        kb_text = "以下为候选资料（可能部分不相关，请自行甄别）：\n" + "\n".join(kb_lines)
        msgs.append({"role": "system", "content": kb_text})

    # 3) 当前问题
    msgs.append({"role": "user", "content": (user_input or "").strip()})

    return msgs



def messages_to_prompt(messages: List[Dict[str, str]]) -> str:
    """把多轮 messages（含 system/用户/助手、retrieved 的 system 段）扁平为单个 prompt。"""
    role_map = {"system": "系统", "user": "用户", "assistant": "助手"}
    parts: List[str] = []
    for m in messages:
        role = role_map.get((m.get("role") or "").lower(), "系统")
        content = (m.get("content") or "").strip()
        if content:
            parts.append(f"[{role}]\n{content}")
    parts.append("\n请基于以上上下文与候选资料，严谨作答：")
    return "\n\n".join(parts)