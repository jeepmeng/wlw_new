# dialog_service/llm_service.py
from typing import AsyncGenerator, Optional, Dict, Any, List
import asyncio
from contextlib import suppress

# 你已有的：DeepSeek 的异步流封装（保持不变）
from dialog_service.llm_service import call_llm as deepseek_call_llm  # 若与当前文件冲突，请改成正确引用
from config.settings import settings
import re
from collections import defaultdict


class StreamDeduper:
    """
    用于流式增量的去重与抑制：
      - 只追加“新尾巴”（处理全量重发/重叠）
      - 基于 n-gram 的复读抑制
      - 正则连续重复折叠
    """
    def __init__(self, ngram=16, max_count=3):
        self.buf = ""                     # 已累计文本
        self.ngram = ngram               # n-gram 长度（推荐 12~24）
        self.max_count = max_count       # 某 n-gram 允许的最大重复次数
        self.ngram_cnt = defaultdict(int)

    def _tail_only(self, piece: str) -> str:
        """只返回 piece 相对于 buf 的新增尾巴。"""
        if not piece:
            return ""
        b = self.buf
        p = piece
        # 若 piece 已经完全包含在 buf 的尾部，直接丢弃
        if p and b.endswith(p):
            return ""

        # 找到 b 的某个后缀与 p 的最长前缀重叠，输出不重叠的尾巴
        max_overlap = min(len(b), len(p))
        k = max_overlap
        while k > 0 and not b.endswith(p[:k]):
            k -= 1
        return p[k:]

    def _suppress_by_ngram(self, text: str) -> str:
        """按 n-gram 计数抑制复读：若某片段重复次数超阈值，跳过。"""
        if not text:
            return ""

        # 如果文本太短，不做 ngram 抑制
        if len(text) <= self.ngram:
            return text

        out_chars = []
        for i, ch in enumerate(text):
            out_chars.append(ch)
            # 每产生一个新 ngram 就统计
            if len(self.buf) + len(out_chars) >= self.ngram:
                start = len(self.buf) + len(out_chars) - self.ngram
                # 从“整体输出”的视角抽取 ngram
                ngram_str = (self.buf + "".join(out_chars))[start:start+self.ngram]
                self.ngram_cnt[ngram_str] += 1
                if self.ngram_cnt[ngram_str] > self.max_count:
                    # 超阈值，回退这个字符，不加入
                    out_chars.pop()
        return "".join(out_chars)

    def _collapse_repeats(self, text: str) -> str:
        """把 text 内部的连续重复大段折叠（如 AAAA→A）。"""
        if not text:
            return ""
        # 重复片段长度阈值，中文建议 10~30，这里用 20
        return re.sub(r'(.{20,}?)\1+', r'\1', text)

    def feed(self, piece: str) -> str:
        """
        输入一个增量，返回**应该输出**的新增尾巴（已去重/抑制/折叠）。
        """
        tail = self._tail_only(piece)
        if not tail:
            return ""
        tail = self._suppress_by_ngram(tail)
        if not tail:
            return ""
        tail = self._collapse_repeats(tail)
        if not tail:
            return ""
        # 记入总缓冲
        self.buf += tail
        return tail


# ===== Qwen 同步流式 → 异步封装（保留：字符串增量路径） =====
def _qwen_sync_iter(
    prompt: Optional[str] = None,
    *,
    messages: Optional[List[Dict[str, str]]] = None,
    model: str,
    api_key: str,
    parameters: Optional[Dict[str, Any]] = None
):
    """dashscope 同步流式生成器，在线程中跑。支持 prompt 或 messages。"""
    from dashscope import Generation

    if not (prompt or messages):
        raise ValueError("必须提供 prompt 或 messages 之一")

    params = dict(parameters or {})
    # 默认强制联网+引用+message 格式
    params.setdefault("enable_search", True)
    params.setdefault("search_options", {
        "forced_search": True,
        "enable_source": True,
        "citation_format": "[ref_<number>]",
        "search_strategy": "turbo",
    })
    params.setdefault("result_format", "message")

    call_kwargs = {
        "api_key": api_key,
        "model": model,
        "stream": True,
        "parameters": params,
        "extra_body": params,   # 双管齐下，最大兼容
    }

    if messages is not None:
        call_kwargs["messages"] = messages
    else:
        call_kwargs["prompt"] = prompt

    responses = Generation.call(**call_kwargs)

    acc = ""  # 处理偶发的全量 text 去重
    for event in responses:
        out = getattr(event, "output", {}) or {}

        # 1) 增量
        delta = out.get("text_delta")
        if delta:
            acc += delta
            yield delta
            continue

        # 2) Chat 风格 choices[].delta.content/text/message.content
        choices = out.get("choices") or []
        if choices:
            d = (choices[0].get("delta") or {})
            # 新旧两种字段都试一下
            chunk = d.get("content") or d.get("text")
            if not chunk:
                msg = (choices[0].get("message") or {})
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
    prompt: Optional[str] = None,
    *,
    messages: Optional[List[Dict[str, str]]] = None,
    model: str,
    api_key: str,
    parameters: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[str, None]:
    """
    把 dashscope 的同步 stream 包装成真正的 async 流（字符串增量）。
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    DONE = object()

    def worker():
        try:
            for piece in _qwen_sync_iter(
                prompt,
                messages=messages,
                model=model,
                api_key=api_key,
                parameters=parameters,
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


# ==== NEW: 安全读取工具（Qwen 原始事件解析时会用到） ====
def safe_get(obj, attr, default=None):
    try:
        return getattr(obj, attr)
    except Exception:
        return default
def parse_qwen_stream_chunk_once(chunk):
    """
    只返回该事件的一段增量（命中即止），避免同一帧多路重复。
    返回: (text_delta: str|None, search_info_dict: dict|None)
    """
    text_piece = None
    search_info = None

    out = safe_get(chunk, "output", None)

    # 优先级：text_delta → delta.text/content → message.content → output_text → output.text
    if out:
        tp = safe_get(out, "text_delta", None)
        if isinstance(tp, str) and tp:
            text_piece = tp
        else:
            try:
                choices = safe_get(out, "choices", []) or []
                if choices:
                    delta = safe_get(choices[0], "delta", {}) or {}
                    dc = delta.get("text") or delta.get("content")
                    if isinstance(dc, str) and dc:
                        text_piece = dc
                    else:
                        msg = safe_get(choices[0], "message", None)
                        mc = getattr(msg, "content", None) if msg else None
                        if isinstance(mc, str) and mc:
                            text_piece = mc
            except Exception:
                pass

        if text_piece is None:
            piece = safe_get(chunk, "output_text", None)
            if isinstance(piece, str) and piece:
                text_piece = piece
            else:
                maybe_text = safe_get(out, "text", None)
                if isinstance(maybe_text, str) and maybe_text:
                    text_piece = maybe_text

        si = safe_get(out, "search_info", None)
        if si:
            try:
                sd = si if isinstance(si, dict) else getattr(si, "__dict__", {})
            except Exception:
                sd = {}
            if isinstance(sd, dict) and sd.get("search_results"):
                keep = []
                for it in sd["search_results"]:
                    keep.append({
                        "index": it.get("index"),
                        "title": it.get("title"),
                        "url": it.get("url"),
                        "site_name": it.get("site_name", ""),
                        "icon": it.get("icon", "")
                    })
                search_info = {"search_results": keep}

    return text_piece, search_info

# ==== NEW: Qwen 原始事件解析器（返回文本增量 + 搜索来源） ====
def parse_qwen_stream_chunk(chunk):
    """
    返回: (text_delta: str|None, search_info_dict: dict|None)
    - text_delta：当前事件追加的文本（可能是“根据”、“天气…”等）
    - search_info_dict：只在事件里带有 search_info 时返回 dict，否则 None
    """
    text_piece = None
    search_info = None

    # 1) 直取 output_text（某些版本会给）
    piece = safe_get(chunk, "output_text", None)
    if isinstance(piece, str) and piece:
        text_piece = piece

    out = safe_get(chunk, "output", None)
    if out:
        # 2) text_delta（常见于新版）
        td = safe_get(out, "text_delta", None)
        if isinstance(td, str) and td:
            text_piece = (text_piece or "") + td

        # 3) choices[].message.content（常见）
        try:
            choices = safe_get(out, "choices", []) or []
            if choices:
                delta = safe_get(choices[0], "delta", {}) or {}
                # 有的直接给 message.content，不走 delta
                msg = safe_get(choices[0], "message", None)
                if msg:
                    mc = getattr(msg, "content", None)
                    if isinstance(mc, str) and mc:
                        text_piece = (text_piece or "") + mc
                # 兼容 delta.content / delta.text
                dc = delta.get("content") or delta.get("text")
                if isinstance(dc, str) and dc:
                    text_piece = (text_piece or "") + dc
        except Exception:
            pass

        # 4) 兜底 output.text
        if not text_piece:
            maybe_text = safe_get(out, "text", None)
            if isinstance(maybe_text, str) and maybe_text:
                text_piece = maybe_text

        # 5) 搜索来源（本次事件若带就取）
        si = safe_get(out, "search_info", None)
        if si:
            try:
                sd = si if isinstance(si, dict) else getattr(si, "__dict__", {})
            except Exception:
                sd = {}
            if isinstance(sd, dict) and sd.get("search_results"):
                keep = []
                for it in sd["search_results"]:
                    keep.append({
                        "index": it.get("index"),
                        "title": it.get("title"),
                        "url": it.get("url"),
                        "site_name": it.get("site_name", ""),
                        "icon": it.get("icon", "")
                    })
                search_info = {"search_results": keep}

    return text_piece, search_info


# ==== NEW: Qwen 原始事件异步流（不做拼接，直接把 dashscope 的 chunk 吐出来） ====
async def qwen_stream_raw_events(
    *,
    model: str,
    api_key: str,
    messages: Optional[List[Dict[str, str]]] = None,
    prompt: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[object, None]:
    """
    直接把 dashscope.Generation.call(..., stream=True) 的 chunk 原样异步吐出来（不做文本拼接）
    仅用于需要解析 search_info、choices[].message.content 等结构化字段的场景。
    """
    if not (messages or prompt):
        raise ValueError("必须提供 messages 或 prompt")

    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    DONE = object()

    def worker():
        try:
            from dashscope import Generation

            extra_body = dict(extra or {})
            parameters = dict(extra or {})

            # 默认：强制联网 + 返回引用 + message 格式
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
                "stream": True,
                "extra_body": extra_body,
                "parameters": parameters,
            }
            if messages is not None:
                call_kwargs["messages"] = messages
            else:
                call_kwargs["prompt"] = prompt

            responses = Generation.call(**call_kwargs)
            for chunk in responses:
                placed = False
                while not placed:
                    try:
                        queue.put_nowait(chunk)
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
            yield item
    finally:
        with suppress(Exception):
            task.cancel()


# ===== 统一入口（保留：字符串增量的老接口，以兼容 DeepSeek/Qwen 简单用法） =====
async def call_llm_stream(
    prompt: Optional[str] = None,
    *,
    messages: Optional[List[Dict[str, str]]] = None,
    provider: str = "deepseek",               # "deepseek" | "qwen"
    enable_search: Optional[bool] = None,     # 仅对 qwen 有意义
    qwen_parameters: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[str, None]:
    """
    统一的流式接口（字符串增量）：
      - DeepSeek 走你已有的 async 生成器
      - Qwen 走 dashscope（从 settings.qwen 读取 key/model），但**仅返回字符串片段**
        若你需要结构化 search_info，请改用 qwen_stream_raw_events + parse_qwen_stream_chunk
    """
    if provider == "deepseek":
        async for chunk in deepseek_call_llm(prompt or ""):
            yield chunk
        return

    if provider == "qwen":
        api_key = getattr(settings.qwen, "api_key", "") or ""
        model = getattr(settings.qwen, "model", "qwen-plus")

        if not api_key:
            raise ValueError("缺少 Qwen API Key：请在 config.dev.yaml -> qwen.api_key 配置")

        params = dict(qwen_parameters or {})
        if enable_search is not None:
            params["enable_search"] = bool(enable_search)
        params.setdefault("enable_search", True)
        params.setdefault("search_options", {
            "forced_search": True,
            "enable_source": True,
            "citation_format": "[ref_<number>]",
            "search_strategy": "turbo",
        })
        params.setdefault("result_format", "message")

        async for chunk in _qwen_stream_async(
            prompt=prompt,
            messages=messages,
            model=model,
            api_key=api_key,
            parameters=params
        ):
            yield chunk
        return

    raise ValueError(f"未知 provider: {provider}")