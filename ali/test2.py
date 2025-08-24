# -*- coding: utf-8 -*-
"""
Qwen (DashScope) Streaming Demo:
- 非流式：打印 search_info（来源列表）+ 正文片段（对照）
- 流式：打印增量内容（多路径兜底），若无则打印原始事件结构
- 三处同时传参（顶层/parameters/extra_body），最大兼容
用法：
  python test2.py --q "杭州明天天气如何？请给出处引用。" --model qwen-plus
"""

import os
import sys
import json
import argparse


def safe_get(obj, attr, default=None):
    """dashscope 的 __getattr__ 可能抛 KeyError，这里做统一兜底。"""
    try:
        return getattr(obj, attr)
    except Exception:
        return default


def safe_pick(*candidates):
    """返回第一个非空字符串。"""
    for x in candidates:
        if isinstance(x, str) and x:
            return x
    return None


def run_demo(question: str, model: str):
    try:
        import dashscope
        from dashscope import Generation
        print("dashscope version:", getattr(dashscope, "__version__", "unknown"))
    except Exception as e:
        print("请先安装 dashscope：pip install dashscope")
        raise e

    # api_key = os.getenv("DASHSCOPE_API_KEY")
    # if not api_key:
    #     print("未检测到环境变量 DASHSCOPE_API_KEY，请先设置。")
    #     sys.exit(1)

    # 消息（Chat 风格）
    messages = [{"role": "user", "content": question}]

    # —— 最大兼容：把参数放三处（顶层、parameters、extra_body）——
    top_level = dict(
        enable_search=True,
        search_options={
            "forced_search": True,
            "enable_source": True,
            "citation_format": "[ref_<number>]",
            "search_strategy": "turbo",
        },
        result_format="message",
    )
    extra_body = dict(top_level)
    parameters = dict(top_level)

    # ===== 非流式：先拿 search_info（推荐用于来源展示） =====
    print("\n" + "=" * 20 + " 非流式：获取 search_info（对照） " + "=" * 20)
    try:
        resp = Generation.call(
            api_key="sk-e8d4973beecd4a43bdce4718b0b2444c",
            model=model,
            messages=messages,
            stream=False,
            extra_body=extra_body,
            parameters=parameters,
            **top_level,
        )
        out = safe_get(resp, "output", {}) or {}
        info = out.get("search_info") or {}
        results = info.get("search_results") or []
        if results:
            for web in results:
                idx = web.get("index")
                title = web.get("title")
                url = web.get("url")
                print(f"[ref_{idx}] {title} {url}")
        else:
            print("（非流式）没有返回 search_info / search_results 为空。")

        # 顺带打印一小段正文，方便你确认内容
        try:
            choices = out.get("choices") or []
            non_stream_text = (choices[0].get("message") or {}).get("content", "")
            if non_stream_text:
                print("\n[非流式正文片段]\n", non_stream_text[:300], "...")
        except Exception:
            pass
    except Exception as e:
        print("非流式失败：", repr(e))

    # ===== 流式：打印增量内容；解析不到就打印原始事件 =====
    print("\n" + "=" * 20 + " 流式输出开始 " + "=" * 20)
    try:
        responses = Generation.call(
            api_key="sk-e8d4973beecd4a43bdce4718b0b2444c",
            model=model,
            messages=messages,
            stream=True,
            extra_body=extra_body,
            parameters=parameters,
            **top_level,
        )

        full_text = ""
        event_idx = 0

        for chunk in responses:
            event_idx += 1
            printed = False

            # 1) 最常见：直接 output_text
            piece = safe_get(chunk, "output_text", None)
            if piece:
                print(piece, end="", flush=True)
                full_text += piece
                printed = True

            # 2) 从 output 里取
            out = safe_get(chunk, "output", None)
            if out:
                # 2.1) text_delta
                text_delta = safe_get(out, "text_delta", None)
                if text_delta:
                    print(text_delta, end="", flush=True)
                    full_text += text_delta
                    printed = True

                # 2.2) choices[].delta.message.content / content / text
                try:
                    choices = safe_get(out, "choices", []) or []
                    if choices:
                        delta = safe_get(choices[0], "delta", {}) or {}
                        msg = delta.get("message") or {}
                        content_piece = safe_pick(
                            msg.get("content"),
                            delta.get("content"),
                            delta.get("text"),
                        )
                        if content_piece:
                            print(content_piece, end="", flush=True)
                            full_text += content_piece
                            printed = True
                except Exception:
                    pass

                # 2.3) 兜底：output.text
                if not printed:
                    maybe_text = safe_get(out, "text", None)
                    if maybe_text:
                        print(maybe_text, end="", flush=True)
                        full_text += maybe_text
                        printed = True

                # 2.4) 若事件里带了 search_info，就打印（并不总是会带）
                try:
                    search_info = safe_get(out, "search_info", None)
                    if search_info:
                        print("\n\n[流中搜索信息]")
                        try:
                            print(
                                json.dumps(
                                    search_info
                                    if isinstance(search_info, dict)
                                    else getattr(search_info, "__dict__", str(search_info)),
                                    ensure_ascii=False,
                                    indent=2,
                                )
                            )
                        except Exception:
                            print(search_info)
                        print()
                except Exception:
                    pass

            # 3) 若以上均未命中，打印原始事件供排查
            if not printed:
                try:
                    to_dict = safe_get(chunk, "to_dict", None)
                    if callable(to_dict):
                        print(f"\n[原始事件#{event_idx}] {to_dict()}\n")
                    else:
                        # 最后兜底直接打印对象（可能比较丑，但能看字段）
                        print(f"\n[原始事件#{event_idx}] {chunk}\n")
                except Exception:
                    print(f"\n[原始事件#{event_idx}] <无法打印，dir={dir(chunk)}>\n")

        print("\n" + "=" * 20 + " 流式输出结束 " + "=" * 20)
        print("\n【整段合并】\n", full_text if full_text.strip() else "(空)")
    except Exception as e:
        print("流式失败：", repr(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--q", "--question", dest="q", type=str,
                        default="杭州明天天气如何？请给出处引用。")
    parser.add_argument("--model", type=str, default="qwen-plus")
    args = parser.parse_args()
    run_demo(args.q, args.model)