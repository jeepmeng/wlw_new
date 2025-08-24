# -*- coding: utf-8 -*-
"""
Qwen (DashScope) - 精简版：对比非流式 vs 流式 的 search_info
只打印来源，不打印正文。
"""

import argparse
from urllib.parse import urlparse


def safe_get(obj, attr, default=None):
    try:
        return getattr(obj, attr)
    except Exception:
        return default


def normalize_results(search_info):
    """统一解析 search_info"""
    results = []
    if not search_info:
        return results
    if isinstance(search_info, dict):
        cand = search_info.get("search_results") or search_info.get("results") or []
    elif isinstance(search_info, list):
        cand = search_info
    else:
        cand = getattr(search_info, "__dict__", {}) or {}
        cand = cand.get("search_results") or []
    for it in cand:
        if not isinstance(it, dict):
            continue
        results.append({
            "index": it.get("index"),
            "title": it.get("title") or "",
            "url": it.get("url") or "",
        })
    return results


def url_key(url: str):
    from urllib.parse import urlparse
    url = (url or "").strip()
    if not url:
        return None
    try:
        p = urlparse(url)
        path = (p.path or "/").rstrip("/")
        return (p.netloc.lower(), path.lower())
    except Exception:
        return url.lower()


def print_results(label, results):
    print(f"\n==== {label} (共{len(results)}条) ====")
    for r in results:
        print(f"[ref_{r.get('index')}] {r.get('title')} {r.get('url')}")


def run_demo(question: str, model: str):
    from dashscope import Generation

    messages = [{"role": "user", "content": question}]
    params = dict(
        enable_search=True,
        search_options={"forced_search": True, "enable_source": True},
        result_format="message",
    )

    API_KEY = "sk-e8d4973beecd4a43bdce4718b0b2444c"

    # 非流式
    resp = Generation.call(
        api_key=API_KEY, model=model, messages=messages,
        stream=False, extra_body=params, parameters=params, **params
    )
    non_stream_info = safe_get(resp, "output", {}).get("search_info", {})
    non_stream_results = normalize_results(non_stream_info)
    print_results("非流式 search_info", non_stream_results)

    # 流式
    responses = Generation.call(
        api_key=API_KEY, model=model, messages=messages,
        stream=True, extra_body=params, parameters=params, **params
    )
    seen = set()
    stream_results = []
    for chunk in responses:
        out = safe_get(chunk, "output", None)
        if not out:
            continue
        s = safe_get(out, "search_info", None)
        if not s:
            continue
        for r in normalize_results(s):
            key = url_key(r.get("url"))
            if key and key not in seen:
                seen.add(key)
                stream_results.append(r)
                print(f"[流式] [ref_{r.get('index')}] {r.get('title')} {r.get('url')}")

    print_results("流式 search_info 去重汇总", stream_results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--q", dest="q", type=str,
                        default="杭州明天天气如何？请给出处引用。")
    parser.add_argument("--model", type=str, default="qwen-plus")
    args = parser.parse_args()
    run_demo(args.q, args.model)