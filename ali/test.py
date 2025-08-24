from dashscope import Generation

API_KEY = "sk-e8d4973beecd4a43bdce4718b0b2444c"
MODEL = "qwen-plus"   # 建议用 qwen-plus 或 qwen2.5-32b-instruct，qwen-long 可能不支持联网

def stream_answer(query: str):
    responses = Generation.call(
        api_key=API_KEY,
        model=MODEL,
        prompt=query,
        parameters={"enable_search": True,
                    "search_options": {
                        "forced_search": True,
                        "search_strategy": "max",
                        "enable_source": True,
                        "citation_format": "[ref_<number>]",  # 角标形式为[ref_i]
                    }
                    },  # 联网开关写在 parameters
        stream=True
    )

    acc = ""  # 已累计文本
    for event in responses:
        out = getattr(event, "output", {}) or {}

        # 1) 纯增量：最干净
        delta = out.get("text_delta")
        if delta:
            acc += delta
            yield delta
            continue

        # 2) Chat风格：choices[0].delta.content / text
        choices = out.get("choices") or []
        if choices:
            d = (choices[0].get("delta") or {})
            chunk = d.get("content") or d.get("text")
            if chunk:
                acc += chunk
                yield chunk
                continue

        # 3) 全量文本兜底（某些版本会周期性下发完整text）
        full = out.get("text")
        if full:
            if len(full) > len(acc):
                # 只打印新增的尾巴，避免重复
                tail = full[len(acc):]
                acc = full
                if tail:
                    yield tail
            continue

    # 需要完整答案时，返回 acc
    return acc

if __name__ == "__main__":
    q = ("2023年4月4日的股市大盘帮我分析一下")
    print("Q:", q)
    print("A:", end="", flush=True)

    final = ""
    for piece in stream_answer(q):
        final += piece
        print(piece, end="", flush=True)

    print("\n\n[完整结果]\n", final)