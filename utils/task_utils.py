# utils/task_utils.py

from task.gen_vector_chain import encode_text_task

def submit_vector_task_with_option(
    text: str,
    write_to_redis: bool = False,
    use_chain_style: bool = False
):
    """
    向量任务统一入口：
    - write_to_redis=True → apply_async + 写入 Redis（用于接口轮询）
    - use_chain_style=True → 用于 chain 构建 .s()
    - 默认：直接调用 .delay()
    """
    if use_chain_style:
        return encode_text_task.s(text)
    elif write_to_redis:
        return encode_text_task.apply_async(args=[text], kwargs={"use_redis": True})
    else:
        return encode_text_task.delay(text)



class NonRetryableLoaderError(Exception):
    """用于标记不需要 retry 的异常"""
    pass