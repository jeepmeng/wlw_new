from typing import Iterable, List
from .sensitive_filter_ac import SensitiveFilterAC, _mask_text

class StreamingMasker:
    def __init__(self, sf: SensitiveFilterAC, placeholder: str = "[已屏蔽]"):
        self.sf = sf
        self.tail = ""
        self.tail_len = max(sf.max_pat_len - 1, 0)
        self.placeholder = placeholder

    def feed(self, chunk: str) -> List[str]:
        if not chunk:
            return []
        window = self.tail + chunk
        # 计算安全输出边界：保留尾部 tail_len 供下一次跨边界匹配
        safe_upto = max(len(window) - self.tail_len, 0)
        safe_part = window[:safe_upto]
        keep_part = window[safe_upto:]
        masked_safe, _ = self.sf.mask(safe_part)
        # 更新尾巴为未遮罩原文（用于下一次匹配），输出则用已遮罩的安全段
        self.tail = keep_part
        return [masked_safe] if masked_safe else []

    def flush(self) -> List[str]:
        if not self.tail:
            return []
        masked_tail, _ = self.sf.mask(self.tail)
        self.tail = ""
        return [masked_tail] if masked_tail else []