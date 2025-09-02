# utils/pdf_ocr.py
from typing import List, Dict
import io
import re
import fitz  # PyMuPDF
from PIL import Image
import numpy as np
from paddleocr import PaddleOCR

# --------- 小优化：按语言缓存 PaddleOCR 实例，避免重复初始化 ----------
_OCR_CACHE: Dict[str, PaddleOCR] = {}

def _get_paddle(lang: str) -> PaddleOCR:
    inst = _OCR_CACHE.get(lang)
    if inst is None:
        inst = PaddleOCR(use_angle_cls=True, lang=lang)
        _OCR_CACHE[lang] = inst
    return inst
# -----------------------------------------------------------------------

def _normalize_whitespace(text: str) -> str:
    text = text.replace("\u00A0", " ")  # 不换行空格
    text = re.sub(r"[\t\r]+", " ", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()

def ocr_image(img: Image.Image, lang: str) -> str:
    """
    强制使用 PaddleOCR.predict()，避免 cls 参数问题。
    """
    ocr = _get_paddle(lang)
    img_np = np.array(img.convert("RGB"))
    result = ocr.predict(img_np)   # ✅ 固定走 predict，不再用 ocr()
    texts: List[str] = []
    for page in result:
        for line in page:
            txt = line[1][0]
            texts.append(txt)
    return _normalize_whitespace("\n".join(texts))

def extract_pdf_with_ocr(
    file_bytes: bytes,
    *,
    dpi: int = 200,
    ocr_lang: str = "ch",
) -> Dict:
    """
    与现有任务链兼容的最简版：
    返回 {"pages": [...], "full_text": str}
    - 每页：{"page_no", "text_blocks", "ocr_blocks", "merged_text"}
    """
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    all_pages: List[Dict] = []
    full_text_parts: List[str] = []

    for page_index in range(doc.page_count):
        page = doc.load_page(page_index)

        # 1) 可复制文本
        text = page.get_text("text") or ""
        text = _normalize_whitespace(text)

        # 2) 整页渲染 + OCR
        mat = fitz.Matrix(dpi / 72, dpi / 72)  # DPI 缩放
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        ocr_text = ocr_image(img, lang=ocr_lang)

        # 3) 合并：按行去重，保留新增
        merged = text
        if ocr_text:
            existing = set(line.strip() for line in text.splitlines() if line.strip())
            new_lines = [ln for ln in ocr_text.splitlines() if ln.strip() and ln.strip() not in existing]
            if new_lines:
                merged = (text + "\n" + "\n".join(new_lines)).strip()

        all_pages.append({
            "page_no": page_index + 1,
            "text_blocks": text,
            "ocr_blocks": ocr_text,
            "merged_text": merged,
        })
        full_text_parts.append(merged)

    return {
        "pages": all_pages,
        "full_text": "\n\n".join(full_text_parts).strip(),
    }