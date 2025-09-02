from fastapi import APIRouter, UploadFile, File, HTTPException
import io, fitz, numpy as np
from PIL import Image
from typing import Dict, List
from paddleocr import PaddleOCR
import logging

router = APIRouter(prefix="/ocr", tags=["ocr"])
log = logging.getLogger("ocr_router")

# 全局缓存
_OCR: Dict[str, PaddleOCR] = {}

__all__ = ["router", "_OCR"]
def get_ocr(lang: str) -> PaddleOCR:
    return _OCR[lang]

def ocr_predict(img: Image.Image, lang: str) -> List[str]:
    ocr = get_ocr(lang)
    res = ocr.predict(np.array(img.convert("RGB")))
    texts = []
    for page in res:
        for line in page:
            txt = line[1][0]
            if txt:
                texts.append(txt)
    return texts

def _normalize(s: str) -> str:
    import re
    s = s.replace("\u00A0", " ")
    s = re.sub(r"[\t\r]+", " ", s)
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

@router.post("/pdf")
async def ocr_pdf(file: UploadFile = File(...), lang: str = "ch", dpi: int = 200):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "only pdf supported")

    pdf_bytes = await file.read()
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        raise HTTPException(400, "invalid pdf")

    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)

    try:
        parts: List[str] = []
        for i in range(doc.page_count):
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            res = ocr_predict(img, lang=lang)
            parts.extend(res)
    except Exception as e:
        log.exception(f"[OCR] route crashed: {e}")
        raise HTTPException(500, "ocr failed")

    return {"full_text": _normalize("\n".join(parts))}