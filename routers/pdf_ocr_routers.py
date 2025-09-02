# routers/pdf_ocr_routers.py
from fastapi import APIRouter, UploadFile, File, HTTPException
import io, os, threading, logging
import fitz
import numpy as np
from PIL import Image
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from paddleocr import PaddleOCR

router = APIRouter(prefix="/ocr", tags=["ocr"])
log = logging.getLogger("ocr_router")

# ---- 运行时稳定性参数（在导入 Paddle 后也可设置，但建议尽早设置）----
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_THREADING_LAYER", "SEQ")
# ------------------------------------------------------------------------

# 全局模型缓存 + 串行初始化锁
_OCR: Dict[str, PaddleOCR] = {}
_MODEL_LOCK = threading.Lock()

# 全局“单线程”执行器：所有 OCR 任务统一进这个线程，避免跟 FastAPI 线程池抢资源
_OCR_EXEC = ThreadPoolExecutor(max_workers=int(os.getenv("OCR_EXEC_WORKERS", "1")))

def get_ocr(lang: str) -> PaddleOCR:
    inst = _OCR.get(lang)
    if inst is None:
        with _MODEL_LOCK:
            inst = _OCR.get(lang)
            if inst is None:
                log.info(f"[OCR] init PaddleOCR(lang={lang}) ...")
                inst = PaddleOCR(use_angle_cls=True, lang=lang)
                _OCR[lang] = inst
                log.info(f"[OCR] PaddleOCR(lang={lang}) ready.")
    return inst

def _normalize(s: str) -> str:
    import re
    s = s.replace("\u00A0"," ")
    s = re.sub(r"[\t\r]+"," ", s)
    s = re.sub(r"\s+\n","\n", s)
    s = re.sub(r"\n{3,}","\n\n", s)
    s = re.sub(r"\s{2,}"," ", s)
    return s.strip()

# 真正干活的函数（同步、CPU 密集型），会跑在 _OCR_EXEC 的唯一线程里
def _ocr_pdf_bytes(pdf_bytes: bytes, lang: str, dpi: int) -> str:
    doc = None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)

        parts: List[str] = []
        ocr = get_ocr(lang)

        for i in range(doc.page_count):
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            # 注意：Pillow 打开后要显式 close，避免文件句柄泄漏
            img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            try:
                res = ocr.predict(np.array(img))
            finally:
                img.close()

            for page_res in res:
                for line in page_res:
                    txt = line[1][0]
                    if txt:
                        parts.append(txt)

        return _normalize("\n".join(parts))
    finally:
        # 释放 PDF 资源
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass

@router.post("/pdf")
def ocr_pdf(file: UploadFile = File(...), lang: str = "ch", dpi: int = 200, timeout_s: int = 300):
    """
    同步接口：FastAPI 会把它放到自己的线程池里，但我们内部又把 OCR 推给 _OCR_EXEC（单线程）去跑。
    这样可避免与默认线程池/OpenMP 线程打架，减少卡死/死锁。
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "only pdf supported")

    # 同步读取上传体（避免 async/await 与大文件交叉）
    pdf_bytes = file.file.read()
    if not pdf_bytes:
        raise HTTPException(400, "empty file")

    # 提交到全局单线程执行器
    future = _OCR_EXEC.submit(_ocr_pdf_bytes, pdf_bytes, lang, dpi)
    try:
        text = future.result(timeout=timeout_s)
    except FutureTimeout:
        # 线程被长时间占用（极大 PDF），客户端可重试或加大 timeout_s
        raise HTTPException(504, "ocr timeout")
    except Exception as e:
        log.exception(f"[OCR] failed: {e}")
        raise HTTPException(500, "ocr failed")

    return {"full_text": text}

# 暴露 _OCR 供 app2.py 预加载使用
__all__ = ["router", "_OCR"]