# strong_ocr_final.py
# 依赖（Ubuntu 推荐最小集）：
#   pip install paddlepaddle==3.1.1 paddleocr==3.2.0 pymupdf pillow requests opencv-python
#   sudo apt-get update && sudo apt-get install -y libglib2.0-0 libsm6 libxrender1 libxext6
#
# 用法：
#   1) 修改下方 CONFIG 中的 pdf_url（MinIO 下载地址），或改为本地文件：file:///path/to/xxx.pdf
#   2) python strong_ocr_final.py
#
# 说明：
#   - 只用 PyMuPDF 渲染，无需装 poppler
#   - 结果会写到 ./ocr_output/{文件名_时间戳}/ 下的 page_*.txt 和 result.json
#   - 兼容 PaddleX 的 OCRResult：直接用 rec_texts / rec_scores / rec_boxes

import os
import io
import json
import time
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union

import requests
import fitz  # PyMuPDF
import numpy as np
import cv2
from PIL import Image
from paddleocr import PaddleOCR


# ============== CONFIG（按需修改）=============
pdf_url = "http://172.16.19.237:9001/api/v1/download-shared-object/aHR0cDovLzEyNy4wLjAuMTo5MDAwL3dsdy10ZXN0LyVFNSU4OCU5OCVFNCVCQiU5OCVFOCU4MSVBQSVFNSU4RCU5QSVFNSVBMyVBQiVFNiVBRiU5NSVFNCVCOCU5QSVFOCVBRiU4MSVFNiU4OSVBQiVFNiU4RiU4RiVFNCVCQiVCNi5wZGY_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1MWkMxNDBCMVZWTEtWR081RExDMSUyRjIwMjUwOTAyJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDkwMlQwMjA3MjhaJlgtQW16LUV4cGlyZXM9NDMyMDAmWC1BbXotU2VjdXJpdHktVG9rZW49ZXlKaGJHY2lPaUpJVXpVeE1pSXNJblI1Y0NJNklrcFhWQ0o5LmV5SmhZMk5sYzNOTFpYa2lPaUpNV2tNeE5EQkNNVlpXVEV0V1IwODFSRXhETVNJc0ltVjRjQ0k2TVRjMU5qYzROek16TUN3aWNHRnlaVzUwSWpvaVlXUnRhVzRpZlEuTUF3VDVkdGZsOE04YWZ6WTZwYzFZeE1yWnJJUnNRYmRFYUhUSC13VEMwQ0YyVlFka0kxMnRnN1pvcjlDM1dtcG8tcWxvYTlDQzFwcEZOYzRvQzJmalEmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JnZlcnNpb25JZD1udWxsJlgtQW16LVNpZ25hdHVyZT0wYTMyNDkxMDlmNjY3NWU1NTYzNTQwMTAxNWIyM2VlMTE5OTJlYjZhZWY1ODAxM2VlY2JjZWNlMmZkNGMyMTIy"    # ← MinIO 下载地址（带签名参数也可）
dpi = 350                                              # 渲染清晰度（300~400 常用）
only_first_page = False                                # True 仅识别第1页；False 识别整本
min_score = 0.35                                       # 文本置信度过滤阈值
upscale_factor = 1.6                                   # 失败后尝试放大倍数
bin_block_size = 35                                    # 二值化 blockSize（奇数）
bin_C = 10                                             # 二值化常数
preview_lines = 30                                     # 控制台预览行数
output_root = "./ocr_output"                           # 结果输出根目录
timeout_sec = 120                                      # 下载超时
# ============================================


def _ensure_dir(p: Union[str, Path]):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _now_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def download_pdf_to_temp(url: str, timeout: int = 120) -> str:
    """支持 http(s) 与 file:// 本地路径"""
    if url.startswith("file://"):
        return url.replace("file://", "", 1)

    fd, tmp_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    try:
        print(f"[DL] GET {url}")
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):  # 1MB
                    if chunk:
                        f.write(chunk)
        print(f"[DL] Saved -> {tmp_path}")
        return tmp_path
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def render_page_to_pil(doc: fitz.Document, page_idx: int, dpi: int = 300) -> Image.Image:
    page = doc[page_idx]
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    print(f"[PDF] Page {page_idx+1}/{doc.page_count} -> {img.width}x{img.height}")
    return img


def pil_to_rgb_array(img: Image.Image) -> np.ndarray:
    return np.array(img.convert("RGB"))


def upscale(arr_rgb: np.ndarray, scale: float) -> np.ndarray:
    h, w = arr_rgb.shape[:2]
    return cv2.resize(arr_rgb, (int(w*scale), int(h*scale)), interpolation=cv2.INTER_CUBIC)


def adaptive_binarize(arr_rgb: np.ndarray, block_size: int, C: int) -> np.ndarray:
    gray = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2GRAY)
    if block_size % 2 == 0:
        block_size += 1  # 必须奇数
    bin_img = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, block_size, C
    )
    return bin_img  # 单通道


def parse_ocr_result_blocks(blocks: List[Any]) -> List[Dict[str, Any]]:
    """
    解析 paddlex 的 OCRResult：
      - rec_texts: List[str]
      - rec_scores: List[float]
      - rec_boxes: np.ndarray (N,4,2)  四点坐标
    返回：[{text, score, box:[[x,y]x4]}...]
    """
    out = []
    for blk in blocks:
        rec_texts = getattr(blk, "rec_texts", None)
        rec_scores = getattr(blk, "rec_scores", None)
        rec_boxes = getattr(blk, "rec_boxes", None)

        if rec_texts is None or rec_scores is None:
            # 兜底：字典风格
            if isinstance(blk, dict):
                rec_texts = blk.get("rec_texts", [])
                rec_scores = blk.get("rec_scores", [])
                rec_boxes = blk.get("rec_boxes", None)

        if rec_texts is None or rec_scores is None:
            continue

        n = min(len(rec_texts), len(rec_scores))
        for i in range(n):
            t = str(rec_texts[i]) if rec_texts[i] is not None else ""
            s = float(rec_scores[i]) if rec_scores[i] is not None else 0.0
            if not t:
                continue
            item = {"text": t, "score": s}
            if rec_boxes is not None and len(rec_boxes) > i:
                # numpy -> python list
                try:
                    box = rec_boxes[i].tolist()
                    item["box"] = box
                except Exception:
                    pass
            out.append(item)
    return out


def ocr_predict_array(ocr: PaddleOCR, arr: np.ndarray) -> List[Dict[str, Any]]:
    """
    入参 arr 可以是 RGB(H,W,3) 或 灰度(H,W)；最终会确保传 RGB 给 paddle（避免 doc 预处理报错）。
    返回统一结构：[{text, score, box?}, ...]
    """
    if arr.ndim == 2:  # 灰度 -> RGB
        arr_rgb = cv2.cvtColor(arr, cv2.COLOR_GRAY2RGB)
    elif arr.ndim == 3 and arr.shape[2] == 3:
        arr_rgb = arr
    else:
        raise ValueError(f"Unsupported array shape: {arr.shape}")

    blocks = ocr.predict(arr_rgb)  # [OCRResult, ...]
    return parse_ocr_result_blocks(blocks or [])


def main():
    # 输出目录
    tag = _now_tag()
    out_dir = _ensure_dir(Path(output_root) / f"ocr_{tag}")
    out_json = out_dir / "result.json"

    # 初始化 OCR（新版替代 use_angle_cls）
    ocr = PaddleOCR(use_textline_orientation=True, lang="ch")

    # 下载到临时文件（本地 file:// 则直接返回路径）
    pdf_path = download_pdf_to_temp(pdf_url, timeout=timeout_sec)
    try:
        with fitz.open(pdf_path) as doc:
            if doc.page_count == 0:
                print("⚠️ PDF 无页面"); return

            page_indices = [0] if only_first_page else list(range(doc.page_count))
            all_pages: List[Dict[str, Any]] = []
            total_lines = 0

            for pidx in page_indices:
                pil_img = render_page_to_pil(doc, pidx, dpi=dpi)
                arr_rgb = pil_to_rgb_array(pil_img)

                # Pass-1：原图
                lines = ocr_predict_array(ocr, arr_rgb)
                # Pass-2：放大
                if not lines and upscale_factor and upscale_factor > 1.0:
                    arr_up = upscale(arr_rgb, upscale_factor)
                    lines = ocr_predict_array(ocr, arr_up)
                # Pass-3：二值化
                if not lines:
                    arr_bin = adaptive_binarize(arr_rgb, bin_block_size, bin_C)
                    lines = ocr_predict_array(ocr, arr_bin)

                # 过滤低分
                lines = [x for x in lines if x.get("score", 0) >= min_score]

                # 保存每页 txt
                page_txt = out_dir / f"page_{pidx+1:04d}.txt"
                with page_txt.open("w", encoding="utf-8") as f:
                    for item in lines:
                        f.write(item["text"] + "\n")

                # 控制台预览
                print("\n===== OCR 结果（预览）=====")
                if not lines:
                    print("（空）未识别到文本。可尝试提高 dpi / 调整放大与二值化参数。")
                else:
                    for i, item in enumerate(lines[:preview_lines], 1):
                        print(f"{i:02d}. {item['score']:.3f}  {item['text']}")
                    if len(lines) > preview_lines:
                        print(f"...（共 {len(lines)} 行，仅预览前 {preview_lines} 行）")

                total_lines += len(lines)
                all_pages.append({
                    "page_index": pidx,
                    "lines": lines
                })

            # 汇总 JSON
            with out_json.open("w", encoding="utf-8") as f:
                json.dump({
                    "pdf_url": pdf_url,
                    "dpi": dpi,
                    "only_first_page": only_first_page,
                    "min_score": min_score,
                    "pages": all_pages,
                    "total_lines": total_lines
                }, f, ensure_ascii=False, indent=2)

            print(f"\n✅ 完成：共识别 {total_lines} 行；结果目录：{out_dir.resolve()}")
    finally:
        # 自动清理临时 PDF（file:// 情况不删）
        if pdf_path and not pdf_url.startswith("file://") and os.path.exists(pdf_path):
            os.remove(pdf_path)
            print(f"[CLEANUP] Removed temp file: {pdf_path}")


if __name__ == "__main__":
    main()