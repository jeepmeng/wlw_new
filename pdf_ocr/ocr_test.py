# pdf_ocr/ocr_diag.py
import requests, io, os, sys
import fitz  # PyMuPDF
import numpy as np
from PIL import Image, ImageOps
from paddleocr import PaddleOCR

FILE_URL = "http://localhost:9001/api/v1/download-shared-object/aHR0cDovLzEyNy4wLjAuMTo5MDAwL3dsdy10ZXN0LyVFNSU4OCU5OCVFNCVCQiU5OCVFOCU4MSVBQSVFNSU4RCU5QSVFNSVBMyVBQiVFNiVBRiU5NSVFNCVCOCU5QSVFOCVBRiU4MSVFNiU4OSVBQiVFNiU4RiU4RiVFNCVCQiVCNi5wZGY_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1MWkMxNDBCMVZWTEtWR081RExDMSUyRjIwMjUwOTAxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDkwMVQxOTAyMTBaJlgtQW16LUV4cGlyZXM9NDMxOTkmWC1BbXotU2VjdXJpdHktVG9rZW49ZXlKaGJHY2lPaUpJVXpVeE1pSXNJblI1Y0NJNklrcFhWQ0o5LmV5SmhZMk5sYzNOTFpYa2lPaUpNV2tNeE5EQkNNVlpXVEV0V1IwODFSRXhETVNJc0ltVjRjQ0k2TVRjMU5qYzROek16TUN3aWNHRnlaVzUwSWpvaVlXUnRhVzRpZlEuTUF3VDVkdGZsOE04YWZ6WTZwYzFZeE1yWnJJUnNRYmRFYUhUSC13VEMwQ0YyVlFka0kxMnRnN1pvcjlDM1dtcG8tcWxvYTlDQzFwcEZOYzRvQzJmalEmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JnZlcnNpb25JZD1udWxsJlgtQW16LVNpZ25hdHVyZT0zYjNlYThiMDdkOTY3ZmExYzIzMGZmODgwZDU1YzFlYzUzNzI0MGFjN2VhYmM4ZjVhMmFmM2Q1OTAzNzUxODZi"
OUT_DIR = "ocr_diag_out"
os.makedirs(OUT_DIR, exist_ok=True)

def normalize(text: str) -> str:
    import re
    text = text.replace("\u00A0", " ")
    text = re.sub(r"[\t\r]+", " ", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()

def flatten(res):
    # 兼容不同返回结构，统一抽出文本
    texts = []
    if isinstance(res, dict):
        res = res.get("result", res.get("data", res))
    pages = res if isinstance(res, list) else [res]
    for page in pages:
        if isinstance(page, dict):
            page = page.get("data", page.get("result", []))
        if not isinstance(page, list):
            continue
        for line in page:
            # line: [box, (text, score)]
            if isinstance(line, (list, tuple)) and len(line) >= 2:
                val = line[1]
                if isinstance(val, (list, tuple)) and val:
                    txt = val[0]
                    if isinstance(txt, str) and txt.strip():
                        texts.append(txt.strip())
    return texts

def enhance(img: Image.Image) -> Image.Image:
    # 简单增强：转灰、自动对比度、轻度阈值
    gray = ImageOps.grayscale(img)
    gray = ImageOps.autocontrast(gray)
    # 自适应阈值（Otsu 的近似简化）
    np_img = np.array(gray)
    th = np_img.mean()
    bin_img = (np_img > th).astype(np.uint8) * 255
    return Image.fromarray(bin_img)

def main():
    print("→ 下载:", FILE_URL)
    r = requests.get(FILE_URL)
    r.raise_for_status()
    pdf_bytes = r.content
    print(f"✅ 下载完成，大小={len(pdf_bytes)/1024:.1f} KB")

    # 1) 先看是否本身就有可复制文本
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    print(f"📄 总页数: {doc.page_count}")
    text0 = normalize("".join([(doc.load_page(i).get_text("text") or "") for i in range(doc.page_count)]))
    print(f"【PyMuPDF 原生文本预览】:\n{(text0[:400] or '（空）')}\n")

    # 2) 初始化 OCR（新版参数，减少弃用警告）
    ocr = PaddleOCR(use_textline_orientation=True, lang="ch")

    DPI = 360   # 提高分辨率
    zoom = DPI / 72.0
    mat = fitz.Matrix(zoom, zoom)

    all_texts = []
    for i in range(doc.page_count):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        png_path = os.path.join(OUT_DIR, f"page_{i+1}_raw.png")
        with open(png_path, "wb") as f:
            f.write(pix.tobytes("png"))
        print(f"🖼️ 已保存渲染图: {png_path}  size={pix.width}x{pix.height}")

        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")

        # 3) 先跑 predict
        try:
            res_pred = ocr.predict(np.array(img))
            texts_pred = flatten(res_pred)
        except Exception as e:
            print(f"[WARN] predict 异常: {e}")
            texts_pred = []

        # 4) 再跑 ocr（兼容旧版）
        try:
            res_ocr = ocr.ocr(np.array(img))
            texts_ocr = flatten(res_ocr)
        except Exception as e:
            print(f"[WARN] ocr 异常: {e}")
            texts_ocr = []

        use_texts = texts_pred or texts_ocr
        print(f"· 第 {i+1}/{doc.page_count} 页：predict行数={len(texts_pred)} / ocr行数={len(texts_ocr)} / 采用={len(use_texts)}")

        # 5) 如果两者都空，做一次简单增强再试
        if not use_texts:
            enh = enhance(img)
            enh_path = os.path.join(OUT_DIR, f"page_{i+1}_enh.png")
            enh.save(enh_path)
            print(f"🧪 增强图已保存: {enh_path}")
            try:
                res_pred2 = ocr.predict(np.array(enh))
                texts_pred2 = flatten(res_pred2)
            except Exception as e:
                print(f"[WARN] predict(增强) 异常: {e}")
                texts_pred2 = []
            try:
                res_ocr2 = ocr.ocr(np.array(enh))
                texts_ocr2 = flatten(res_ocr2)
            except Exception as e:
                print(f"[WARN] ocr(增强) 异常: {e}")
                texts_ocr2 = []
            use_texts = texts_pred2 or texts_ocr2
            print(f"· 第 {i+1} 页增强后：predict行数={len(texts_pred2)} / ocr行数={len(texts_ocr2)} / 采用={len(use_texts)}")

        all_texts.extend(use_texts)
        img.close()

    full_text = normalize("\n".join(all_texts))
    print("\n===== OCR 文本预览（前 600 字）=====")
    print(full_text[:600] or "【空】")

    out_txt = os.path.join(OUT_DIR, "full_text.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(full_text)
    print(f"\n✅ 结果已保存: {out_txt}")

if __name__ == "__main__":
    sys.exit(main())