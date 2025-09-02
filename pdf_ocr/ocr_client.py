# pdf_ocr/ocr_client.py
# -*- coding: utf-8 -*-
"""
轻量 OCR 客户端（对接独立 OCR 微服务）
- 同步 HTTP 调用：/ocr/pdf:sync、/ocr/image:sync
- 默认返回 full_text；也可返回完整结构（pages/total_lines/full_text）
- 内置重试、超时、可选 Bearer Token 鉴权

依赖：
    pip install requests
"""

from __future__ import annotations

import io
import time
from typing import Any, Dict, Optional, Tuple

import requests


__all__ = [
    "OCRClientError",
    "ocr_pdf_bytes",
    "ocr_image_bytes",
]


class OCRClientError(Exception):
    """OCR 客户端调用失败异常（包含可选的 HTTP 状态码与请求 URL）"""

    def __init__(self, message: str, status_code: Optional[int] = None, url: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url = url

    def __str__(self) -> str:
        base = f"OCRClientError: {self.message}"
        if self.status_code is not None:
            base += f" (HTTP {self.status_code})"
        if self.url:
            base += f" [URL={self.url}]"
        return base


# ===================== 内部工具 =====================

def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    tail = path.lstrip("/")
    return f"{base}/{tail}"


def _default_headers(token: Optional[str] = None, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": "ocr-client/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _post_multipart(
    url: str,
    filename: str,
    data_bytes: bytes,
    fields: Dict[str, Any],
    *,
    content_type: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 120,
    verify: bool = True,
    max_retries: int = 2,
    backoff_base: float = 0.5,
) -> Dict[str, Any]:
    """
    以 multipart/form-data 方式上传二进制，并带表单字段。
    - 指数退避重试：网络异常/5xx 时重试；4xx 不重试
    - 返回：解析后的 JSON（dict），异常时抛 OCRClientError
    """
    files = {
        # requests 会自动设置 multipart 边界与 Content-Type
        "file": (filename, io.BytesIO(data_bytes), content_type),
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.post(url, data=fields, files=files, headers=headers, timeout=timeout, verify=verify)
        except requests.RequestException as e:
            # 仅网络/超时类异常触发重试
            if attempt <= max_retries:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue
            raise OCRClientError(f"HTTP 请求异常：{e}", status_code=None, url=url)

        # 非 2xx：决定是否重试
        if not (200 <= resp.status_code < 300):
            # 5xx 可重试；4xx 不重试
            if 500 <= resp.status_code < 600 and attempt <= max_retries:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue
            # 抛出详细错误
            text = (resp.text or "").strip()
            raise OCRClientError(
                f"服务端错误：{text[:512]}",
                status_code=resp.status_code,
                url=url,
            )

        # 成功：解析 JSON
        try:
            return resp.json()
        except ValueError:
            raise OCRClientError("响应不是合法 JSON", status_code=resp.status_code, url=url)


# ===================== 对外主函数 =====================

def ocr_pdf_bytes(
    base_url: str,
    pdf_bytes: bytes,
    *,
    lang: str = "ch",
    dpi: int = 350,
    min_score: float = 0.35,
    upscale_factor: float = 1.6,
    bin_block_size: int = 35,
    bin_C: int = 10,
    timeout: int = 120,
    token: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    verify_tls: bool = True,
    join_pages: bool = True,
    max_retries: int = 2,
) -> str | Dict[str, Any]:
    """
    调用 /ocr/pdf:sync
    - join_pages=True：返回整篇文本 full_text（string）
    - join_pages=False：返回完整结构 dict：{pages,total_lines,full_text,status}
    """
    url = _join_url(base_url, "/ocr/pdf:sync")
    headers = _default_headers(token, extra_headers)
    fields = {
        "dpi": str(dpi),
        "min_score": str(min_score),
        "upscale_factor": str(upscale_factor),
        "bin_block_size": str(bin_block_size),
        "bin_C": str(bin_C),
        "lang": lang,
    }

    data = _post_multipart(
        url=url,
        filename="file.pdf",
        data_bytes=pdf_bytes,
        fields=fields,
        content_type="application/pdf",
        headers=headers,
        timeout=timeout,
        verify=verify_tls,
        max_retries=max_retries,
    )

    if data.get("status") != "ok":
        # 远端约定：status != ok 视为业务失败
        raise OCRClientError(f"OCR 失败：{str(data)[:512]}", status_code=None, url=url)

    if join_pages:
        return data.get("full_text", "") or ""
    return data


def ocr_image_bytes(
    base_url: str,
    image_bytes: bytes,
    *,
    lang: str = "ch",
    min_score: float = 0.35,
    upscale_factor: float = 1.6,
    bin_block_size: int = 35,
    bin_C: int = 10,
    timeout: int = 120,
    token: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    verify_tls: bool = True,
    join_lines: bool = True,
    max_retries: int = 2,
    filename_hint: str = "image.bin",
    mime_hint: str = "application/octet-stream",
) -> str | Dict[str, Any]:
    """
    调用 /ocr/image:sync
    - join_lines=True：返回整图文本 full_text（string）
    - join_lines=False：返回完整结构 dict：{pages:[{lines: [...]}], total_lines, full_text, status}
    """
    url = _join_url(base_url, "/ocr/image:sync")
    headers = _default_headers(token, extra_headers)
    fields = {
        "min_score": str(min_score),
        "upscale_factor": str(upscale_factor),
        "bin_block_size": str(bin_block_size),
        "bin_C": str(bin_C),
        "lang": lang,
    }

    data = _post_multipart(
        url=url,
        filename=filename_hint,
        data_bytes=image_bytes,
        fields=fields,
        content_type=mime_hint,
        headers=headers,
        timeout=timeout,
        verify=verify_tls,
        max_retries=max_retries,
    )

    if data.get("status") != "ok":
        raise OCRClientError(f"OCR 失败：{str(data)[:512]}", status_code=None, url=url)

    if join_lines:
        return data.get("full_text", "") or ""
    return data


# ===================== 便捷封装（可选） =====================

def call_ocr_by_ext(
    base_url: str,
    raw_bytes: bytes,
    ext: str,
    *,
    lang: str = "ch",
    dpi: int = 350,
    timeout: int = 120,
    token: Optional[str] = None,
    verify_tls: bool = True,
) -> Tuple[str, bool]:
    """
    根据扩展名自动分流到 pdf 或 image。
    返回：(full_text, used_ocr=True/False)
    - 未识别类型会抛 OCRClientError；由上层捕获后自行回退 LOADER_MAP。
    """
    e = (ext or "").lower().strip(".")
    if e == "pdf":
        text = ocr_pdf_bytes(
            base_url, raw_bytes, lang=lang, dpi=dpi, timeout=timeout, token=token, verify_tls=verify_tls
        )
        return text, True

    if e in {"jpg", "jpeg", "png", "bmp", "tif", "tiff"}:
        text = ocr_image_bytes(
            base_url, raw_bytes, lang=lang, timeout=timeout, token=token, verify_tls=verify_tls
        )
        return text, True

    raise OCRClientError(f"不支持的 OCR 扩展名: .{ext}", status_code=None, url=None)


# ===================== 用法示例（注释） =====================
# from pdf_ocr.ocr_client import ocr_pdf_bytes, OCRClientError
# try:
#     txt = ocr_pdf_bytes("http://127.0.0.1:8000", pdf_bytes, lang="ch", dpi=350)
# except OCRClientError as e:
#     logger.error(f"OCR 调用失败：{e}")
#     # → 回退到原 LOADER_MAP 流程