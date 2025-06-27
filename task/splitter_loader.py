from langchain_community.document_loaders import (
    PyPDFLoader, UnstructuredWordDocumentLoader, TextLoader,
    UnstructuredMarkdownLoader, CSVLoader, JSONLoader
)
from langchain.text_splitter import (
    CharacterTextSplitter, MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter
)

from pypdf.errors import PdfStreamError
from typing import Callable, Dict
from langchain_core.documents import Document
import pandas as pd
import json
from utils.logger_manager import get_logger  # ✅ 引入日志模块
from utils.task_utils import NonRetryableLoaderError

logger = get_logger("task_file_loader")

# ✅ Excel 文件处理函数：保留结构 + 日志
def load_excel_as_text(path: str) -> list:
    try:
        sheets = pd.read_excel(
            path,
            sheet_name=None,
            engine="openpyxl" if path.endswith(".xlsx") else "xlrd"
        )
        docs = []

        for name, df in sheets.items():
            df = df.dropna(how="all", axis=0).dropna(how="all", axis=1)
            if df.empty or df.shape[0] < 3:
                logger.info(f"📄 Excel 跳过空 Sheet：{name}")
                continue

            name_lower = name.lower()
            if any(kw in name_lower for kw in ["temp", "辅助", "hidden", "缓存", "系统"]):
                logger.info(f"📄 Excel 跳过无用 Sheet：{name}")
                continue

            try:
                markdown = df.to_markdown(index=False, tablefmt="grid")
                text = f"【Sheet: {name}】\n{markdown}"
                docs.append(Document(page_content=text))
            except Exception as e:
                logger.warning(f"⚠️ Sheet {name} 转换失败: {e}")
                docs.append(Document(page_content=f"⚠️ Sheet {name} 转换失败: {e}"))

        if not docs:
            msg = "⚠️ 所有 Sheet 被跳过或无有效内容"
            logger.warning(f"{path} - {msg}")
            return [Document(page_content=msg)]

        return docs

    except Exception as e:
        logger.exception(f"❌ Excel 读取失败: {e}")
        return [Document(page_content=f"❌ Excel 读取失败: {e}")]

# ✅ loader 封装器：统一异常日志
def safe_loader(loader_func: Callable[[str], any], path: str, ext: str) -> list:
    # try:
    #     return loader_func(path)
    # except Exception as e:
    #     logger.exception(f"❌ 文件加载失败: .{ext} - {path} - 错误: {e}")
    #     raise RuntimeError(f"文件加载失败: .{ext} - {e}")
    try:
        return loader_func(path)
    except PdfStreamError as e:
        # 明确这类错误不应触发 retry
        logger.error(f"❌ PDF 文件结构损坏，不重试: .{ext} - {path} - 错误: {e}")
        raise NonRetryableLoaderError(f"PDF 文件结构损坏: {e}")
    except Exception as e:
        logger.exception(f"❌ 文件加载失败: .{ext} - {path} - 错误: {e}")
        raise RuntimeError(f"文件加载失败: .{ext} - {e}")

# ✅ 文件加载器映射（加入异常包装）
LOADER_MAP: Dict[str, Callable[[str], any]] = {
    "pdf": lambda path: safe_loader(lambda p: PyPDFLoader(p).load(), path, "pdf"),
    "docx": lambda path: safe_loader(lambda p: UnstructuredWordDocumentLoader(p).load(), path, "docx"),
    "txt": lambda path: safe_loader(lambda p: TextLoader(p).load(), path, "txt"),
    "md": lambda path: safe_loader(lambda p: UnstructuredMarkdownLoader(p).load(), path, "md"),
    "csv": lambda path: safe_loader(lambda p: CSVLoader(file_path=p).load(), path, "csv"),
    "xlsx": lambda path: safe_loader(load_excel_as_text, path, "xlsx"),
    "xls": lambda path: safe_loader(load_excel_as_text, path, "xls"),
    "json": lambda path: safe_loader(load_json_with_auto_schema, path, "json"),
}

# ✅ 分割器智能映射函数（带异常日志）
def smart_split(ext: str, docs: list):
    try:
        if ext == "md":
            splitter = MarkdownHeaderTextSplitter(headers_to_split_on=[("#", 1), ("##", 2)])
            texts = [doc.page_content for doc in docs]
            chunks = []
            for text in texts:
                chunks.extend(splitter.split_text(text))  # ✅ 向后兼容
            return chunks

        elif ext in ["pdf", "docx", "txt"]:
            splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        else:
            splitter = CharacterTextSplitter(chunk_size=500, chunk_overlap=50)

        return splitter.split_documents(docs)

    except Exception as e:
        logger.exception(f"❌ 分段失败（.{ext} 文档），错误: {e}")
        return [Document(page_content=f"❌ 分段失败: {e}")]

# ✅ 分割器注册表
SPLITTER_MAP: Dict[str, Callable[[list], list]] = {
    ext: (lambda docs, ext=ext: smart_split(ext, docs))
    for ext in LOADER_MAP.keys()
}



def load_json_with_auto_schema(path: str):
    # 先读取原始内容，判断结构
    with open(path, 'r', encoding='utf-8') as f:
        content = json.load(f)

    # 动态选择 jq_schema
    if isinstance(content, list):
        # 是数组，尝试提取 content 字段
        jq_schema = ".[] | .content"
    elif isinstance(content, dict):
        # 是对象，直接提取 content
        jq_schema = ".content"
    else:
        # 是字符串或其他，直接返回原文
        jq_schema = "."

    # 使用动态 schema 加载文档
    return JSONLoader(file_path=path, jq_schema=jq_schema).load()