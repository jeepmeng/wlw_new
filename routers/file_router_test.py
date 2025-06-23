from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import Callable, Dict
from langchain_community.document_loaders import (
    PyPDFLoader, UnstructuredWordDocumentLoader, TextLoader,
    UnstructuredMarkdownLoader, CSVLoader, UnstructuredExcelLoader, JSONLoader
)
from langchain.text_splitter import CharacterTextSplitter
import os, tempfile

app = FastAPI()

# ============================
# ✅ 注册表：后缀 -> 加载器 & 分割器
# ============================

# 加载器注册表
LOADER_MAP: Dict[str, Callable[[str], any]] = {
    "pdf": lambda path: PyPDFLoader(path).load(),
    "docx": lambda path: UnstructuredWordDocumentLoader(path).load(),
    "txt": lambda path: TextLoader(path).load(),
    "md": lambda path: UnstructuredMarkdownLoader(path).load(),
    "csv": lambda path: CSVLoader(file_path=path).load(),
    "xlsx": lambda path: UnstructuredExcelLoader(path).load(),
    "xls": lambda path: UnstructuredExcelLoader(path).load(),
    "json": lambda path: JSONLoader(path).load(),
}

# 分割器注册表（统一使用字符分割器，可按需优化）
SPLITTER_MAP: Dict[str, Callable[[list], list]] = {
    ext: (lambda docs: CharacterTextSplitter(chunk_size=500, chunk_overlap=50).split_documents(docs))
    for ext in LOADER_MAP.keys()
}

# ============================
# ✅ 主接口
# ============================

@app.post("/upload_and_parse")
async def upload_and_parse(file: UploadFile = File(...)):
    ext = file.filename.split(".")[-1].lower()

    if ext not in LOADER_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: .{ext}")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        raw_docs = LOADER_MAP[ext](tmp_path)
        chunks = SPLITTER_MAP[ext](raw_docs)

        return {
            "filename": file.filename,
            "chunks": [chunk.page_content for chunk in chunks]
        }
    finally:
        os.remove(tmp_path)
