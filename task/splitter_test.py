# test_splitter.py
import sys
from splitter_loader import LOADER_MAP, SPLITTER_MAP
from pathlib import Path


def test_document_split(file_path: str):
    ext = Path(file_path).suffix[1:].lower()
    if ext not in LOADER_MAP:
        print(f"❌ 不支持的文件类型: .{ext}")
        return

    print(f"✅ 加载并分割文件: {file_path}")
    loader = LOADER_MAP[ext]
    splitter = SPLITTER_MAP[ext]

    docs = loader(file_path)
    print(f"📄 文档总段数: {len(docs)} (未切割)")

    chunks = splitter(docs)
    print(f"✂️ 切割后段数: {len(chunks)}")

    # for i, chunk in enumerate(chunks[:5]):
    #     print("\n------------------")
    #     print(f"段 {i+1}:\n{chunk.page_content[:300]}...")
    for i, chunk in enumerate(chunks):
        print(f"\n段 {i + 1}:")
        print(chunk.page_content)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python test_splitter.py <文件路径>")
    else:
        test_document_split(sys.argv[1])