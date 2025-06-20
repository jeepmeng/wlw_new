from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter,CharacterTextSplitter

# 加载文档
loader = PyPDFLoader("/Users/liufucong/Downloads/PLC模拟器使用说明.pdf")
documents = loader.load()

# 切分文本
# splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)

# docs = splitter.split_documents(documents)

full_text = "\n".join([doc.page_content for doc in documents])
splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=100)
chunks = splitter.split_text(full_text)

# print(docs)


for chunk in chunks:
    print(chunk)
    # print("="*80)
