from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import logging

router = APIRouter()

# 🪵 日志配置
logger = logging.getLogger("api")
logging.basicConfig(level=logging.INFO)

# ✅ 数据模型
class TextItem(BaseModel):
    id: Optional[str] = None
    content: str
    description: Optional[str] = None

class VectorItem(BaseModel):
    id: Optional[str] = None
    text: str
    vector: List[float]

class DialogItem(BaseModel):
    user_id: str
    question: str
    answer: str

class SearchQuery(BaseModel):
    query: str
    top_k: int = 10

class MixedSearchQuery(BaseModel):
    query: str
    top_k: int = 10
    alpha: float = 0.5

class ResponseModel(BaseModel):
    msg: str
    data: Optional[dict] = None

# 📄 文本接口
@router.post("/text/add", response_model=ResponseModel)
def add_text(item: TextItem):
    try:
        logger.info(f"Add text: {item}")
        return {"msg": "text added", "data": item.dict()}
    except Exception as e:
        logger.exception("Failed to add text")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/text/describe", response_model=ResponseModel)
def add_text_description(item: TextItem):
    try:
        logger.info(f"Update description: {item}")
        return {"msg": "description updated", "data": item.dict()}
    except Exception as e:
        logger.exception("Failed to update description")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/text/query", response_model=ResponseModel)
def query_text(id: str):
    try:
        logger.info(f"Query text id={id}")
        return {"msg": "text fetched", "data": {"id": id, "content": "示例文本"}}
    except Exception as e:
        logger.exception("Failed to query text")
        raise HTTPException(status_code=500, detail=str(e))

# 📌 向量接口
@router.post("/vector/add", response_model=ResponseModel)
def add_vector(item: VectorItem):
    try:
        logger.info(f"Add vector: {item.id}")
        return {"msg": "vector added", "data": {"id": item.id}}
    except Exception as e:
        logger.exception("Failed to add vector")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/vector/compare", response_model=ResponseModel)
def compare_vector(item: VectorItem):
    try:
        logger.info(f"Compare vector for id={item.id}")
        return {"msg": "similarity result", "data": {"score": 0.88}}
    except Exception as e:
        logger.exception("Failed to compare vector")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/vector/query", response_model=ResponseModel)
def vector_query(item: VectorItem):
    try:
        logger.info(f"Vector query for text: {item.text}")
        return {"msg": "topK vector search", "data": {"results": []}}
    except Exception as e:
        logger.exception("Failed vector query")
        raise HTTPException(status_code=500, detail=str(e))

# 💬 对话接口
@router.get("/dialog/history/{user_id}", response_model=ResponseModel)
def get_history(user_id: str):
    try:
        logger.info(f"Get dialog history for user {user_id}")
        return {"msg": "history fetched", "data": {"dialogs": []}}
    except Exception as e:
        logger.exception("Failed to get dialog history")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/dialog/add", response_model=ResponseModel)
def add_dialog(item: DialogItem):
    try:
        logger.info(f"Add dialog for user {item.user_id}")
        return {"msg": "dialog added"}
    except Exception as e:
        logger.exception("Failed to add dialog")
        raise HTTPException(status_code=500, detail=str(e))

# 🔍 检索接口
@router.get("/search/bm25", response_model=ResponseModel)
def search_bm25(query: str, top_k: int = 10):
    try:
        logger.info(f"BM25 search query: {query}")
        return {"msg": "bm25 search result", "data": {"results": []}}
    except Exception as e:
        logger.exception("Failed BM25 search")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/mix", response_model=ResponseModel)
def search_mix(item: MixedSearchQuery):
    try:
        logger.info(f"Mixed search: {item.query}, alpha={item.alpha}")
        return {"msg": "mixed search result", "data": {"results": []}}
    except Exception as e:
        logger.exception("Failed mixed search")
        raise HTTPException(status_code=500, detail=str(e))
