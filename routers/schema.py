from typing import Optional, List, Dict, Any
from pydantic import BaseModel,HttpUrl, Field
from uuid import UUID

# ✅ 请求数据模型
class VectorItem(BaseModel):
    id: Optional[str] = None
    text: str


class ResponseModel(BaseModel):
    msg: str
    data: Optional[dict] = None


# 插入向量记录
class InsertVectorItem(BaseModel):
    zhisk_file_id: int
    content: str
    vector: List[float]
    jsons: Dict[str, Any]
    code: str
    category: str
    uu_id: str


# 批量插入问题
class InsertQuesBatchItem(BaseModel):
    uu_id: str
    sentences: List[str]
    vectors: List[List[float]]


# 更新记录字段
class UpdateByIdItem(BaseModel):
    update_data: Dict[str, Any]
    record_id: int
    updata_ques: List[str]
    up_ques_vect: List[List[float]]



class FileMeta(BaseModel):
    url: str
    filename: str
    file_id: str
    user_id: str

class FileBatchRequest(BaseModel):
    files: List[FileMeta]

    # === 新增：是否启用 PDF OCR（不判断扩展名；收到 True 就尝试按 PDF OCR 解析）===
    enable_pdf_ocr: Optional[bool] = False
    # === 可选 OCR 参数（不传采用默认/环境值）===
    ocr_lang: Optional[str] = "ch"
    ocr_dpi: Optional[int] = 200




class WriteQuesBatch(BaseModel):
    uu_id: str
    sentences: list[str]
    vectors: list[list[float]]



class SearchRequest(BaseModel):
    query: str
    use_bm25: bool = True
    use_vector: bool = True
    alpha: float = 0.6  # 权重


class ScoreDetail(BaseModel):
    bm25: float
    vector: float

class SearchResult(BaseModel):
    id: str
    content: str
    score: float
    score_detail: Dict[str, float]
    source: str  # "hybrid", "bm25", "vector"



class StartDialogRequest(BaseModel):
    user_id: str
    title: Optional[str] = None
    provider: Optional[str] = "deepseek"

class AskRequest(BaseModel):
    user_id: str
    question: str
    provider: Optional[str] = "deepseek"  # "deepseek" | "qwen"
    enable_search: Optional[bool] = True  # 仅对 qwen 生效

class ControlRequest(BaseModel):
    user_id: str
    action: str  # stop | pause | resume | resend
    message_id: Optional[UUID] = None



class ChunkUpdateRequest(BaseModel):
    chunk_id: str
    new_content: str
    update_by: str  # 新增字段


class QuestionUpdateRequest(BaseModel):
    question_id: str
    new_question: str




class QueryZhiskFilesRequest(BaseModel):
    page: int = Field(default=1, ge=1, description="当前页码，从1开始")
    page_size: int = Field(default=10, ge=1, le=100, description="每页数据条数")
    sort_field: Optional[str] = Field(default="create_time", description="排序字段")
    sort_order: Optional[str] = Field(default="desc", description="排序方式：asc 或 desc")


class QueryByFileIdRequest(BaseModel):
    zhisk_file_id: str = Field(..., description="目标文件对应的 zhisk_file_id")


class UUIDQueryRequest(BaseModel):
    uuids: List[str] = Field(..., description="待查询记录的 UUID 列表")