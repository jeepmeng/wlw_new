from typing import Optional, List, Dict, Any
from pydantic import BaseModel,HttpUrl


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

class FileBatchRequest(BaseModel):
    files: List[FileMeta]






class WriteQuesBatch(BaseModel):
    uu_id: str
    sentences: list[str]
    vectors: list[list[float]]