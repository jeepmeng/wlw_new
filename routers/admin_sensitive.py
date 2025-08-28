# routers/admin_sensitive.py
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from sensitive.sensitive_filter_ac import SensitiveFilterAC

router = APIRouter(prefix="/admin/sensitive", tags=["sensitive-admin"])

# ---------- helpers ----------
def _now_iso() -> str:
    # 统一用 UTC ISO8601，避免 ES 映射为 date 时的解析歧义
    return datetime.now(timezone.utc).isoformat()

# ---------- DI from app.state ----------
def get_es(request: Request) -> AsyncElasticsearch:
    es = getattr(request.app.state, "es", None)
    if es is None:
        raise HTTPException(500, "ES not initialized")
    return es

def get_sf(request: Request) -> SensitiveFilterAC:
    sf = getattr(request.app.state, "sf", None)
    if sf is None:
        raise HTTPException(500, "Sensitive filter not initialized")
    return sf

def get_index_name(request: Request) -> str:
    idx = getattr(request.app.state, "sensitive_index", None)
    if not idx:
        raise HTTPException(500, "Index name not set")
    return idx

def get_ignore_case(request: Request) -> bool:
    return bool(getattr(request.app.state, "ignore_case", False))

# ---------- schemas ----------
class TermIn(BaseModel):
    term: str
    category: str = "default"
    is_active: bool = True
    norm: Optional[str] = None
    severity: Optional[int] = None
    source: Optional[str] = None

# ---------- routes ----------
@router.post("/terms")
async def add_term(
    body: TermIn,
    es: AsyncElasticsearch = Depends(get_es),
    sf: SensitiveFilterAC = Depends(get_sf),
    index_name: str = Depends(get_index_name),
    ignore_case: bool = Depends(get_ignore_case),
):
    doc: Dict[str, Any] = body.model_dump()
    # 规范化
    doc["term"] = (doc.get("term") or "").strip()
    doc["term_text"] = doc["term"]                     # 用于 search_as_you_type
    if ignore_case:
        doc["norm"] = (doc.get("norm") or doc["term"]).lower()
    doc.setdefault("is_active", True)
    doc.setdefault("updated_at", _now_iso())

    resp = await es.index(index=index_name, document=doc, refresh="wait_for")
    n = await sf.refresh()
    return {"ok": True, "id": resp["_id"], "count": n, "version": sf.version_tag}


@router.put("/terms/{doc_id}")
async def update_term(
    doc_id: str,
    body: TermIn,
    es: AsyncElasticsearch = Depends(get_es),
    sf: SensitiveFilterAC = Depends(get_sf),
    index_name: str = Depends(get_index_name),
    ignore_case: bool = Depends(get_ignore_case),
):
    doc: Dict[str, Any] = body.model_dump()
    # 规范化
    doc["term"] = (doc.get("term") or "").strip()
    doc["term_text"] = doc["term"]
    if ignore_case:
        doc["norm"] = (doc.get("norm") or doc["term"]).lower()
    doc["updated_at"] = _now_iso()

    # ES 8 async client：update 的 doc 参数会包装成 {"doc": doc}
    await es.update(index=index_name, id=doc_id, doc=doc, refresh="wait_for")
    n = await sf.refresh()
    return {"ok": True, "id": doc_id, "count": n, "version": sf.version_tag}


@router.delete("/terms/{doc_id}")
async def delete_term(
    doc_id: str,
    es: AsyncElasticsearch = Depends(get_es),
    sf: SensitiveFilterAC = Depends(get_sf),
    index_name: str = Depends(get_index_name),
):
    # 软删除：is_active = false
    await es.update(
        index=index_name,
        id=doc_id,
        doc={"is_active": False, "updated_at": _now_iso()},
        refresh="wait_for",
    )
    n = await sf.refresh()
    return {"ok": True, "id": doc_id, "count": n, "version": sf.version_tag}


@router.get("/terms")
async def list_terms(
    from_: int = Query(0, alias="from"),
    size: int = Query(50, ge=1, le=500),
    es: AsyncElasticsearch = Depends(get_es),
    index_name: str = Depends(get_index_name),
):
    body = {
        "from": from_,
        "size": size,
        "sort": [{"updated_at": "desc"}, {"_id": "asc"}],
        "query": {"match_all": {}},
    }
    resp = await es.search(index=index_name, body=body)
    total = resp["hits"]["total"]["value"] if isinstance(resp["hits"]["total"], dict) else resp["hits"]["total"]
    return {"total": total, "items": resp["hits"]["hits"]}


@router.get("/search")
async def search_terms(
    q: str,
    size: int = Query(20, ge=1, le=200),
    es: AsyncElasticsearch = Depends(get_es),
    index_name: str = Depends(get_index_name),
):
    # 基于 search_as_you_type 的前缀/近似搜索
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": q,
                "type": "bool_prefix",
                "fields": ["term_text", "term_text._2gram", "term_text._3gram"],
            }
        },
    }
    resp = await es.search(index=index_name, body=body)
    return {"items": resp["hits"]["hits"]}


@router.post("/refresh")
async def force_refresh(sf: SensitiveFilterAC = Depends(get_sf)):
    n = await sf.refresh()
    return {"ok": True, "count": n, "version": sf.version_tag}