import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from fastapi import Request
from .sensitive_filter_ac import SensitiveFilterAC

CHECK_FIELDS = ("content","text","message","question","title","desc")

class InputBlocker(BaseHTTPMiddleware):
    def __init__(self, app, sf: SensitiveFilterAC):
        super().__init__(app)
        self.sf = sf

    async def dispatch(self, request: Request, call_next):
        if request.method in ("POST","PUT","PATCH"):
            ct = request.headers.get("content-type","")
            if ct.startswith("application/json"):
                raw = await request.body()
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    hits=[]
                    masked_obj = obj

                    def check(o):
                        nonlocal hits
                        if isinstance(o, str):
                            m, h = self.sf.mask(o)
                            if h: hits += h
                            return m
                        if isinstance(o, dict):
                            out = {}
                            for k,v in o.items():
                                if k in CHECK_FIELDS and isinstance(v, str):
                                    out[k], h = self.sf.mask(v)
                                    if h: hits += h
                                else:
                                    out[k] = v
                            return out
                        if isinstance(o, list):
                            return [check(x) for x in o]
                        return o

                    masked_obj = check(obj)
                    if hits:
                        return Response(
                            content=json.dumps({
                                "ok": False,
                                "reason": "含敏感词，已阻止发送",
                                "masked_input": masked_obj,
                                "hits": hits,
                                "policy_version": self.sf.version_tag
                            }, ensure_ascii=False),
                            status_code=400,
                            media_type="application/json"
                        )
                except Exception:
                    pass
        return await call_next(request)