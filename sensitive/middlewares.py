# sensitive/middlewares.py
import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from fastapi import Request
from .sensitive_filter_ac import SensitiveFilterAC
from config.settings import settings  # 兜底拿 check_fields

class InputBlocker(BaseHTTPMiddleware):
    def __init__(self, app, sf: SensitiveFilterAC = None, check_fields: list[str] = None):
        super().__init__(app)
        self._sf = sf
        self._check_fields = tuple(check_fields or ())

    async def dispatch(self, request: Request, call_next):
        # 延迟获取：优先用构造参数，否则从 app.state 里取
        sf: SensitiveFilterAC = self._sf or getattr(request.app.state, "sf", None)
        if sf is None:
            # 过滤器尚未就绪，直接放行
            return await call_next(request)

        check_fields = self._check_fields or \
                       tuple(getattr(request.app.state, "sensitive_check_fields", ())) or \
                       tuple(getattr(settings.sensitive, "check_fields", ()))

        if request.method in ("POST","PUT","PATCH"):
            ct = request.headers.get("content-type","")
            if ct.startswith("application/json"):
                raw = await request.body()
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    hits = []

                    def mask_obj(o):
                        nonlocal hits
                        if isinstance(o, str):
                            masked, h = sf.mask(o)
                            if h: hits += h
                            return masked
                        if isinstance(o, dict):
                            out = {}
                            for k,v in o.items():
                                if k in check_fields and isinstance(v, str):
                                    out[k], h = sf.mask(v)
                                    if h: hits += h
                                else:
                                    out[k] = mask_obj(v)
                            return out
                        if isinstance(o, list):
                            return [mask_obj(x) for x in o]
                        return o

                    masked_obj = mask_obj(obj)
                    if hits:
                        return Response(
                            content=json.dumps({
                                "ok": False,
                                "reason": "含敏感词，已阻止发送",
                                "masked_input": masked_obj,
                                "hits": hits,
                                "policy_version": sf.version_tag
                            }, ensure_ascii=False),
                            status_code=400,
                            media_type="application/json"
                        )
                except Exception:
                    pass
        return await call_next(request)