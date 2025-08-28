from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from .sensitive_filter_ac import SensitiveFilterAC
from .middlewares import InputBlocker
from ..settings import settings

@asynccontextmanager
async def lifespan(app):
    es = AsyncElasticsearch(settings.es_url, basic_auth=(settings.es_user, settings.es_pass))
    sf = SensitiveFilterAC(
        es=es,
        index=settings.sensitive_index,
        ignore_case=settings.ignore_case,
        page_size=settings.page_size,
        max_single_fetch=settings.max_single_fetch,
    )
    n = await sf.refresh()
    print(f"[sensitive] loaded {n} terms, version={sf.version_tag}, max_len={sf.max_pat_len}")

    app.state.es = es
    app.state.sf = sf
    app.state.sensitive_index = settings.sensitive_index
    app.state.ignore_case = settings.ignore_case

    app.add_middleware(InputBlocker, sf=sf)
    try:
        yield
    finally:
        await es.close()