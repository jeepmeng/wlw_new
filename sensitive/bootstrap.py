# sensitive/bootstrap.py（只保留构建与注入）
from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from .sensitive_filter_ac import SensitiveFilterAC
from config.settings import settings

@asynccontextmanager
async def lifespan(app):
    es_user = (settings.elasticsearch.username or "").strip()
    es_pass = (settings.elasticsearch.password or "").strip()
    es = AsyncElasticsearch(settings.elasticsearch.host,
                            basic_auth=(es_user, es_pass)) if es_user else AsyncElasticsearch(settings.elasticsearch.host)

    sf = SensitiveFilterAC(
        es=es,
        index=settings.elasticsearch.indexes.sensitive_index,
        ignore_case=settings.sensitive.ignore_case,
        page_size=settings.sensitive.page_size,
        max_single_fetch=settings.sensitive.max_single_fetch,
    )
    n = await sf.refresh()
    print(f"[sensitive] loaded {n} terms, version={sf.version_tag}, max_len={sf.max_pat_len}")

    # 注入给运行期使用
    app.state.es = es
    app.state.sf = sf
    app.state.sensitive_index = settings.elasticsearch.indexes.sensitive_index
    app.state.ignore_case = settings.sensitive.ignore_case
    app.state.sensitive_check_fields = settings.sensitive.check_fields

    try:
        yield
    finally:
        await es.close()