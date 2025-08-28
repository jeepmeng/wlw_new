# app2.py（示例）
from fastapi import FastAPI
from sensitive.bootstrap import lifespan
from sensitive.middlewares import InputBlocker
from config.settings import settings

from routers.admin_sensitive import router as admin_sensitive_router
from routers.dialog_routers_2 import router as dialog_router
from routers.search_api import router as search_router
from routers.async_2_vector_api_merged import router as async_vector_router

app = FastAPI(lifespan=lifespan)

# 关键：此时应用还没启动，可以安全添加中间件
if settings.sensitive.enable_input_block:
    # 不传 sf/check_fields，运行期从 app.state 读取
    app.add_middleware(InputBlocker)



# 注册路由
app.include_router(async_vector_router)
app.include_router(dialog_router)
app.include_router(search_router)
app.include_router(admin_sensitive_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app2:app", host="0.0.0.0", port=8000, reload=True)