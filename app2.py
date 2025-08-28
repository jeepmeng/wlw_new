from fastapi import FastAPI
from sensitive.bootstrap import lifespan
from routers.admin_sensitive import router as admin_sensitive_router
from routers.dialog_routers import router as dialog_router
from routers.search_api import router as search_router
from routers.async_2_vector_api_merged import router as async_vector_router

app = FastAPI(lifespan=lifespan)

# 注册你原有路由
app.include_router(async_vector_router)
app.include_router(dialog_router)
app.include_router(search_router)
# 注册敏感词管理
app.include_router(admin_sensitive_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)