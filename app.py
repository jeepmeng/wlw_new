from fastapi import FastAPI
from db_service.pg_pool import init_pg_pool, close_pg_pool
from routers.async_vector_api import router as async_vector_router
from routers.async_2_vector_api_merged import router as async_vector_router


app = FastAPI()

@app.on_event("startup")
async def startup():
    await init_pg_pool()

@app.on_event("shutdown")
async def shutdown():
    await close_pg_pool()


app.include_router(async_vector_router)

# app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)