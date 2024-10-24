from fastapi import FastAPI

from tarantool.router import router as tarantool_router

app = FastAPI(title="TSM_Tarantool", root_path="/ver1")

app.include_router(tarantool_router)
