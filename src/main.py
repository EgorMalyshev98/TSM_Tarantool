from fastapi import FastAPI

from src.auth.base_config import auth_backend, fastapi_users
from src.auth.schemas import UserCreate, UserRead
from src.tarantool.router import router as tarantool_router
from os import environ as env

app = FastAPI(title="TSM_Tarantool", root_path=env.get("ROOT_PATH"))

app.include_router(tarantool_router)
app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth",
    tags=["Auth"],
)
app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["Auth"],
)


@app.get("/health")
def health_check():
    return {"status": "healthy"}
