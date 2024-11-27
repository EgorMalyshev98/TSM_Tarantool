from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from fastapi.security import APIKeyHeader

from src.auth.base_config import auth_header, current_verified_user
from src.auth.models import User
from src.database import session_manager
from src.tarantool.models import PlanRequest, UploadTable
from src.tarantool.services.plan.make_plan import LoaderService, TarantoolService
from src.tarantool.services.upload import UploadService

router = APIRouter(prefix="/tarantool")
upload_router = APIRouter(prefix="/upload")


@router.post("/plan/")
def plan(
    oper_plan: PlanRequest,
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    """Получить план работ на заданных участках"""

    areas = [[area.start, area.finish] for area in oper_plan.areas]
    num_days = oper_plan.num_days
    data = LoaderService.get_plan_source_data(areas)  # TODO Dependency injection
    service = TarantoolService(data)  # TODO Dependency injection

    return service.create_plan(areas, num_days)


@upload_router.post("/table/")
def load_data(
    session_id: Annotated[str, Body()],
    table: UploadTable,
    user: User = Depends(current_verified_user),
    auth_header=Depends(auth_header),
):
    session = session_manager.sessions.get(session_id, None)
    with session.connection.cursor() as cursor:
        UploadService.upload_table(table, cursor)

    return {"status": "ok"}


@upload_router.post("/clear-table/")
def clear_table(
    session_id: Annotated[str, Body()],
    table_name: Annotated[str, Body()],
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    session = session_manager.sessions.get(session_id, None)
    if not session:
        return HTTPException(404, "session not found")

    with session.connection.cursor() as cursor:
        UploadService.clear_table(table_name, cursor)
    return f"table {table_name} truncated"


@upload_router.post("/start/", response_description="return session id", response_class=PlainTextResponse)
def start_upload(
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    return session_manager.create()


@upload_router.post("/complete/", response_class=PlainTextResponse)
def complete_upload(
    session_id: Annotated[str, Body()],
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    session_manager.complete(session_id)
    return f"{session_id} complete"
