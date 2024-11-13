from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import APIKeyHeader

from src.auth.base_config import auth_header, current_verified_user
from src.auth.models import User
from src.tarantool.models import PlanRequest, UploadTable
from src.tarantool.services.plan.make_plan import LoaderService, TarantoolService
from src.tarantool.services.upload import UploadService

router = APIRouter(prefix="/tarantool")


@router.post("/plan/")
def plan(
    oper_plan: PlanRequest,
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    """Получить план работ на заданных участках"""
    areas = [[area.start, area.finish] for area in oper_plan.areas]
    num_days = oper_plan.num_days

    # is_res_limit = oper_plan.is_resource_limit

    try:
        data = LoaderService.get_plan_source_data(areas)  # TODO Dependency injection
        service = TarantoolService(data)  # TODO Dependency injection

        return service.create_plan(areas, num_days)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/upload/")
def load_data(table: UploadTable, user: User = Depends(current_verified_user), auth_header=Depends(auth_header)):
    UploadService.upload_table(table)

    return {"status": "ok"}
