import pandas as pd
from fastapi import APIRouter, Depends
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

    is_res_limit = oper_plan.is_resource_limit

    data = LoaderService.get_plan_source_data(areas)  # TODO Dependency injection
    service = TarantoolService(data)  # TODO Dependency injection
    if is_res_limit:
        return service.create_plan_with_resource_constrain(areas, num_days)

    plan = service.create_plan(areas)
    print(data.prd, data.fact, pd.DataFrame(plan["data"], columns=plan["columns"]), sep="\n")

    return plan


@router.post("/upload/")
def load_data(table: UploadTable, user: User = Depends(current_verified_user), auth_header=Depends(auth_header)):
    UploadService.upload_table(table)

    return {"status": "ok"}
