from fastapi import APIRouter, Depends
from fastapi.security import APIKeyHeader

from src.auth.base_config import auth_header, current_verified_user
from src.auth.models import User
from src.log import logger
from src.tarantool.models import PlanRequest
from src.tarantool.services.plan.make_plan import LoaderService, TarantoolService

router = APIRouter(prefix="/tarantool")


@router.post("/plan/")
def plan(
    oper_plan: PlanRequest,
    user: User = Depends(current_verified_user),
    auth_header: APIKeyHeader = Depends(auth_header),
):
    """Получить план работ на заданных участках"""
    logger.debug("рассчитываем план")
    areas = [[area.start, area.finish] for area in oper_plan.areas]
    num_days = oper_plan.num_days

    is_res_limit = oper_plan.is_resource_limit

    data = LoaderService.get_plan_source_data(areas)  # TODO Dependency injection
    service = TarantoolService(data)  # TODO Dependency injection

    if is_res_limit:
        return service.create_plan_with_resource_constrain(areas, num_days)

    return service.create_plan(areas)


@router.post("/upload/")
def load_data(user: User = Depends(current_verified_user), auth_header=Depends(auth_header)):
    return {"data": "batch"}
