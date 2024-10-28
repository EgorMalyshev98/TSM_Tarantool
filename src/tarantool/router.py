from fastapi import APIRouter, Depends
from fastapi.security import APIKeyHeader

from src.auth.base_config import auth_header, current_verified_user
from src.auth.models import User
from src.log import logger
from src.tarantool.models import PlanRequest
from src.tarantool.services.tarantool import TarantoolService

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

    if is_res_limit:
        return TarantoolService().create_plan_with_resource_constraint(areas, num_days)

    return TarantoolService().create_plan(areas)
