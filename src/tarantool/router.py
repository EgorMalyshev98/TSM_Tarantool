from fastapi import APIRouter

from tarantool.models import PlanRequest
from tarantool.services.tarantool import TarantoolService

router = APIRouter(prefix="/tarantool")


@router.post("/plan/")
def plan(oper_plan: PlanRequest):
    """Получить план работ на заданных участках"""

    areas = [[area.start, area.finish] for area in oper_plan.areas]
    num_days = oper_plan.num_days
    is_res_limit = oper_plan.is_resource_limit

    if is_res_limit:
        return TarantoolService().create_plan_with_resource_constraint(areas, num_days)

    return TarantoolService().create_plan(areas)
