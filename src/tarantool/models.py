from typing import Any, List

from pydantic import BaseModel


class Area(BaseModel):
    start: float
    finish: float


class PlanRequest(BaseModel):
    areas: List[Area]
    num_days: int
    is_resource_limit: bool


class PlanResponse(BaseModel):
    oper_plan: Any
    res_plan: Any
