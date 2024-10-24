from typing import List

from pydantic import BaseModel


class Area(BaseModel):
    start: float
    finish: float


class PlanRequest(BaseModel):
    areas: List[Area]
    num_days: int
    is_resource_limit: bool
