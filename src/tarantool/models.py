from dataclasses import dataclass
from typing import Any, List

import pandas as pd
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


class UploadTable(BaseModel):
    name: str
    columns: List[str]
    rows: List[List[Any]]


@dataclass(frozen=True)
class PlanSources:
    prd: pd.DataFrame
    fact: pd.DataFrame
    contract: pd.DataFrame
    norms: pd.DataFrame
    hierarchy: pd.DataFrame
    resources: pd.DataFrame
