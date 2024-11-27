from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator


class Area(BaseModel):
    start: float
    finish: float

    @field_validator("start", "finish")
    @classmethod
    def value_check(cls, v: float):
        if v < 0:
            raise ValueError("Пикет не должнен быть отрицательным")
        return v

    @model_validator(mode="after")
    def check_range(self):
        if self.finish <= self.start:
            raise ValueError("Окончание участка должно быть больше, чем начало")
        return self


class PlanRequest(BaseModel):
    areas: List[Area]
    num_days: int = Field(gt=0, description="Кол-во планируемых дней должно быть больше 0")

    @model_validator(mode="after")
    def check_areas(self):
        self.areas = self._set_valid_areas(self.areas)
        return self

    @staticmethod
    def _set_valid_areas(areas: List[Area]) -> List[Area]:
        valid_areas = []
        areas.sort(key=lambda x: x.start)

        for i, area in enumerate(areas):
            if not i:
                valid_areas.append(area)
                continue

            pre_area = areas[i - 1]

            if pre_area.finish > area.finish:
                continue
            if pre_area.finish > area.start:
                valid_areas.append(Area(start=pre_area.finish, finish=area.finish))
                continue

            valid_areas.append(area)

        return valid_areas


class PlanResponse(BaseModel):
    oper_plan: Any
    res_plan: Any


class UploadTable(BaseModel):
    name: str
    columns: List[str]
    rows: List[List[Any]]


@dataclass(frozen=True)
class PlanSources:
    prd: Dict[str, pd.DataFrame]
    fact: Dict[str, pd.DataFrame]
    contract: pd.DataFrame
    norms: pd.DataFrame
    technology: pd.DataFrame
    resources: pd.DataFrame
