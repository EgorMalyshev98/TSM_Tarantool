from typing import List

import pandas as pd

from src.tarantool.models import PlanSources
from src.tarantool.repository import Query
from src.tarantool.services.plan.operations import OperationSelector
from src.tarantool.services.plan.resources import TechRequire


class LoaderService:
    @staticmethod
    def get_plan_source_data(input_areas: List[List[int]]) -> PlanSources:
        """получение данных из хранилища для формирования плана работ
        Args:
            input_areas (List[List[int]]): вводные участки: [[начало, окончание]]

        Returns:
            PlanSources
        """
        query = Query()

        return PlanSources(
            prd=pd.concat([query.get_prd(start, finish) for start, finish in input_areas]),
            fact=pd.concat([query.get_fact(start, finish) for start, finish in input_areas]),
            contract=query.get_contract(),
            hierarchy=query.get_technology(),
            norms=query.get_norm(),
            resources=query.get_available_tech(),
        )


class TarantoolService:
    def __init__(self, data: PlanSources):
        self.operation_selector = OperationSelector(data)
        self.tech_require = TechRequire(data)

    def _get_operations_plan(self, input_areas: List[List[int]]):
        opers = pd.concat([self.operation_selector.select(start, finish) for start, finish in input_areas]).reset_index(
            drop=True,
        )
        opers.loc[:, "sort_key"] = opers.index
        return opers

    def create_plan(self, input_areas: List[List[int]]):
        opers = self._get_operations_plan(input_areas)

        return {"columns": opers.columns.to_list(), "data": opers.to_numpy().tolist()}

    def create_plan_with_resource_constrain(self, input_areas: List[List[int]], num_days: int):
        opers = self._get_operations_plan(input_areas)
        req_workload = self.tech_require.require_workload(opers[["operation_type", "vol_remain", "sort_key"]])

        last_oper = self.tech_require.workload_constrain(req_workload, num_days)

        return {
            "columns": opers.columns.to_list(),
            "data": opers[opers["sort_key"] < last_oper].to_numpy().tolist(),
        }


if __name__ == "__main__":
    print(LoaderService.get_plan_source_data([[33, 59], [70, 80]]))
