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
        prd = (
            pd.concat([query.get_prd(start, finish) for start, finish in input_areas])
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

        fact = (
            pd.concat([query.get_fact(start, finish) for start, finish in input_areas])
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

        return PlanSources(
            prd=prd,
            fact=fact,
            contract=query.get_contract(),
            hierarchy=query.get_technology(),
            norms=query.get_norm(),
            resources=query.get_available_tech(),
        )


class TarantoolService:
    def __init__(self, data: PlanSources):
        self.operation_selector = OperationSelector(data)
        self.tech_require = TechRequire(data)
        self.cols = [
            "num_con",
            "operation_type",
            "start_p",
            "finish_p",
            "volume_p",
            "volume_f",
            "vol_remain",
            "hierarchy",
            "cost_remain",
            "sort_key",
        ]

    def _get_operations_plan(self, input_areas: List[List[int]]):
        opers = pd.concat([self.operation_selector.select(start, finish) for start, finish in input_areas]).reset_index(
            drop=True,
        )
        opers.loc[:, "sort_key"] = opers.index
        return opers[self.cols]

    def create_plan(self, input_areas: List[List[int]]):
        opers = self._get_operations_plan(input_areas)

        return {"columns": opers.columns.to_list(), "data": opers.to_numpy().tolist()}

    def create_plan_with_resource_constrain(self, input_areas: List[List[int]], num_days: int):
        opers = self._get_operations_plan(input_areas)
        req_workload = self.tech_require.require_workload(opers[["operation_type", "vol_remain", "sort_key"]])

        last_oper = self.tech_require.workload_constrain(req_workload, num_days)

        if not last_oper:
            return {
                "columns": opers.columns.to_list(),
                "data": opers.to_numpy().tolist(),
            }

        return {
            "columns": opers.columns.to_list(),
            "data": opers[opers["sort_key"] < last_oper].to_numpy().tolist(),
        }


if __name__ == "__main__":
    query = Query()

    areas = [[9775.42, 9800]]
    data = LoaderService.get_plan_source_data(areas)

    plan = TarantoolService(data).create_plan(areas)
