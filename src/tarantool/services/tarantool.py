from typing import List

import pandas as pd

from src.tarantool.services.dev_data_source import DataSources, data
from src.tarantool.services.operations import OperationSelector
from src.tarantool.services.resources import TechRequire


class TarantoolService:
    def __init__(self):
        self.operation_selector = OperationSelector(data.prd, data.fact, data.contract, data.hierarchy)
        self.tech_require = TechRequire(data.resources, data.norms)

    def _load_data(self, input_areas: List[List[int]]) -> DataSources:
        # TODO Database query logic
        return data

    def _get_operations_plan(self, input_areas: List[List[int]]):
        opers = pd.concat([self.operation_selector.select(start, finish) for start, finish in input_areas]).reset_index(
            drop=True,
        )
        opers.loc[:, "sort_key"] = opers.index
        return opers

    def create_plan(self, input_areas: List[List[int]]):
        opers = self._get_operations_plan(input_areas)

        return {"columns": opers.columns.to_list(), "data": opers.to_json(orient="values")}

    def create_plan_with_resource_constraint(self, input_areas: List[List[int]], num_days: int):
        opers = self._get_operations_plan(input_areas)
        req_workload = self.tech_require.require_workload(opers[["operation_type", "vol_remain", "sort_key"]])

        last_oper = self.tech_require.workload_constrain(req_workload, num_days)

        return {
            "columns": opers.columns.to_list(),
            "data": opers[opers["sort_key"] < last_oper].to_json(orient="values"),
        }
