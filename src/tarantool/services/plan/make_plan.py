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
            technology=query.get_technology(),
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
            "level",
            "cost_remain",
            "sort_key",
        ]

    def _get_operations_plan(self, input_areas: List[List[int]]):
        opers = pd.concat([self.operation_selector.select(start, finish) for start, finish in input_areas]).reset_index(
            drop=True,
        )
        opers.loc[:, "sort_key"] = opers.index
        return opers[self.cols]

    # def create_plan(self, input_areas: List[List[int]]):
    #     opers = self._get_operations_plan(input_areas)

    #     return {"columns": opers.columns.to_list(), "data": opers.to_numpy().tolist()}

    def create_plan(self, input_areas: List[List[int]], num_days: int):
        opers = self._get_operations_plan(input_areas)
        req_workload = self.tech_require.require_workload(opers)
        opers_with_res = self.tech_require.workload_constrain(req_workload, num_days)

        return {
            "columns": opers_with_res.columns.to_list(),
            "data": opers_with_res.to_numpy().tolist(),
        }


# if __name__ == "__main__":
#     areas = [[1500, 1600]]
#     data = LoaderService.get_plan_source_data(areas)
#     selector = OperationSelector(data)
#     start, finish = areas[0]

#     prd = data.prd.copy().merge(data.technology, how="left", on="operation_type")

#     point_objects = prd[prd["is_point_object"] is True].rename(
#         columns={"picket_start": "start_p", "picket_finish": "finish_p"}
#     )
#     base_operations = prd[prd["is_point_object"] is False]
#     base_operations = selector._select_pikets(start, finish, base_operations)
#     key_operations = base_operations[base_operations["is_key_oper"] is True].drop(columns="is_key_oper")
#     non_key_operations = base_operations[base_operations["is_key_oper"] is False].drop(columns="is_key_oper")

#     cols = ["level", "start_p", "finish_p"]
#     print(point_objects.columns, key_operations.columns, non_key_operations[cols], sep="\n")
