from typing import List

import pandas as pd

from src.database import pool
from src.tarantool.models import PlanSources
from src.tarantool.repository import Query
from src.tarantool.services.plan.operations import OperationSelector
from src.tarantool.services.plan.resources import TechRequire


class LoaderService:
    @staticmethod
    def _set_works_dict(areas: List[List[int]], df: pd.DataFrame):
        areas_dict = {}

        for start, finish in areas:
            mask = (df.input_start == start) & (df.input_fin == finish)
            local_df = df[mask].drop(columns=["input_start", "input_fin"])
            areas_dict[(start, finish)] = local_df

        return areas_dict

    @classmethod
    def get_plan_source_data(cls, input_areas: List[List[int]]) -> PlanSources:
        """получение данных из хранилища для формирования плана работ
        Args:
            input_areas (List[List[int]]): вводные участки: [[начало, окончание]]

        Returns:
            PlanSources
        """
        with pool.connection() as conn:
            query = Query(conn)

            prd_dict = {(start, finish): query.get_prd(start, finish) for start, finish in input_areas}
            fact_dict = {(start, finish): query.get_fact(start, finish) for start, finish in input_areas}

            return PlanSources(
                prd=prd_dict,
                fact=fact_dict,
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
            "unit",
            "construct_type",
            "construct_name",
            "work_name",
            "is_key_oper",
            "is_point_object",
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
            "data": opers_with_res.fillna("").to_numpy().tolist(),
        }


if __name__ == "__main__":
    areas = [[0, 5], [5, 10]]
    data = LoaderService.get_plan_source_data(areas)
    selector = OperationSelector(data)
    start, finish = areas[0]

    df = selector.select(start, finish)

    print(df.describe())
