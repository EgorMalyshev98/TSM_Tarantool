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
            "input_start",
            "input_finish",
            "global_level",
        ]
        self.out_cols_sequence = [
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
            "technique_type",
            "require_workload",
            "id",
            "date",
            "technique_name",
            "workload_limit",
            "cum_workload",
            "global_level",
            "input_start",
            "input_finish",
        ]

    def _get_operations_plan(self, input_areas: List[List[int]]):
        opers = pd.concat([self.operation_selector.select(start, finish) for start, finish in input_areas]).reset_index(
            drop=True,
        )
        opers.loc[:, "sort_key"] = opers.index
        return opers[self.cols]

    @staticmethod
    def _first(df: pd.DataFrame):
        df.iloc[1:] = pd.NA
        return df

    @classmethod
    def _drop_duplicates(cls, df: pd.DataFrame, group_cols: List[str], drop_dub_cols: List[str]) -> pd.DataFrame:
        df.loc[:, drop_dub_cols] = df.groupby(group_cols)[drop_dub_cols].transform(cls._first)
        return df

    @staticmethod
    def _set_point_obj_start_finish(df: pd.DataFrame):
        df.loc[df.is_point_object, ["input_start", "input_finish"]] = df[df.is_point_object][
            ["start_p", "finish_p"]
        ].to_numpy()

        return df

    def create_plan(self, input_areas: List[List[int]], num_days: int):
        opers = self._get_operations_plan(input_areas)
        req_workload = self.tech_require.require_workload(opers)
        opers_with_res = (
            self.tech_require.workload_constrain(req_workload, num_days)
            .pipe(
                self._drop_duplicates,
                group_cols=["sort_key"],
                drop_dub_cols=["volume_p", "volume_f", "vol_remain", "cost_remain"],
            )
            .pipe(self._set_point_obj_start_finish)
            .loc[:, self.out_cols_sequence]
        )

        return {
            "columns": opers_with_res.columns.to_list(),
            "data": opers_with_res.fillna("").to_numpy().tolist(),
        }


if __name__ == "__main__":
    areas = [[5, 10]]
    data = LoaderService.get_plan_source_data(areas)
    service = TarantoolService(data)

    plan = service.create_plan(areas, 1)
