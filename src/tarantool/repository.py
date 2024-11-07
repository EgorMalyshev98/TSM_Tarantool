from typing import Any, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import get_session


class Query:
    def __init__(self, db: Session = next(get_session())):  # TODO Dependency injection
        self.db = db

    def get_prd(self, start: int, finish: int):
        query = """
            SELECT
                id,
                num_prd,
                operation_type,
                num_con,
                work_name,
                unit,
                picket_start,
                picket_finish,
                length,
                vol_prd
            FROM tarantool.dev_app__prd
            WHERE
                picket_finish >= (
                    SELECT min(picket_finish)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_finish >= :start
                )
                AND
                picket_start <= (
                    SELECT max(picket_start)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_start <= :finish
                )
        """
        params = {"start": start, "finish": finish}

        return self._get_data(query, params)

    def get_available_tech(self):
        query = """
            SELECT
                id,
                "date",
                technique_type,
                technique_name,
                quantity,
                shift_work
            FROM
                tarantool.dev_app__available_tech;
        """
        return self._get_data(query)

    def get_technology(self):
        query = """
            SELECT
                id,
                "hierarchy",
                operation_type,
                work_name,
                unit
            FROM
                tarantool.dev_app__technology;
        """
        return self._get_data(query)

    def get_contract(self):
        query = """
            SELECT
                id,
                num_con,
                work_name,
                unit,
                vol,
                price,
                "cost"
            FROM
                tarantool.dev_app__contract;
        """
        return self._get_data(query)

    def get_norm(self):
        query = """
                SELECT
                    id,
                    operation_type,
                    operation_name,
                    unit,
                    technique_type,
                    technique_name,
                    num_of_tech,
                    workload_1000_units
                FROM
                    tarantool.dev_app__norm;
        """
        return self._get_data(query)

    def get_fact(self, start: int, finish: int):
        query = """
            SELECT
                id,
                operation_type,
                ispol,
                num_con,
                work_name,
                unit,
                picket_start,
                picket_finish,
                length,
                vol_fact
            FROM tarantool.dev_app__fact

            WHERE
                picket_finish >= (
                    SELECT min(picket_finish)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_finish >= 33
                )
                AND
                picket_start <= (
                    SELECT max(picket_start)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_start <= 85
                )
        """
        params = {"start": start, "finish": finish}

        return self._get_data(query, params)

    def _get_data(self, query: str, query_params: Optional[dict[str, Any]] = None) -> pd.DataFrame:
        query = text(query)
        result = self.db.execute(query, query_params)
        data = result.all()
        cols = result.keys()

        self.db.rollback()

        return pd.DataFrame(data, columns=cols)
