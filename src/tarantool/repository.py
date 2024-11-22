import io
from typing import Any, Optional

import pandas as pd
from fastapi import HTTPException
from psycopg import Connection, Cursor, sql
from psycopg2.errors import UndefinedColumn

from src.log import logger
from src.tarantool.models import UploadTable

OID_TYPES_MAP = {23: "int64", 1043: "string", 701: "float64", 16: "bool", 1082: "datetime64[ns]"}


class Statement:
    @staticmethod
    def truncate(table_name: str, cursor: Cursor):
        try:
            logger.debug(cursor.execute("SELECT 1"))
            stmt = sql.SQL("TRUNCATE TABLE {table};").format(table=sql.Identifier(table_name))
            logger.debug(stmt.as_string(cursor))
            cursor.execute(stmt)

        except Exception as e:
            logger.error(e)
            raise

    @staticmethod
    def batch_insert(table: UploadTable, cursor: Cursor):
        csv_buffer = io.StringIO()
        try:
            for row in table.rows:
                s_row = "\t".join(map(str, row)) + "\n"
                csv_buffer.write(s_row)

            csv_buffer.seek(0)

            stmt = sql.SQL("COPY {table}({columns}) FROM stdin (format csv, delimiter '\t', NULL '')").format(
                table=sql.Identifier(table.name), columns=sql.SQL(", ").join(map(sql.Identifier, table.columns))
            )

            with cursor.copy(stmt) as copy:
                copy.write(csv_buffer.read())

        except UndefinedColumn as e:
            logger.log(e)
            raise HTTPException(422, detail=e) from e

        except Exception as e:
            logger.error(e)
            raise


class Query:
    def __init__(self, db: Connection):  # TODO Dependency injection
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
                    WHERE picket_finish >= %(start)s
                )
                AND
                picket_start <= (
                    SELECT max(picket_start)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_start <= %(finish)s
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
                level,
                operation_type,
                construct_type,
                construct_name,
                is_key_oper,
                is_point_object
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
                    WHERE picket_finish >= %(start)s
                )
                AND
                picket_start <= (
                    SELECT max(picket_start)
                    FROM tarantool.dev_app__prd dap
                    WHERE picket_start <= %(finish)s
                )
        """
        params = {"start": start, "finish": finish}

        return self._get_data(query, params)

    def _get_data(self, query: str, query_params: Optional[dict[str, Any]] = None) -> pd.DataFrame:
        with self.db.cursor() as cur:
            result = cur.execute(query, query_params)
            data = result.fetchall()
            cols = [col_desc[0] for col_desc in result.description]
            col_types = [OID_TYPES_MAP[col_desc.type_code] for col_desc in result.description]
            d_types = dict(zip(cols, col_types))
            return pd.DataFrame(data, columns=cols).astype(d_types)
