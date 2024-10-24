import pandas as pd
from dev_data_source import DataSources, data
from resources import TechRequire

from tarantool.services.operations import OperationSelector


class TarantoolService:
    pass


def main(data: DataSources):
    input_areas = [(33, 40), (40, 59)]
    num_days = 1
    is_resource_limit = True

    selector = OperationSelector(data.prd, data.fact, data.contract, data.hierarchy)
    plan_opers = pd.concat([selector.select(start, finish) for start, finish in input_areas]).reset_index(drop=True)
    plan_opers.loc[:, "sort_key"] = plan_opers.index

    tech = TechRequire(data.resources, data.norms)
    req_workload = tech.require_workload(plan_opers[["operation_type", "vol_remain", "sort_key"]])

    if is_resource_limit:
        last_oper = tech.workload_constrain(req_workload, num_days)
        plan_opers = plan_opers[plan_opers["sort_key"] < last_oper]

    return plan_opers


if __name__ == "__main__":
    pd.options.mode.copy_on_write = True

    main(data)
