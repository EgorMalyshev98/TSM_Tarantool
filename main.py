import pandas as pd
import numpy as np
from dataclasses import dataclass
from resources import Tech
from opertaions import OperationSelector
from pprint import pprint

from dev_data_source import DataSources, data



def main(data: DataSources):
    input_cost = 2000000
    
    input_areas = [(33, 40), (40, 59)]
    num_days = 1
    
    selector = OperationSelector(data.prd, data.fact, data.contract, data.hierarchy)
    
    plan_opers = [selector.select(start, finish) for start, finish in input_areas]
    opers = pd.concat(plan_opers).reset_index(drop=True)
    
    
if __name__ == '__main__':
    
    pd.options.mode.copy_on_write = True

    main(data)

