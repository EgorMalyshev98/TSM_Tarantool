from operator import index
from os import replace
from typing import List
import pandas as pd
from pprint import pprint
import numpy as np


def sort_works(works: np.ndarray):
    counter = 0
    for curr_inx, curr_row in enumerate(works):
        curr_row = works[curr_inx]
        prev_row = works[curr_inx - 1]
        
        if not curr_inx:
            works[curr_inx][4] = counter
            counter += 1
            continue
            
        p_finish, p_level = prev_row[2], prev_row[3]
        c_finish, c_level, sort_col = curr_row[2], curr_row[3], curr_row[4]
        
        if sort_col:
            continue
        
        if c_level > p_level and c_finish > p_finish:
            for n_inx, next_row in enumerate(works[curr_inx:]):
                n_start, n_level = next_row[1], next_row[3]
                if n_level == p_level and n_start < c_finish:
                    works[n_inx + curr_inx][4] = counter
                    works[curr_inx][4] = counter + 1
                    counter += 1
            counter += 1   
            continue
                
        works[curr_inx][4] = counter
        counter += 1
        
    return works


if __name__ == '__main__':
    
    data = \
        [['snt_prs', 33.0, 40.0, 1],
        ['ust_nasp', 33.0, 50.0, 2],
        ['ust_kv', 33.0, 50.0, 2],
        ['ust_geotxtl', 33.0, 45.0, 3],
        ['ust_pps', 33.0, 45.0, 4],
        ['snt_prs', 40.0, 45.0, 1],
        ['snt_prs', 45.0, 59.0, 1],
        ['snt_prs', 60.0, 70.0, 1],
        ['ust_geotxtl', 45.0, 59.0, 3],
        ['ust_pps', 45.0, 59.0, 4],
        ['ust_kv', 50.0, 59.0, 2],
        ]

    cols = ['operation_type', 'start_p', 'finish_p', 'hierarchy']

    df = pd.DataFrame(data, columns=cols).sort_values(['start_p', 'hierarchy'])
    
    df.loc[:, 'sort_col'] = None
    cols = df.columns.to_list()
    works = df.values
    sorted_ = sort_works(works)

    out_df = pd.DataFrame(sorted_, columns=cols).sort_values('sort_col')
    print(out_df)
    
