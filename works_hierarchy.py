from main import OperationSelector
import pandas as pd
import numpy as np  



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
df = pd.DataFrame(data, columns=cols).sort_values(['start_p', 'hierarchy']).reset_index(drop=True)

df.loc[:, 'sort_key'] = OperationSelector._add_sort_key(df[['start_p', 'finish_p', 'hierarchy']])

print()