import pandas as pd

data_1 = [[1, "a"], [2, "b"]]

data_2 = [[1, "a"], [3, "c"]]

df1 = pd.DataFrame(data_1)
df2 = pd.DataFrame(data_2)

print(pd.concat([df1, df2]))
