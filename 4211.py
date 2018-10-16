import numpy as np
import pandas as pd

#1.1
# df = pd.read_csv("./4211 proj/dataport-export_gas_oct2015-mar2016.csv")
# data_id = df['dataid'].nunique()
# print("number of house =", data_id)

df = pd.read_csv("./short10000data.csv")

# sort data into order by id, if same id then by time
df = df.sort_values(["dataid", "localminute"], ascending=[True, True])
df = df.reset_index(drop = True)

# two type of malfunction
# 1: data reports back when change in gas use is < 2 cubic foot
# 2: data reports back new meter_value is smaller than old meter_value

malfunction = pd.DataFrame(columns = ["localminute", "dataid", "meter_value"])

# type 1 malfunction
for index in range(len(df['dataid']) - 1):
    if (index == 0):
        index += 1
    if ((df['meter_value'][index] <= df['meter_value'][index - 1]) 
        and (df['dataid'][index] == df['dataid'][index - 1])):
        malfunction.loc[df.index[index]] = df.iloc[index]

# type 2 malfunction
for index in range(len(df['dataid']) - 1):
    if (index == 0):
        index += 1
    if ((df['localminute'][index] != df['localminute'][index - 1]) 
        and ((df['meter_value'][index] - df['meter_value'][index - 1]) < 2)
        and (df['dataid'][index] == df['dataid'][index - 1])):
        malfunction.loc[df.index[index]] = df.iloc[index]
              
malfunction.drop_duplicates(keep = 'first')
malfunction = malfunction.reset_index(drop = True)

malfunction

# #how to make a dictionary 
# a = {test_df['dataid'][i] : {'start_time' : test_df['localminute'][i-1]}}
#         a[test_df['dataid'][i]].update({'end_time' : test_df['localminute'][j]})
#         ded_meter.update(a)
              
        
# pd.set_option('display.max_rows', 10000)

#1.2