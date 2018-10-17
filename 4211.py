import numpy as np
import pandas as pd
from ipywidgets import FloatProgress
from IPython.display import display
import timeit
start_time = timeit.default_timer()

#1.1

# df = pd.read_csv("./4211 proj/dataport-export_gas_oct2015-mar2016.csv")
# data_id = df['dataid'].nunique()
# print("number of house =", data_id)

df = pd.read_csv("./short100data.csv")

# Sort data into order by id, if same id then by time
df = df.sort_values(["dataid", "localminute"], ascending=[True, True])
df = df.reset_index(drop = True)

# two type of malfunction
# 1: data reports back when change in gas use is < 2 cubic foot
# 2: data reports back new meter_value is smaller than old meter_value

# Progress bar cuz I always feel it is not working
f = FloatProgress(min=0, max=(len(df['dataid']) - 1))
display(f)
f2 = FloatProgress(min=0, max=2)
display(f2)

# Create empty df and dictionary
malfunction = pd.DataFrame(columns = ["localminute", "dataid", "meter_value"])
bad_array = []

prev_good = True
_id = None
prev_time = None
# Assumption: the first data point is always correct as the 2nd pt is wrt to it, 3rd wrt to 2nd....etc
# Find malfunction of type 1 and 2
# Sort malfunction with time period label
# Append malfunction period to array
for row in df.itertuples():
    if(_id is None or _id != row.dataid):
        prev_good = True
        prev_time = row.localminute
        _id = row.dataid
    # for estimating time take to process actual data
    try:
        if (row.Index == 0):
            continue
        if (((row.meter_value <= df.meter_value[row.Index-1]) 
             and (row.dataid == df.dataid[row.Index-1])) 
        or ((row.localminute != df.localminute[row.Index-1]) 
            and (row.dataid == df.dataid[row.Index-1]) 
            and ((row.meter_value - df.meter_value[row.Index-1]) < 2))):
            malfunction.loc[row.Index] = row[1:4]
            if(prev_good == True):
                # if wan time start from 1st bad pt, then change prev_time to row.localminute
                bad_array.append([_id, prev_time, row.localminute])
            else:
                bad_array[-1][2] = row.localminute
            prev_good = False
            prev_time = row.localminute
        else:
            prev_good = True
            prev_time = row.localminute            
        f.value+=1
    except KeyboardInterrupt:
        print(f.value)

elapsed = timeit.default_timer() - start_time
print("%ds" %(elapsed))

# #how to make a dictionary 
# a = {test_df['dataid'][i] : {'start_time' : test_df['localminute'][i-1]}}
#         a[test_df['dataid'][i]].update({'end_time' : test_df['localminute'][j]})
#         ded_meter.update(a)

# Cast to int64
malfunction['dataid'] = malfunction['dataid'].astype(np.int64)
malfunction['meter_value'] = malfunction['meter_value'].astype(np.int64)

malfunction.reset_index(drop = True, inplace = True)
f2.value+=1

# Merge 2 df to compare diff, if one of the label value is different,
# value in _merge label will be different hence being able to differeniate the difference
df = pd.merge(df, malfunction, on=['localminute', 'dataid', 'meter_value'], how='outer', indicator=True)\
.query("_merge != 'both'")\
.drop(['_merge'], axis=1)\
.reset_index(drop=True)
f2.value+=1

malfunction = pd.DataFrame(bad_array, columns = ['dataid', 'start_time', 'end_time'])
malfunction.T

df.to_csv('./clean_df.csv', index = False)
malfunction.to_csv('./malfunction.csv', index = False)

        
# pd.set_option('display.max_rows', 10000)

elapsed = timeit.default_timer() - start_time - elapsed
print("%ds" %(elapsed))
print("total: %ds" %(timeit.default_timer() - start_time))

#1.2