import numpy as np
import pandas as pd
from ipywidgets import FloatProgress
from IPython.display import display
import timeit
start_time = timeit.default_timer()
pd.options.mode.chained_assignment = None 

#1.1

df = pd.read_csv("./4211 proj/dataport-export_gas_oct2015-mar2016.csv")
# data_id = df['dataid'].nunique()
# print("number of house =", data_id)

# df = pd.read_csv("./short10000data.csv")
# df = pd.read_csv('./200k.csv')

# Sort data into order by id, if same id then by time
df.sort_values(["dataid", "localminute"], ascending=[True, True], inplace = True)
df.reset_index(drop = True, inplace = True)

df.localminute = df.localminute.str.slice(0,19)
df.localminute = pd.to_datetime(df.localminute, infer_datetime_format = True, format = "%Y/%m/%d %I:%M:%S %p")

# two type of malfunction
# 1: data reports back when change in gas use is < 2 cubic foot
# 2: data reports back new meter_value is smaller than old meter_value
# However, continuous report of same reading from same id over 12hrs,
# the first reading after 12hrs will be treated as good reading

# Progress bar cuz I always feel it is not working
# f = FloatProgress(min=0, max=(len(df['dataid']) - 1))
# display(f)

# Create empty df and dictionary
malfunction = pd.DataFrame(columns = ["localminute", "dataid", "meter_value"])
bad_array = []
to_drop = []

prev_good = True
_id = None
_12hr = None
# Sort malfunction with time period label
# Assumption: the first data point is always correct as the 2nd pt is wrt to it, 3rd wrt to 2nd....etc
# Find malfunction of type 1 and 2
# Append malfunction period and data to respective array
for row in df.itertuples():
    if(_id is None or _id != row.dataid):
        prev_good = True
        _id = row.dataid
    # for estimating time take to process actual data
    if (row.Index == 0):
        continue
    if (((row.meter_value <= df.meter_value[row.Index-1])
         and (row.dataid == df.dataid[row.Index-1])) 
        or ((row.localminute != df.localminute[row.Index-1]) 
            and (row.dataid == df.dataid[row.Index-1]) 
            and ((row.meter_value - df.meter_value[row.Index-1]) < 2))):
        if(prev_good == True):
            bad_array.append([_id, row.localminute, row.localminute])
            to_drop.append(row.Index)
            # update 12hr pt
            _12hr = row.localminute
        else:
            if ((row.localminute - _12hr) >= pd.to_timedelta("12:00:00")):
                prev_good = True
                continue
            else:
                bad_array[-1][2] = row.localminute
                to_drop.append(row.Index)
        prev_good = False
    else:
        prev_good = True 

#Remove some values to speed up the time, those removed time are kept tracked in bad_array
df.drop(index = to_drop, inplace = True)
df.reset_index(drop = True, inplace = True)

# # Cast to int64
# malfunction['dataid'] = malfunction['dataid'].astype(np.int64)
# malfunction['meter_value'] = malfunction['meter_value'].astype(np.int64)

# # Merge 2 df to compare diff, if one of the label value is different,
# # value in _merge label will be different hence being able to differeniate the difference
# df = pd.merge(df, malfunction, on=['localminute', 'dataid', 'meter_value'], how='outer', indicator=True)\
# .query("_merge != 'both'")\
# .drop(['_merge'], axis=1)\
# .reset_index(drop=True)

# Make bad_array into df and transform it for shape to be correct
malfunction = pd.DataFrame(bad_array, columns = ['dataid', 'start_time', 'end_time'])
malfunction.T

df.to_csv('./clean_df.csv', index = False)
malfunction.to_csv('./malfunction.csv', index = False)

# pd.set_option('display.max_rows', 10000)

print("total: %ds" %(timeit.default_timer() - start_time))

#1.2