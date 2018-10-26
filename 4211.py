import numpy as np
import pandas as pd
from ipywidgets import FloatProgress
from IPython.display import display
import timeit
from datetime import timedelta
from pathlib import Path
# start_time = timeit.default_timer() * 1000
currentPath = str(Path().resolve())
# Put raw csv in same 
path = currentPath + "./csv/dataport-export_gas_oct2015-mar2016.csv"

# Read in raw data and find 
df = pd.read_csv(path)
data_id = df['dataid'].nunique()
print("Number of houses =", data_id)

# Read in raw data and find 
df = pd.read_csv(path)
data_id = df['dataid'].nunique()
print("Number of houses =", data_id)

# Sort data into order by id, if same id then by time
df.sort_values(["dataid", "localminute"], ascending=[True, True], inplace = True)
df.reset_index(drop = True, inplace = True)

# Cast localminute into proper time format for further processing
df.localminute = df.localminute.str.slice(0,19)
df.localminute = pd.to_datetime(df.localminute, infer_datetime_format = True, format = "%Y/%m/%d %I:%M:%S %p")
print(df.localminute[0])
print("Length of df:", len(df))

# Types of malfunction
# 1: data reported back when change in gas use is < 2 cubic foot
# 2: data reported back new meter_value is smaller than old meter_value
# 3: data reported back is >2, but time > 15s, threshold value for reporting malfunction will be 7 cubic foot/hr 
# (US household average daily usage = 168 cubic foot)
# 4: However, continuous report of same reading from same id over 12hrs,
# the first reading after 12hrs will be treated as good reading,
# as the gas company will probably want to know whether is a meter malfunctioning
# even though its reading did not change for a period of time

# Progress bar cuz I always feel it is not working
# f = FloatProgress(min=0, max=(len(df['dataid']) - 1))
# display(f)

# Create empty df and array for more efficient removal of item in df
malfunction = pd.DataFrame(columns = ["localminute", "dataid", "meter_value"])
bad_array = []
to_drop = []

prev_good = True
_id = None
_12hr = None
# Sort malfunction with time period label
# Assumption: the first data point is always correct as the 2nd pt is wrt to it, 3rd wrt to 2nd....etc
# Append malfunction period and data to respective array
for row in df.itertuples():
    if(_id is None or _id != row.dataid):
        prev_good = True
        _id = row.dataid
    # for estimating time take to process actual data
    if (row.Index == 0):
        continue
    #1 and #2
    if (((row.meter_value <= df.meter_value[row.Index-1])
         and (row.dataid == df.dataid[row.Index-1])) 
        or ((row.localminute != df.localminute[row.Index-1]) 
            and (row.dataid == df.dataid[row.Index-1]) 
            and ((row.meter_value - df.meter_value[row.Index-1]) < 2))):
        if(prev_good == True):
            bad_array.append([_id, row.localminute, row.localminute])
            to_drop.append(row.Index)
            #4, update 12hr pt
            _12hr = row.localminute
        else:
            #4  
            if ((row.localminute - _12hr) >= pd.to_timedelta("12:00:00")):
                prev_good = True
                continue
            else:
                bad_array[-1][2] = row.localminute
                to_drop.append(row.Index)
        prev_good = False
    else:
        prev_good = True
        #3
        if(((row.meter_value - df.meter_value[row.Index-1]) > 2) 
           and ((((row.localminute - df.localminute[row.Index-1]) / timedelta(hours = 1)) * 7) 
                > (row.meter_value - df.meter_value[row.Index - 1]))):
            bad_array[-1][2] = row.localminute
print("Length of bad_array:", len(bad_array))
print("Length of to_drop:", len(to_drop))

#Remove malfunction data to produce clean df
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

# Convert bad_array into df and transform it for shape to be correct
malfunction = pd.DataFrame(bad_array, columns = ['dataid', 'start_time', 'end_time'])
malfunction.T

# Save part1 result into csv for easier access in part2
df.to_csv('./clean_df.csv', index = False)
malfunction.to_csv('./malfunction.csv', index = False)

# pd.set_option('display.max_rows', 10000)

# elapsed = timeit.default_timer() * 1000 - start_time
# print("total: %ds" %(elapsed/1000)) if ((elapsed > 5000) == True) else print("total: %dms" %elapsed)
print("length of df:", len(df))
print("length of malfunction:", len(malfunction))

import pandas as pd
import numpy as np
import datetime as dt
from collections import namedtuple

gas_data = pd.read_csv('./csv/clean_df.csv')
len(gas_data)

gas_data.localminute = gas_data.localminute.str.slice(0, 19)
gas_data.localminute = pd.to_datetime(gas_data.localminute,
        infer_datetime_format=True, format='%Y/%m/%d %I:%M:%S %p')
gas_data.localminute = gas_data.localminute.map(lambda x: \
        x.replace(minute=0, second=0))

# gas_data=gas_data[gas_data['dataid']==35];

_hr = dt.timedelta(hours=1)
temp_gas_hr = pd.DataFrame(columns=gas_data.columns)
temp_gas_hr = gas_data
id_list = gas_data['dataid'].unique()
ind = 0

# temp_row=namedtuple('temp_row',gas_data.columns)

for _id in id_list:

    ind = ind + 1

    temp_gas_data = gas_data[gas_data['dataid'] == _id]
    temp_gas_data.reset_index(drop=True, inplace=True)
    for (index, row) in temp_gas_data.iterrows():
        if index == 0:
            prev_row = row.copy()
        else:
            time_diff = row.localminute - prev_row.localminute
            if time_diff > _hr:
                time_diff = int(time_diff.total_seconds() / 3600)
                for j in range(1, time_diff):
                    time_change = dt.timedelta(hours=j)
                    new_time = prev_row.localminute + time_change
                    temp_row.localminute = new_time
                    temp_gas_hr = temp_gas_hr.append(temp_row)

        prev_row = row.copy()
        temp_row = prev_row.copy()

temp_gas_hr.drop_duplicates(['localminute', 'dataid'], keep='last',
                            inplace=True)

temp_gas_hr = temp_gas_hr.sort_values(by=['dataid', 'localminute'])
temp_gas_hr.reset_index(drop=True, inplace=True)
temp_gas_hr.to_csv('hourly_readings_final.csv', index = False)
