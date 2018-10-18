import numpy as np
import pandas as pd
from ipywidgets import FloatProgress
from IPython.display import display
import timeit
start_time = timeit.default_timer()
pd.options.mode.chained_assignment = None 

#1.1

# df = pd.read_csv("./4211 proj/dataport-export_gas_oct2015-mar2016.csv")
# data_id = df['dataid'].nunique()
# print("number of house =", data_id)

df = pd.read_csv("./short100data.csv")
# df = pd.read_csv('./200k.csv')

# Sort data into order by id, if same id then by time
# Remove id with same meter reading as they are spoiled
df.sort_values(["dataid", "localminute"], ascending=[True, True], inplace = True)
# df.drop_duplicates(subset = ['dataid', 'meter_value'], keep = 'first', inplace = True)
df.reset_index(drop = True, inplace = True)


# two type of malfunction
# 1: data reports back when change in gas use is < 2 cubic foot
# 2: data reports back new meter_value is smaller than old meter_value

# Progress bar cuz I always feel it is not working
# f = FloatProgress(min=0, max=(len(df['dataid']) - 1))
# display(f)

# Create empty df and dictionary
malfunction = pd.DataFrame(columns = ["localminute", "dataid", "meter_value"])
bad_array = []

prev_good = True
_id = None
prev_time = None
_12hr = None
# Sort malfunction with time period label
# Append malfunction period to array
for row in df.itertuples():
    if(_id is None or _id != row.dataid):
        prev_good = True
        prev_time = row.localminute
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
        else:
            bad_array[-1][2] = row.localminute
        prev_good = False
        prev_time = row.localminute
    else:
        prev_good = True
        prev_time = row.localminute            
#     f.value+=1
# insert before array append after if(prev_good == True)
# #############################################################
# if (row.localminute - _12hr >= 12):
#     bad_array.append([_id, row.localminute, row.localminute])
#     prev_good = True
#     prev_time = row.localminute
# _12hr = row.localminute
# #############################################################


print("Bad array: %ds" %(timeit.default_timer() - start_time))


#Remove some values to speed up the time, those removed time are kept tracked in bad_array
df.drop_duplicates(subset = ['dataid', 'meter_value'], keep = 'first', inplace = True)
df.reset_index(drop = True, inplace = True)

temp_index_holder = []

# Assumption: the first data point is always correct as the 2nd pt is wrt to it, 3rd wrt to 2nd....etc
# Find malfunction of type 1 and 2
#######################################
# malfunction.loc[row.Index] = row[1:4] 
#######################################
for row in df.itertuples():
    # for estimating time take to process actual data
    if (row.Index == 0):
        continue
    if (((row.meter_value <= df.meter_value[row.Index-1]) 
         and (row.dataid == df.dataid[row.Index-1])) 
        or ((row.localminute != df.localminute[row.Index-1]) 
            and (row.dataid == df.dataid[row.Index-1]) 
            and ((row.meter_value - df.meter_value[row.Index-1]) < 2))):
        temp_index_holder.append(row.Index)    
    
print("Copy to malfunction: %ds" %(timeit.default_timer() - start_time))

df.drop(index = temp_index_holder, inplace = True)
df.reset_index(drop = True, inplace = True)

# # Cast to int64
# malfunction['dataid'] = malfunction['dataid'].astype(np.int64)
# malfunction['meter_value'] = malfunction['meter_value'].astype(np.int64)

# malfunction.reset_index(drop = True, inplace = True)

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