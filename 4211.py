import numpy as np
import pandas as pd

#1.1
# df = pd.read_csv("./dataport-export_gas_oct2015-mar2016.csv")
# data_id = df['dataid'].nunique()
# print("number of house =", data_id)

test_df = pd.read_csv("./short100data.csv")

#sort data into order by id, if same id then by time
test_df = test_df.sort_values(["dataid", "localminute"], ascending=[True, True])
test_df = test_df.reset_index(drop = True)

#find malfunction meters 
malfunction = test_df['meter_value'].duplicated(keep = 'first')

# Change bool to int
malfunction = malfunction.astype(int)

# put all malfunction meter id and time period of malfunctioning into dictionary
ded_meter = {}
for i in range(len(malfunction) - 1):
    if (malfunction[i] == 1):
        a = {test_df['dataid'][i] : {'start_time' : test_df['localminute'][i-1]}}
        j = i
        while malfunction[j] != 1:
            j+=1
        a[test_df['dataid'][i]].update({'end_time' : test_df['localminute'][j]})
        ded_meter.update(a)
if malfunction[len(malfunction) - 1] == 1:
    i = len(malfunction) - 1
    a = {test_df['dataid'][i] : {'start_time' : test_df['localminute'][i-1], 'end_time' : test_df['localminute'][i]}}
    ded_meter.update(a)

#1.2