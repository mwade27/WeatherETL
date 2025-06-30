from datetime import datetime 
import pandas as pd
import json 

df = pd.read_csv("../data/weather_data.csv")

# Convert 'dt', sunrise and sunset to date time
def convert_to_datetime(df):
    #Converting the time units from unix to standard time
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['sys_sunrise'] = pd.to_datetime(df['sys_sunrise'], unit='s')
    df['sys_sunset'] = pd.to_datetime(df['sys_sunset'], unit='s')
    #Create timezone offset column to help calculate the offset in the times
    df['time_offset'] = pd.to_timedelta(df['timezone'],unit='s')
    #Compute the timezone offsets and update the columns to show standard time format
    df['dt'] = df['dt'] + df['time_offset']
    df['sys_sunrise'] = df['sys_sunrise'] + df['time_offset']
    df["sys_sunset"] = df['sys_sunset'] + df['time_offset']
    
    #Split the date and time
    df["Date"] = df['dt'].dt.date
    df["Time"] = df['dt'].dt.time
    df["Hour"] = df['dt'].dt.hour
    df["Month"] = df['dt'].dt.month
    #Drops the timeoffset and dt columns since they are not needed 
    df.drop(columns = ["time_offset","dt"], inplace = True)
    
    """
    New transformations
    israining or snowing columns
    temparture binding so 32 < would be 'Freezing', <85 extreme heat
    Calculate daylight duration 
    humidity levels
    change visibility from meters to ft 
    
    
    
    """


convert_to_datetime(df)

print(df['sys_sunrise'].head())
print(df['sys_sunset'].head())

df.to_csv("../data/weather_data_transformed.csv", index = False)

