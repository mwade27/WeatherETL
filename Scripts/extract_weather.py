import requests
import pandas as pd
import json
from datetime import datetime

Target_cities = ["Birmingham","Miami","New York","Los Angeles","Phoenix","Nashville","Dallas",
                 "Seattle","Boise", "Denver", "Atlanta","Chicago","Kansas City","Salt Lake City","Minneapolis"]
Target_City = "Huntsville"
key = "2f6d50ff0279460ed80c970e966166c7"

weather_data = []    
for city in Target_cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},US&appid={key}&units=imperial"
    response = requests.get(url)
    if response.status_code == 200:
        data =response.json()
        weather_data.append({
            "city": data['name'],
            "id": data['id'],
            "timezone": data['timezone'],
            "longitude": data['coord']['lon'],
            "latitude": data['coord']['lat'],
            "weather_id": data['weather'][0]['id'],
            "weather_main": data['weather'][0]['main'],
            "weather_description": data['weather'][0]['description'],
            "weather_icon": data['weather'][0]['icon'],
            "main_temp": data['main']['temp'],
            "main_feels_like": data['main']['feels_like'],
            "main_temp_min": data['main']['temp_min'],
            "main_temp_max": data['main']['temp_max'],
            "main_pressure": data['main']['pressure'],
            "main_humidity": data['main']['humidity'],
            "main_sea_level": data['main'].get('sea_level', None),
            "main_grnd_level": data['main'].get('grnd_level', None),
            "visibility": data.get('visibility', None),
            "wind_speed": data['wind']['speed'],
            "wind_deg": data['wind']['deg'],
            "wind_gust": data['wind'].get('gust', None),
            "clouds_all": data['clouds']['all'],
            "rain_1h": data.get('rain', {}).get('1h',0.0),
            "snow_1h": data.get('snow',{}).get('1h', 0.0),
            "dt": data['dt'],
            "sys_sunrise": data['sys']['sunrise'],
            "sys_sunset": data['sys']['sunset'],
            "sys_country": data['sys']['country'],
        })
    else:
        print("Failed to retrieve data:", response.status_code)
        print("Message:" + response.json().get("message", "No message available"))

df = pd.DataFrame(weather_data)
df.to_csv("../data/weather_data.csv", index=False)
print("Data extraction complete. Data saved to weather_data.csv.")

"""Variables for the ETL process:
       coord: lon, lat
       weather: id,main,description,icon
       base: base
       main: temp,feels_like,temp_min,temp_max,pressure,humidity,sea_level,grnd_level
       visibility: visibility
       wind: speed,deg
       clouds: all
       dt: dt
       sys: type,id,country,sunrise,sunset,1h
       timezone: timezone
       other: id,name,cod
       """