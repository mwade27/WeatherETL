import requests
import pandas as pd
import json
from datetime import datetime,date,timedelta, time
from dotenv import load_dotenv
import os
import time as time_module

load_dotenv()
#Target_cities = ["Birmingham","Miami","New York","Los Angeles","Phoenix"]
Target_cities = ["Birmingham","Miami","New York","Los Angeles","Phoenix","Nashville",
                "Seattle","Boise", "Denver", "Atlanta","Chicago","Kansas City"]
Target_City = "Huntsville"
API_KEY = os.getenv("API_KEY")

weather_data = [] 
"""
Uses the basic OpenWeather url 
to get the lat and long of the target 
creates loop for cities in Target Cities 
sends request to api and fetches content 
adds the content to the list weather_data
""" 
def extractcitydata(Target_cities,weather_data,API_KEY):
    for city in Target_cities:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city},US&appid={API_KEY}&units=imperial"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            weather_data.append({
                "city": data['name'],
                "id": data['id'], 
                "longitude": data['coord']['lon'],
                "latitude": data['coord']['lat'],
                "sea_level": data['main'].get('sea_level'),
                "ground_level": data['main'].get('grnd_level'),
                "timezone": data['timezone']
            })
        else:
            print("Failed to retrieve data:", response.status_code)
            print("Message:" + response.json().get("message", "No message available"))
    return(weather_data)


extractcitydata(Target_cities,weather_data,API_KEY)

"""
Function that take the weather_data list and api key
creates loops for all the cities in weather_data 
Sends request to api to fetch content for the city
Takes content and adds to the specified city dictionary
returns weather data      
"""
#get the unix time for the timestamps of the current date
hours = [0, 4, 8, 12, 16, 20]
todaysdate = date.today()

unixtimes = []
for h in hours:
    dt = datetime.combine(todaysdate, time(hour=h))
    unixtimes.append(int(dt.timestamp()))

print(unixtimes)

def currentdateweatherdata(weather_data, API_KEY, unixtimes):
    results = []
    for city in weather_data:
        for unix_time in unixtimes:
            url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={city['latitude']}&lon={city['longitude']}&dt={unix_time}&units=imperial&appid={API_KEY}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                hourly = data.get("data", [{}])[0]
                record = {
                    "city": city["city"],
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "timestamp": unix_time,
                    "timezone": data.get("timezone"),
                    "timezone_offset": data.get("timezone_offset"),
                    "datetime": hourly.get("dt"),
                    "sunrise": hourly.get("sunrise"),
                    "sunset": hourly.get("sunset"),
                    "temp": hourly.get("temp"),
                    "feels_like": hourly.get("feels_like"),
                    "pressure": hourly.get("pressure"),
                    "humidity": hourly.get("humidity"),
                    "dew_point": hourly.get("dew_point"),
                    "clouds": hourly.get("clouds"),
                    "uvi": hourly.get("uvi"),
                    "visibility": hourly.get("visibility"),
                    "wind_speed": hourly.get("wind_speed"),
                    "wind_gust": hourly.get("wind_gust"),
                    "wind_deg": hourly.get("wind_deg"),
                    "weather_id": hourly.get("weather", [{}])[0].get("id"),
                    "weather_name": hourly.get("weather", [{}])[0].get("main"),
                    "weather_description": hourly.get("weather", [{}])[0].get("description"),
                    "rain_1h": hourly.get("rain", {}).get("1h", 0),
                    "snow_1h": hourly.get("snow", {}).get("1h", 0)
                }
                results.append(record)
            else:
                print("Failed to retrieve data:", response.status_code)
                print("Message:" + response.json().get("message", "No message available"))
    return results
                 
current_results = currentdateweatherdata(weather_data,API_KEY,unixtimes)


             
#print(weather_data)

    
   
df = pd.DataFrame(current_results)
df.to_csv("../data/current_weather_data.csv", index=False)
print("Data extraction complete. Data saved to weather_data.csv.")
# extracting the historical data 


#historical_weather_data = []
#extractcitydata(Target_cities,historical_weather_data,API_KEY)
def extractHistoricalWeatherdata(historical_data, API_KEY):
    past_dates = []
    today = datetime.now().date()
    hours = [0, 4, 8, 12, 16, 20]
    for i in range(30):
        past_date = today - timedelta(days=i)
        for h in hours:
            past_datetime = datetime.combine(past_date, time(hour=h))
            unix_time = int(past_datetime.timestamp())
            past_dates.append(unix_time)
    results = []
    for city in historical_data:
        for unix_time in past_dates:
            url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={city['latitude']}&lon={city['longitude']}&dt={unix_time}&appid={API_KEY}"
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                hourly = data.get("data", [{}])[0]
                record = {
                    "city": city["city"],
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "timestamp": unix_time,
                    "timezone": data.get("timezone"),
                    "timezone_offset": data.get("timezone_offset"),
                    "datetime": hourly.get("dt"),
                    "sunrise": hourly.get("sunrise"),
                    "sunset": hourly.get("sunset"),
                    "temp": hourly.get("temp"),
                    "feels_like": hourly.get("feels_like"),
                    "pressure": hourly.get("pressure"),
                    "humidity": hourly.get("humidity"),
                    "dew_point": hourly.get("dew_point"),
                    "clouds": hourly.get("clouds"),
                    "uvi": hourly.get("uvi"),
                    "visibility": hourly.get("visibility"),
                    "wind_speed": hourly.get("wind_speed"),
                    "wind_gust": hourly.get("wind_gust"),
                    "wind_deg": hourly.get("wind_deg"),
                    "weather_id": hourly.get("weather", [{}])[0].get("id"),
                    "weather_name": hourly.get("weather", [{}])[0].get("main"),
                    "weather_description": hourly.get("weather", [{}])[0].get("description"),
                    "rain_1h": hourly.get("rain", {}).get("1h", 0),
                    "snow_1h": hourly.get("snow", {}).get("1h", 0)
                }
                results.append(record)
            else:
                print("Failed to retrieve data:", response.status_code)
                print("Message:" + response.json().get("message", "No message available"))
            time_module.sleep(2)
    return results
    
        
    
            
    
    
    
    
    
#historical_results = extractHistoricalWeatherdata(historical_weather_data,API_KEY)
#df = pd.DataFrame(historical_results)

#df.to_csv("../data/historical_weather_data.csv", index=False)
    
    
"""
Step find the last 30 days and add to a list
then iteriate through those and requst the content for the 
cities and store in a new list 
"""





       