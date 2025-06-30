import requests
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()
Target_cities = ["Birmingham","Miami","New York","Los Angeles","Phoenix"]
#Target_cities = ["Birmingham","Miami","New York","Los Angeles","Phoenix","Nashville","Dallas",
#                "Seattle","Boise", "Denver", "Atlanta","Chicago","Kansas City","Salt Lake City","Minneapolis"]
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
                "longitude":data['coord']['lon'],
                "latitude":data['coord']['lat'],
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
def extractdailyweatherdata(weather_data,API_KEY):
    for city in weather_data:
        
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={city['latitude']}&lon={city['longitude']}&exclude=minutely&units=imperial&appid={API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            city.update({
                "timezone": data.get("timezone"),
                "timezone_offset": data.get("timezone_offset"),
                "sunrise": data["current"].get("sunrise"),
                "sunset": data["current"].get("sunset"),
                "temp": data["current"].get("temp"),
                "feels_like": data["current"].get("feels_like"),
                "pressure": data["current"].get("pressure"),
                "humidity": data["current"].get("humidity"),
                "dew_point": data["current"].get("dew_point"),
                "uvi": data["current"].get("uvi"),
                "clouds": data["current"].get("clouds"),
                "visibility": data["current"].get("visibility"),
                "wind_speed": data["current"].get("wind_speed"),
                "wind_deg": data["current"].get("wind_deg"),
                "wind_gust": data["current"].get("wind_gust"),
                "rain": data["current"].get("rain", {}).get("1h", 0),
                "snow": data["current"].get("snow", {}).get("1h", 0),
                "weather_id": data["current"].get("weather", [{}])[0].get("id"),
                "weather_name": data["current"].get("weather", [{}])[0].get("main"),
                "weather_description": data["current"].get("weather", [{}])[0].get("description"),
                "dt": data['daily'][0].get("dt") if "daily" in data and len(data["daily"]) > 0 else None,
                "moonrise": data['daily'][0].get("moonrise") if "daily" in data and len(data["daily"]) > 0 else None,
                "moonset": data['daily'][0].get("moonset") if "daily" in data and len(data["daily"]) > 0 else None,
                "moon_phase": data['daily'][0].get("moon_phase") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily pressure": data['daily'][0].get("pressure") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily humidity": data['daily'][0].get("humidity") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily dew point": data['daily'][0].get("dew_point") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily wind speed": data['daily'][0].get("wind_speed") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily wind gust": data['daily'][0].get("wind_gust") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily wind deg": data['daily'][0].get("wind_deg") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily clouds": data['daily'][0].get("clouds") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily uvi": data['daily'][0].get("uvi") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily pop": data['daily'][0].get("pop") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily rain": data['daily'][0].get("rain", None) if "daily" in data and len(data["daily"]) > 0 else None,
                "daily snow": data['daily'][0].get("snow", None) if "daily" in data and len(data["daily"]) > 0 else None,
                "daily weather_id": data['daily'][0].get("weather", [{}])[0].get("id") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily weather name": data['daily'][0].get("weather", [{}])[0].get("main") if "daily" in data and len(data["daily"]) > 0 else None,
                "daily weather description": data['daily'][0].get("weather", [{}])[0].get("description") if "daily" in data and len(data["daily"]) > 0 else None,
            })
        else:
            print("Failed to retrieve data:", response.status_code)
            print("Message:" + response.json().get("message", "No message available"))
        
    return weather_data
            
extractdailyweatherdata(weather_data,API_KEY)   
print(weather_data)

    
   
df = pd.DataFrame(weather_data)
df.to_csv("../data/daily_weather_data.csv", index=False)
print("Data extraction complete. Data saved to weather_data.csv.")
# extracting the historical data 


historical_weather_data = []
"""
Step find the last 30 days and add to a list
then iteriate through those and requst the content for the 
cities and store in a new list 
"""





       