import mysql.connector
import openmeteo_requests

import pandas as pd
import requests_cache 
import time
from datetime import datetime, timedelta
from retry_requests import retry

#intialize connection to MySQL database
database = mysql.connector.connect(
    host="localhost",
    user="root",
    ,
    database="weather_db"
)
print(database)
print("Database connection successful\n")
cursor = database.cursor()


cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)



# List of target cities 
Cities = ["Huntsville", "Birmingham", "Montgomery", "Mobile", "Tuscaloosa","Hoover"
          , "Auburn", "Dothan", "Madison", "Decatur","Florence", "Prattville",
          "Phenix City","Vestavia Hills", "Opelika"]
# URL for geocoding API
geourl = "https://geocoding-api.open-meteo.com/v1/search?name=Huntsville&count=10&language=en&format=json"
# Function to get coordinates for a list of cities

"""
Function to get the city information 
City info includes: ID, Name, County, Latitude, Longitude
and store it in the database
"""
def get_cityinfo(cities, geourl, cursor):
	
	for city in cities:
		response = requests_cache.CachedSession().get(geourl.replace("Huntsville", city))
		data = response.json()
		add_city = ("INSERT INTO cities"
					"(ID, NAME, COUNTY, LATITUDE, LONGITUDE) "
					"VALUES(%s,%s,%s,%s,%s)"
					"ON DUPLICATE KEY UPDATE LATITUDE=VALUES(LATITUDE), LONGITUDE=VALUES(LONGITUDE)")
		if "results" in data and len(data["results"]) > 0:
			for result in data["results"]:
				if result.get("admin1")== "Alabama" and result.get("name") == city:
					city_data = (
						result["id"],
						result["name"],
						result["admin2"],
						result["latitude"],
						result["longitude"]
					)
					cursor.execute(add_city, city_data)
					break # Move to next city
		else:
			print(f"City {city} not found.")
	    
    
print(get_cityinfo(Cities, geourl, cursor))
#commit city info to database
database.commit()


# url for weather data
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 34.7304,
	"longitude": -86.58594,
	"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min"],
    "temperature_unit": "fahrenheit",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°W")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_weather_code = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

daily_data["weather_code"] = daily_weather_code
daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["temperature_2m_min"] = daily_temperature_2m_min

daily_dataframe = pd.DataFrame(data = daily_data)

print("\nDaily data\n", daily_dataframe)

database.close()