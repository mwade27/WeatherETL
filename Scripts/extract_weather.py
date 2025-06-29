import requests
import pandas as pd
import json

Target_City = "Birmingham"
key = "2f6d50ff0279460ed80c970e966166c7"
url = f"http://api.openweathermap.org/data/2.5/weather?q={Target_City},US&appid={key}&units=imperial"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))  
else:
    print("Failed to retrieve data:", response.status_code)
    print("Message:" + response.json().get("message", "No message available"))