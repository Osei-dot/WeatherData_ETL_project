import pandas as pd
import requests
import os
import json
from sqlachemy import create_engine

#----------------------Data Extraction-----------------------------#
Ghana_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=5.5571096&lon=-0.2012376&appid="
Nigeria_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=9.0643305&lon=7.4892974&appid="
Togo_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.15803455&lon=1.2516823732466&appid="
Benin_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.3676953&lon=2.4252507&appid="
Ginuea_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=10.6617959&lon=-14.6024392&appid="
CoteDivoire_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.8200066&lon=-5.2776034&appid="

Data_Dir= "/home/ubuntu_gelsc/Weather_ETL_Project"
RAW_FILE_PATH =os.path.join(Data_Dir, "rawdata.json")  
os.makedirs(Data_Dir, exist_ok=True)
Weather_API_URL= [Ghana_Weather_call, Nigeria_Weather_call, Togo_Weather_call, Benin_Weather_call, Ginuea_Weather_call, CoteDivoire_Weather_call]
def extract_data(api_urls):
    all_data = []
    for url in api_urls:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            all_data.append(data)
        else:
            print(f"API request failed for {url} with status code {response.status_code}")
    
    with open(RAW_FILE_PATH, 'w') as f:
        json.dump(all_data,f,indent=4)

        
    return RAW_FILE_PATH

WeatherData = extract_data(Weather_API_URL)

#..............................................................#
#...............API Data Transformation........................#

def transform_data(RAW_FILE_PATH: str):
    with open(RAW_FILE_PATH, 'r') as f:
        data = json.load(f)
    
    records = []
    for entry in data:
        record = {
        "country":entry["sys"]["country"],
        "city":entry["name"],
        "weather_main":entry["weather"][0]["main"],
        "weather_description":entry["weather"][0]["description"],
        "temp(c)":entry["main"]["temp"]-273.15,  # Convert Kelvin to Celsius
        "feels_like(C)":entry["main"]["feels_like"]-273.15,  # Convert Kelvin to Celsius
        "humidity":entry["main"]["humidity"],
        "wind_speed":entry["wind"]["speed"] 

        }
        records.append(record)

    df = pd.DataFrame(records)

#---------------------------------------------------------------#
#..............Data Loading..................................#

engine = Create_engine("postgresql://postgres:postgres@localhost:5432/WeatherDB")
df.to_sql('WeatherData', engine, if_exists='replace', index=False)