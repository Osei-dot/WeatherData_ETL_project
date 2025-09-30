import pandas as pd
import requests
import os
import json
from sqlalchemy import create_engine


from datetime import datetime

#------------------------logging-------------------------------#
import logging
import sys

logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler=logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
file_handler=logging.FileHandler('etl_log.log')
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

error_file_handler=logging.FileHandler('error_log.log')
error_file_handler.setLevel(logging.ERROR)
logger.addHandler(error_file_handler)


formatter=logging.Formatter('[%(asctime)s] - %(name)s  - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
error_file_handler.setFormatter(formatter)

#----------------------Data Extraction-----------------------------#
logger.info("Starting data extraction process from API") 

try:
   Ghana_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=5.5571096&lon=-0.2012376&appid="
   Nigeria_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=9.0643305&lon=7.4892974&appid="
   Togo_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.15803455&lon=1.2516823732466&appid="
   Benin_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.3676953&lon=2.4252507&appid="
   Ginuea_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=10.6617959&lon=-14.6024392&appid="
   CoteDivoire_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.8200066&lon=-5.2776034&appid="
except Exception as e:
    logger.error(f"Error in defining API URLs: {e}")

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
            logger.error(f"API request failed with status code {response.status_code}")
    
    
    with open(RAW_FILE_PATH, 'w') as f:
        json.dump(all_data,f,indent=4)

        
    return RAW_FILE_PATH

def execute_extraction(api_urls):

    if any(r.status_code == 200 for r in [requests.get(url) for url in api_urls]):
        logger.info("Extracting data from API......")
        return extract_data(Weather_API_URL)
    else:
        logger.error("No successful API responses received.")
        raise ValueError("All API requests failed.")


#..............................................................#
#...............API Data Transformation and Loading........................#

def transform_data(RAW_FILE_PATH: str):
    with open(RAW_FILE_PATH, 'r') as f:
        data = json.load(f)
    
    records = []
    for entry in data:
        record = {
        "Date":datetime.now().strftime("%Y-%m-%d %H:%M"),
        "country":entry["sys"]["country"],
        "city":entry["name"],
        "weather_main":entry["weather"][0]["main"],
        "weather_description":entry["weather"][0]["description"],
        "temp(c)": round(entry["main"]["temp"]-273.15, 2),  # Convert Kelvin to Celsius, 2 decimal places
        "feels_like(C)":round(entry["main"]["feels_like"]-273.15, 2),  # Convert Kelvin to Celsius, 2 decimal places
        "humidity(%)":entry["main"]["humidity"],
        "wind_speed":entry["wind"]["speed"] 

        }
        records.append(record)

    df = pd.DataFrame(records)
    logger.info("Loading data into the database")
    try:
        engine = create_engine("postgresql+psycopg2://postgres:postgres@172.25.128.1:5432/Today_Weather_Data")
        df.to_sql("TodayWeather", con=engine, if_exists="replace", index=False)
    except Exception as e:
        logger.error(f"Error during data loading to postgress: {e}")
    else:
        logger.info("Data loaded successfully into the database")

#ETL process........................................................
def etl_process():

    return transform_data(RAW_FILE_PATH)

if __name__ == "__main__":
    etl_process()



