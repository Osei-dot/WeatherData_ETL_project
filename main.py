import pandas as pd
import requests
import sqlite3

#Data Extraction
Ghana_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=5.5571096&lon=-0.2012376&appid=1c5002dfe0a4a596985100efecc3338e"
Nigeria_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=9.0643305&lon=7.4892974&appid=1c5002dfe0a4a596985100efecc3338e"
Togo_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.15803455&lon=1.2516823732466&appid=1c5002dfe0a4a596985100efecc3338e"
Benin_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.3676953&lon=2.4252507&appid=1c5002dfe0a4a596985100efecc3338e"
Ginuea_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=10.6617959&lon=-14.6024392&appid=1c5002dfe0a4a596985100efecc3338e"
CoteDivoire_Weather_call="https://api.openweathermap.org/data/2.5/weather?lat=6.8200066&lon=-5.2776034&appid=1c5002dfe0a4a596985100efecc3338e"


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
    return pd.DataFrame(all_data)

df = extract_data(Weather_API_URL)
print(df)


