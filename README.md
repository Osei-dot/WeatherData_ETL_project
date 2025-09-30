**🌦️WEATHER DATA ETL FOR 6 AFRICAN CITIES.**

This project demonstrates an **ETL (Extract, Transform, Load) pipeline** built with **Apache Airflow**. 

It fetches weather data from an API [OpenWeathermapAPI](https://openweathermap.org), processes it, and stores it in PostgreSQL for downstream analysis.

This project was done for 6 selected countries.
1. Ghana (Accra)
2. Nigeria (Abuja)
3. Benin (Cotonou)
4. Ivory Coast (Yamoussoukro)
5. Togo (Station Meteo)
6. Guinea (Kamsar)


These countries/cities were purely selected at random for educational purposes.
The pipeline is orchestrated using **Airflow DAGs** and can be extended to integrate with data warehouses or visualization tools (PowerBI was used)
This can be used for any project that require Weather data from any of the listed project in a tabular form.
With a raw json file format, the project cleans and transform them with an additional timestamp showing what day and time the weather was recorded.
This is good enough to give you insight into how weatherData changes in a day.

**Project Structure**

Weather_ETL_Project/

│── **dags/**

│ └── weather_etl_dag.py # Main Airflow DAG definition

│
│── **Weather_ETL_Project/**

│ └── main.py  # Core ETL script (extract, transform, load)

|── rawdata.json  # Collect raw API queries for every API call 

|── etl_log.log   #  Collect logs filtered for logging.INFO

|──error_log.log  #  Collect logs filtered for logging.ERROR

│
│── requirements.txt  # Python dependencies

│── README.md  # Project documentation


**How to Use it**

1. Create and Activate Virtual Environment
python3 -m venv airflow_env

source airflow_env/bin/activate

3. Install Dependencies
pip install -r requirements.txt

4. Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db reset --yes
airflow db migrate
airflow users create \
  --username admin \
  --firstname First \
  --lastname Last \
  --role Admin \
  --email admin@example.com

5. Start Services
airflow scheduler &
airflow webserver -p 8080 &


The Airflow UI should now be available at:
👉 http://localhost:8080

🚀 Running the DAG

Place your DAG file inside the dags/ folder:

~/airflow/dags/weather_etl_dag.py


Access the Airflow UI and enable the DAG (weather_etl_dag).

Trigger the DAG manually or wait for its scheduled run.

🛠️ Troubleshooting

DAG not showing in UI
Ensure:

The file is named in lowercase (e.g., weather_etl_dag.py)

It’s located inside ~/airflow/dags/

Run airflow dags reserialize #to serialize your dags in metaData

Run airflow dags list to verify it’s loaded

Database Errors (UNIQUE constraint failed)
Run:

airflow db reset --yes
airflow db migrate


Missing Kubernetes modules
Install extras:

pip install apache-airflow[cncf.kubernetes]

📊 Next Steps

Store processed data in a database (e.g., PostgreSQL, BigQuery)

Connect ETL outputs to a dashboard (Power BI, Tableau, or Superset)

Add alerting and monitoring for pipeline failures

👨‍💻 Author

Dickson Osei – [osei-dot](https://github.com/Osei-dot)

<img width="1623" height="897" alt="image" src="https://github.com/user-attachments/assets/6dc35f65-1462-4d66-82a7-ecb6704a0f90" />
<img width="1331" height="378" alt="image" src="https://github.com/user-attachments/assets/6c91469e-81e5-4794-af65-58f05324e25f" />
<img width="1150" height="651" alt="image" src="https://github.com/user-attachments/assets/a901a251-6f75-4fd5-a372-e39dd5188cd1" />



