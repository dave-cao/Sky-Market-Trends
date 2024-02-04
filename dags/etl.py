from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import requests
from airflow.utils.dates import days_ago

# Keys
STOCK_API_KEY = "HSF3ANJAZIIKS79M"
WEATHER_API_KEY = "0823422c3bed464fbc37e0fa728c8c95"

# PARAMS
WEATHER_PARAMS = {
    "city": "Wisconsin",
    "country": "US",
    "start_date": "2023-09-12",
    "end_date": "2024-02-03",
    "key": WEATHER_API_KEY
}
STOCK_PARAMS = {
    "function": "TIME_SERIES_DAILY",
    "symbol": "OSK",
    "apikey": STOCK_API_KEY
}

# Define default arguments for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    'Weather_and_OSHKOSH_ETL_Process',
    default_args=default_args,
    description='A simple ETL process relating weather and stock price',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Define the tasks/functions for ETL


def initialize_database():
    # Connect to SQLite database (or create if it doesn't exist)
    # This creates a file named 'data.db'
    conn = sqlite3.connect('/opt/airflow/data/data.db')
    c = conn.cursor()

    # Create tables
    c.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                    date TEXT PRIMARY KEY,
                    location TEXT,
                    ave_temp REAL,
                    min_temp REAL,
                    max_temp REAL,
                    precipitation REAL,
                    snow REAL,
                    snow_depth REAL,
                    wind_speed REAL,
                    wind_gust REAL,
                    max_wind_speed REAL,
                    solar_radiation REAL,
                    atmospheric_pressure REAL,
                    humidity REAL
                 )''')

    c.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                    date TEXT PRIMARY KEY,
                    name TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER
                 )''')

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def extract_weather_data_to_db(response):
    conn = sqlite3.connect('/opt/airflow/data/data.db')
    c = conn.cursor()
    data = response.json()

    # check if there is a response
    if response.status_code != 200 or data.get("data") is None:
        print("Error fetching weather data")
        return

    # extract the data that we want
    for day in data.get("data", []):
        # our key
        date = day["datetime"]
        # temperature
        ave_temp = day["temp"]
        min_temp = day["min_temp"]
        max_temp = day["max_temp"]

        # weather conditions
        precipitation = day["precip"]
        snow = day["snow"]
        snow_depth = day["snow_depth"]
        wind_speed = day["wind_spd"]
        wind_gust = day["wind_gust_spd"]
        max_wind_speed = day["max_wind_spd"]

        # other
        solar_radiation = day["solar_rad"]
        atmospheric_pressure = day["pres"]
        humidity = day["rh"]

        c.execute("""
            INSERT OR IGNORE INTO weather_data (
                date, location,
                ave_temp, min_temp, max_temp,
                precipitation, snow, snow_depth,
                wind_speed, wind_gust, max_wind_speed,
                solar_radiation, atmospheric_pressure, humidity)
            VALUES
                (?, ?,
                ?, ?, ?,
                ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?)
        """, (date, WEATHER_PARAMS.get("city"),
              ave_temp, min_temp, max_temp,
              precipitation, snow, snow_depth,
              wind_speed, wind_gust, max_wind_speed,
              solar_radiation, atmospheric_pressure, humidity))
    conn.commit()
    conn.close()


def extract_stock_data_to_db(response):
    conn = sqlite3.connect('/opt/airflow/data/data.db')
    c = conn.cursor()
    data = response.json()

    # check if there is a response
    if response.status_code != 200 or data.get("Time Series (Daily)") is None:
        print("Error fetching stock data")
        return

    # extract the data that we want
    for date, day in data.get("Time Series (Daily)").items():
        # our key
        open = day["1. open"]
        high = day["2. high"]
        low = day["3. low"]
        close = day["4. close"]
        volume = day["5. volume"]

        c.execute("""
            INSERT OR IGNORE INTO stock_prices (
                date, name, open, high, low, close, volume)
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
        """, (date, STOCK_PARAMS.get("symbol"), open, high, low, close, volume))

    conn.commit()
    conn.close()


def combine_data_to_db():
    # Connect to the SQLite database
    conn = sqlite3.connect('/opt/airflow/data/data.db')
    c = conn.cursor()

    # Create a new table for storing the query results
    # If the table already exists, this statement will be ignored
    c.execute('''
        CREATE TABLE IF NOT EXISTS combined_data (
            date TEXT PRIMARY KEY,
            name TEXT,
            ave_temp REAL,
            stock_price REAL
        )
    ''')

    # Execute the modified query to fetch the desired data
    c.execute('''
        SELECT weather_data.date, stock_prices.name, weather_data.ave_temp, stock_prices.close
        FROM weather_data
        JOIN stock_prices ON weather_data.date = stock_prices.date
        ORDER BY weather_data.date;
    ''')
    query_results = c.fetchall()

    # Clear the table before inserting new data
    # This ensures the table only contains the latest results each time the script runs
    c.execute('DELETE FROM combined_data')

    # Insert the query results into the new table
    c.executemany('''
        INSERT OR REPLACE INTO combined_data (date, name, ave_temp, stock_price)
        VALUES (?, ?, ?, ?)
    ''', query_results)

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def extract():
    # Your code for extraction
    initialize_database()

    weather_url = "https://api.weatherbit.io/v2.0/history/daily"
    stock_url = "https://www.alphavantage.co/query"

    # get the api responses (EXTRACT)
    print("Fetching data from weather API")
    weather_response = requests.get(weather_url, params=WEATHER_PARAMS)

    print("Fetching data from stocks API")
    stock_response = requests.get(stock_url, params=STOCK_PARAMS)

    # TRANSFORM AND LOAD
    print("Extracting data to database")
    extract_weather_data_to_db(weather_response)
    extract_stock_data_to_db(stock_response)


def load():
    print("Combining data to database")
    combine_data_to_db()
    pass


# Define the task sequence
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)


t2 = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

t1 >> t2
