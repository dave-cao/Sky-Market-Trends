import sqlite3
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# Keys
STOCK_API_KEY = os.environ["STOCK_API_KEY"]
WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]
print(STOCK_API_KEY)
print(WEATHER_API_KEY)


def get_dates():
    # returns the current date and last years date
    today = datetime.now()
    last_year = today - timedelta(days=100)
    formatted_today = today.strftime("%Y-%m-%d")
    formatted_last_year = last_year.strftime("%Y-%m-%d")
    return formatted_last_year, formatted_today


# PARAMS
WEATHER_PARAMS = {
    "city": "Wisconsin",
    "country": "US",
    "start_date": get_dates()[0],
    "end_date": get_dates()[1],
    "key": WEATHER_API_KEY
}
STOCK_PARAMS = {
    "function": "TIME_SERIES_DAILY",
    "symbol": "OSK",
    "apikey": STOCK_API_KEY,
    "outputsize": "full"
}


def initialize_database():
    # Connect to SQLite database (or create if it doesn't exist)
    # This creates a file named 'data.db'
    conn = sqlite3.connect('./data/data.db')
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
    conn = sqlite3.connect('./data/data.db')
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
    conn = sqlite3.connect('./data/data.db')
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
    conn = sqlite3.connect('./data/data.db')
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


def main():
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

    print("Combining data to database")
    combine_data_to_db()


if __name__ == "__main__":
    main()
