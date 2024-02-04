import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the SQLite database
conn = sqlite3.connect('./data/data.db')

# Execute a query to retrieve some data
df = pd.read_sql_query("SELECT * FROM combined_data", conn)

# Don't forget to close the connection
conn.close()

# Assuming df is your DataFrame loaded from the SQL database
# Convert 'date' column to datetime format if it's not already
df['date'] = pd.to_datetime(df['date'])

# Set the 'date' column as the index of the DataFrame
df.set_index('date', inplace=True)

# Plotting
fig, ax1 = plt.subplots()

color = 'tab:red'
ax1.set_xlabel('Date')
ax1.set_ylabel('Average Temperature (celcius)', color=color)
ax1.plot(df.index, df['ave_temp'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

# Create a twin Axes sharing the x-axis
ax2 = ax1.twinx()
color = 'tab:blue'
ax2.set_ylabel('Stock Price of OSK ($)', color=color)
ax2.plot(df.index, df['stock_price'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

# Title and legend
plt.title('Average Temperature and Stock Price Over Time')
fig.tight_layout()  # To ensure the right y-label is not slightly clipped
plt.show()
