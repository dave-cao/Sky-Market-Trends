# Documenting the Steps of the Project

When I first thought of creating this project I realized that I needed a plan. 
I only had 4 days to complete the project so I had to make sure that I had a plan 
and that the project was relatively achievable. 

I decided to break the project into 4 main parts:

1. **Data Collection**
2. **Data Extraction / Cleaning**
3. **Loading the Data into a Database**
4. **Using modern data pipelines to automate the process**
5. Optional: Create a visualization of the data

I also decided to use the following technologies:
- Python
- SQlite
- Docker
- Airflow

I decided to use Docker and Airflow because it was listed as one of the requirements
for the Oshkosh Corporation data engineer intern positions. It was also in a 3.5 hour 
course that I watched on YouTube: https://www.youtube.com/watch?v=PHsC_t0j1dU&t=8614s

## Data Collection

Arguabley the most important part of the project. I wanted to get up to date data
and didn't want to just use a dataset because that is what data scientists do. A 
data engineer would be responsible for getting the data and making sure that it is
up to date.

I opted to use the *weatherBit* API to get the weather data and 
*Alpha Vantage* to get the stock data. I chose these two APIs because they were free.

Now that I got the API's all I needed to do was write a script to get the data and input it 
into a database. My database of choice was SQLite.

## Data Extraction / Cleaning

The main things that I wanted to transform / clean was the date. The date was the most important
part of the data because it would allow a connection between the two datasets that I obtained. 
Luckily for me, they were already in the same format. So now, I just extracted the relevant parts of the 
data and discared the rest.

## Loading the Data into a Database

My database of choice was SQLite. After I extracted the data, I created three different tables.
One for the stock data, one for the weather data, and one for the combined data of both. 
Using SQL I was able to join the two tables together.

## Using modern data pipelines to automate the process

The hardest part of the project for me. First I had to integrate Docker into my project.
This was simple enough because Docker has really well written documentation. I just followed along,
and was able to create a Docker image of my project. 

The harder part was using Airflow. Airflow does have a section in the documentation that talks
about using Docker with Airflow. However, I found it to be a bit confusing. I had to watch a few
YouTube videos to get a better understanding of how to do it. In the end however, I was able to 
create a Docker image of my Airflow project and was able to run it.

Now that I had it set up with Airflow, I could not sit back, relax, and watch the data get updated
everyday while increasing my database automatically! Honestly, a cronjob would have probably
been able to do the same job.

## Optional: Create a visualization of the data and *quick analysis*

I used MatPlotLib and pandas to create a visualization of the data. It compared the stock price of the 
Oshkosh Corporation with the average temperature across a year. Looking at the data, it seems 
that as the temperature increases, the stock price of the Oshkosh Corporation decreases and vice versa. 
Obviously this is a very rudimentary analysis and would probably need a lot more data and analysis to make
a proper conclusion but it can be a good starting point. 

