import requests
import redis
import matplotlib.pyplot as plt
import pandas as pd
import json
from collections import Counter
from datetime import datetime

class WeatherAPI:
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_data(self):
        """Fetches and returns JSON data from the API."""
        response = requests.get(self.api_url)
        response.raise_for_status()
        return response.json()
    
class RedisManager:
    """Manages interactions with RedisJSON."""
    def __init__(self, host='redis-12044.c321.us-east-1-2.ec2.cloud.redislabs.com', port=12044, username='default', password='j7YCYV2tWndx3Z77E5ZnHRFeradJK6Sz'):
        self.db = redis.Redis(host=host, port=port, username=username, password=password, decode_responses=True)
        try:
            self.db.ping()
        except redis.exceptions.ConnectionError:
            print("Redis connection error. Please ensure Redis is running and accessible.")
            exit(1)
    
    def insert_json(self, key, data):
        """Inserts JSON data into RedisJSON."""
        self.db.json().set(key, '$', data)
    
    def get_json(self, key):
        """Retrieves JSON data from RedisJSON."""
        return self.db.json().get(key)
    
class DataProcessor:
    # Function to get temperature data
    def get_temperature_data(data):
        """
        Extracts temperature data from the JSON data and converts it to Fahrenheit.
        """
        periods = data['properties']['periods']
        # Extract temperature data and convert to Fahrenheit
        temperatures = [float(period['temperature']) * 9 / 5 + 32 for period in periods]
        return temperatures

    # Get temperature data
    response = requests.get('https://api.weather.gov/gridpoints/PHI/53,65/forecast/hourly')
    data = json.loads(response.text)
    temperatures = get_temperature_data(data)

    # Function to plot time series data
    def plot_time_series(data, x_label, y_label, title):
        """
        Plots time series data using matplotlib.
        """
        plt.figure(figsize=(10, 6))
        plt.plot(data)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.grid(True)
        plt.show()

    # Plot temperature time series
    plot_time_series(temperatures, "Time (EST)", "Temperature (Fahrenheit)", "Temperature over Time")

        # Function to find the minimum and maximum temperature
    def get_min_max_temp(data):
        """
        Finds the minimum and maximum temperature from the data.
        """
        return min(data), max(data)

    

    # Get minimum and maximum temperature
    min_temp, max_temp = get_min_max_temp(temperatures)
    print(f"Minimum temperature: {min_temp:.2f} F")
    print(f"Maximum temperature: {max_temp:.2f} F")

def main():
    api_url = 'https://api.weather.gov/gridpoints/PHI/53,65/forecast/hourly'
    REDIS_KEY = 'WeatherAPI'

    # Fetch data from API and store in Redis
    fetcher = WeatherAPI(api_url)
    data = fetcher.fetch_data()
    manager = RedisManager()
    manager.insert_json(REDIS_KEY, data)

    # Retrieve data from Redis and process it
    retrieved_data = manager.get_json(REDIS_KEY)

    # Create an instance of DataProcessor
    processor = DataProcessor()

    temperatures = processor.get_temperature_data(retrieved_data)
    processor.plot_time_series(temperatures, "Time (EST)", "Temperature (Fahrenheit)", "Temperature over Time")
    min_temp, max_temp = processor.get_min_max_temp(temperatures)
    print(f"Minimum temperature: {min_temp:.2f} F")
    print(f"Maximum temperature: {max_temp:.2f} F")

if __name__ == '__main__':
    main()
