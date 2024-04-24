from datetime import datetime
from meteostat import Daily
import os
import logging

def fetch_weather_data(meteostation: str,start: datetime, end: datetime)-> bytes:
    """
    Fetches weather data for a given meteostation, start date, and end date,
    and saves it to an HTML file.
    
    Parameters:
        meteostation (str): The ID of the meteostation.
        file_name (str): The name of the HTML file to save the data.
        start (datetime): The start date.
        end (datetime): The end date.
    """
    data = Daily(meteostation, start, end).fetch()
    data = data.reset_index().iloc[:, [0, 1, 2, 3, 4, 6, 7, 9]]


def preview_weather_data(meteostation: str, start: datetime, end: datetime, preview_limit: int = 10):
    """
    Fetches weather data for a given meteostation, start date, and end date,
    and returns a preview in HTML format.
    
    Parameters:
        meteostation (str): The ID of the meteostation.
        start (datetime): The start date.
        end (datetime): The end date.
        preview_limit (int): The maximum number of rows to include in the preview.
        
    Returns:
        str: The HTML preview of the weather data.
    """
    data = Daily(meteostation, start, end).fetch()
    data = data.reset_index().iloc[:, [0, 1, 2, 3, 4, 6, 7, 9]]
    return data.head(preview_limit).to_html(index=False)

#def main():
    start = datetime(2013, 3, 1)
    end = datetime(2024, 1, 1)
    meteostation = "72509"
    file_name = "boston_weather_data.html"
    preview_html = preview_weather_data(meteostation, start, end)
    print(preview_html)  
    fetch_weather_data_and_save(meteostation, file_name, start, end)

def save_as_csv(data: bytes, filename: str ) -> None:
    """
    Save CSV content to a file.

    Args:
        data (bytes): The content of the CSV file as bytes.
        filename (str, optional): The name of the CSV file. Defaults to "output.csv".

    Raises:
        Exception: If an error occurs while saving the data as a CSV file.
    """
    try:
        with open(filename, "wb") as f:
            f.write(data)
        logging.info(f"CSV data saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving data as CSV file: {e}")
        raise

