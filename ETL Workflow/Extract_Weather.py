from datetime import datetime
from meteostat import Daily
import os

def fetch_weather_data_and_save(meteostation: str, file_name: str, start: datetime, end: datetime):
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
    script_dir = os.path.dirname(os.path.realpath(__file__))
    folder_path = os.path.join(script_dir, file_name.split('.')[0])
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'w') as f:
        f.write(data.to_html(index=False))

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

def main():
    start = datetime(2013, 3, 1)
    end = datetime(2024, 1, 1)
    meteostation = "72509"
    file_name = "boston_weather_data.html"
    preview_html = preview_weather_data(meteostation, start, end)
    print(preview_html)  
    fetch_weather_data_and_save(meteostation, file_name, start, end)

if __name__ == "__main__":
    main()
