
import logging
import sys
import os
import pandas as pd
from collections import Counter
from datetime import datetime

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    try:
        dataframe = pd.read_csv(file_path,encoding='latin-1')
        return dataframe
    except Exception as e:
        logging.error(f"Error loading file: {e}")
        raise

def project_columns(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
    try:
        return dataframe[columns]
    except Exception as e:
        logging.error(f"Error performing column projection: {e}")
        raise
    
def convert_shooting_to_boolean(df: pd.DataFrame) -> pd.DataFrame:
  """Converts the 'SHOOTING' column in a DataFrame to boolean (True for 'Y', False otherwise).

  Args:
      df (pd.DataFrame): The DataFrame containing the 'SHOOTING' column.

  Returns:
      pd.DataFrame: The DataFrame with the 'SHOOTING' column converted to boolean.
  """
  df['SHOOTING'] = df['SHOOTING'].str.upper()
  df['SHOOTING'] = df['SHOOTING'].fillna('N').map({'Y': 1, 'N': 0})
  return df

def new_csv(df: pd.DataFrame, output_path,output_file,columns_new:list)-> None:
  df_new = df[columns_new].drop_duplicates(subset=columns_new[0])
  df_new.to_csv(os.path.join(output_path,output_file), index=False)  # Saves the new DataFrame to a CSV

def remove_slccolumn(df: pd.DataFrame, columns_ori_df:list)-> pd.DataFrame:
  if columns_ori_df:
    df.drop(columns_ori_df,axis=1,inplace=True)  
    return df

def clean_crime_data(df: pd.DataFrame, location_cols: list) -> pd.DataFrame:
    """
    Cleans a crime dataset by removing rows with:
    - Missing values in specified location-related columns
    - Location column values equal to -1
    - Invalid latitude or longitude (-1)

    Args:
        df (pd.DataFrame): The crime dataset potentially containing missing or invalid location information.
        location_cols (list): A list of column names containing location-related information.

    Returns:
        pd.DataFrame: The cleaned crime dataset with problematic rows removed.
    """

    filtered_df = df[(df[location_cols] != -1).all(axis=1)].dropna(subset=location_cols)
    if any(col in df.columns for col in ["Latitude", "Longitude"]):
        filtered_df = filtered_df.query("Latitude != -1 and Longitude != -1")
    filtered_df=filtered_df.drop_duplicates(subset="INCIDENT_NUMBER")
    return filtered_df

def add_surrogate_key(df: pd.DataFrame, key_name: str = "DISTRICT_KEY", column: str = "DISTRICT") -> pd.DataFrame:
    """
    Adds a new column with surrogate keys, a serial number column based on the first appearance of each district,
    and another column with the original district values, along with a mapping dictionary.

    Args:
        df (pandas.DataFrame): The DataFrame containing the column with unique values.
        key_name (str, optional): The name of the new column to store the surrogate key. Defaults to "DISTRICT_KEY".
        column (str, optional): The name of the column containing the original district values. Defaults to "DISTRICT".

    Returns:
        pandas.DataFrame: The DataFrame with added surrogate key, serial number, and district value columns, and a mapping dictionary.
    """

    # Create a dictionary to map unique values to surrogate keys (use Counter for frequency)
    district_counts = Counter(df[column])
    # Add a new column with surrogate keys based on the mapping
    df[key_name] = df[column].map(district_counts)

    return df

def read_weather_html_file(file_path: str, index: int = 0) -> pd.DataFrame:
    """
    Read an HTML file and return a specific DataFrame representing a table found in the HTML.

    Args:
        file_path (str): Path to the HTML file.
        index (int): Index of the DataFrame to return from the list. Defaults to 0 (the first DataFrame).

    Returns:
        pandas.DataFrame or None: DataFrame representing the table found in the HTML, or None if no tables are found.
    """
    try:
        # Read HTML file into a list of DataFrames
        dfs = pd.read_html(file_path)
        
        # Check if the specified index is within the range of the list
        if 0 <= index < len(dfs):
            return dfs[index]
        else:
            print(f"No DataFrame found at index {index}.")
            return None
    except Exception as e:
        print(f"Error occurred while reading HTML file: {e}")
        return None

def rename_columns(df: pd.DataFrame, column_DICT: dict) -> pd.DataFrame:
    """
    Reads a CSV file, renames specified columns, and saves the modified DataFrame back to a CSV file.
    
    Parameters:
        input_file_path (str): The path to the input CSV file.
        output_file_path (str): The path to save the modified CSV file.
    """
    df.rename(columns=column_DICT, inplace=True)
    return df

def join_dataframes(df_weather: pd.DataFrame, df_crimes: pd.DataFrame, on: list, how: str
) -> pd.DataFrame:
    """
    Joins two pandas DataFrames based on specified columns and method.

    Args:
        odpady (pd.DataFrame): DataFrame representing waste data.
        obyvatele (pd.DataFrame): DataFrame representing population data.
        on (list): List of column names to join on.
        how (str): Method of join ('inner', 'outer', 'left', 'right').

    Returns:
        pd.DataFrame: Merged DataFrame.

    Raises:
        Exception: If joining data fails.
    """
    try:
        
        merged_df = pd.merge(df_weather, df_crimes, on=on, how=how)
        return merged_df
    except Exception as e:
        logging.error(f"Error joining data: {e}")
        raise

def select_interesting_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects rows from a DataFrame where 'multiple_victims' column equals 1 and 'gender' column equals 'Female'.

    Args:
        dataframe (pd.DataFrame): DataFrame to be filtered.

    Returns:
        pd.DataFrame: Filtered DataFrame containing rows where 'multiple_victims' equals 1 and 'gender' equals 'Female'.

    Raises:
        Exception: If selecting rows fails.
    """
    try:
        df = (df[df["SHOOTING"]==1] ) 
        return df
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise

def save_dataframe_to_csv(df: pd.DataFrame, output_folder: str, file_name: str) -> None:
    """
    Saves a pandas DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to be saved.
        output_folder (str): Path to the output folder where the CSV file will be saved.
        file_name (str): Name of the output CSV file.
        
    Raises:
        Exception: If saving the DataFrame to a CSV file fails.
    """
    try:
        # Create the output folder if it doesn't exist
        output_folder_path = os.path.join(output_folder, "Output")
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)

        # Construct the full path to the output CSV file
        output_path = os.path.join(output_folder_path, file_name)

        # Save the DataFrame to the CSV file
        df.to_csv(output_path, index=False, mode='w')
        logging.info(f"Result has been saved to the file: {output_path}")
    except Exception as e:
        logging.error(f"Failed to save the result to CSV file: {e}")
        raise

def add_primary_key(df: pd.DataFrame, column_name: str ) -> pd.DataFrame:
    """
    Add a new column with unique values acting as a primary key to the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column_name (str): Name of the column for the primary key. Default is 'primary_key'.

    Returns:
        pd.DataFrame: DataFrame with a new column acting as a primary key.
    """
    df[column_name] = range(1, len(df) + 1)
    return df

def drop_nan_and_empty(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Drop NaN values and empty strings from a specified column of a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column (str): Name of the column to drop NaN values and empty strings from.

    Returns:
        pd.DataFrame: DataFrame with NaN values and empty strings dropped from the specified column.
    """
    df[column] = df[column].replace(' ', pd.NA)
    df = df.dropna(subset=[column])
    
    return df




def main():

    list=["INCIDENT_NUMBER","OCCURRED_ON_DATE","OFFENSE_CODE","OFFENSE_CODE_GROUP","OFFENSE_DESCRIPTION","DISTRICT","REPORTING_AREA","SHOOTING","YEAR","MONTH","DAY_OF_WEEK","HOUR","UCR_PART","STREET","Lat","Long","Location"]
    key_name="Ditrict_ID"
    output_path='mohamed-souhail-moughel/Assignement1/crimes-in-boston' 
    columns_newCSV1=["OFFENSE_CODE","OFFENSE_CODE_GROUP"]
    columns_newCSV2=["REPORTING_AREA","Lat","Long","Location"]
    columns_newCSV3=["DISTRICT_KEY","DISTRICT"]
    columns_toclean=["INCIDENT_NUMBER","Lat","Long","Location","STREET"]
    column_weather={
        "time":"OCCURRED_ON_DATE",
        "tavg":"AVG_Temp",
        "tmin":"MIN_Temp",
        "tmax":"MAX_Temp",
        "prcp":"Precipitation"
    }
    col_inner_drop=["wdir","OFFENSE_CODE_GROUP"]
    columns_inner_del=["YEAR","MONTH","DAY_OF_WEEK","Lat","Long","Location","DISTRICT"]

    df = load_csv_to_dataframe("mohamed-souhail-moughel/Assignement1/crimes-in-boston/crime.csv")
    df = project_columns(df, list)
    df = convert_shooting_to_boolean(df)
    df = clean_crime_data(df, columns_toclean)


    new_csv(df, output_path, "offense_data.csv", columns_newCSV1)
    df = drop_nan_and_empty(df, "REPORTING_AREA")
    new_csv(df, output_path, "Location_Reporting.csv", columns_newCSV2)
    df = add_surrogate_key(df)
    new_csv(df, output_path, "District.csv", columns_newCSV3)


    df_weather = read_weather_html_file("mohamed-souhail-moughel/Assignement1/boston_weather_data/boston_weather_data.html")

    df['OCCURRED_ON_DATE'] = pd.to_datetime(df['OCCURRED_ON_DATE']).dt.strftime('%Y-%m-%d')
    df_weather = rename_columns(df_weather, column_weather)
    inner_join_df = join_dataframes(df_weather, df, "OCCURRED_ON_DATE", "inner")
    inner_join_df = select_interesting_rows(inner_join_df)
    inner_join_df = remove_slccolumn(inner_join_df, columns_inner_del)
    inner_join_df = add_primary_key(inner_join_df, "Crime_ID")
    inner_join_df = remove_slccolumn(inner_join_df, col_inner_drop)
    save_dataframe_to_csv(inner_join_df, "mohamed-souhail-moughel/Assignement1", "Crimes_weather.csv")

if __name__ == "__main__":
    main()
