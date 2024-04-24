import logging
import pandas as pd
import os

def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    try:
        dataframe = pd.read_csv(file_path,encoding='latin-1')
        return dataframe
    except Exception as e:
        logging.error(f"Error loading file: {e}")
        raise

def rename_columns(df: pd.DataFrame, column_names: dict) -> pd.DataFrame:
    """
    Reads a CSV file, renames specified columns, and saves the modified DataFrame back to a CSV file.
    
    Parameters:
        input_file_path (str): The path to the input CSV file.
        output_file_path (str): The path to save the modified CSV file.
    """
    df.rename(columns=column_names, inplace=True)
    return df

def convert_victims_to_boolean(df: pd.DataFrame) -> pd.DataFrame:
  """Converts the 'SHOOTING' column in a DataFrame to boolean (True for 'Y', False otherwise).

  Args:
      df (pd.DataFrame): The DataFrame containing the 'SHOOTING' column.

  Returns:
      pd.DataFrame: The DataFrame with the 'SHOOTING' column converted to boolean.
  """
  df['multiple_victims'] = df['multiple_victims'].str.upper()
  df['multiple_victims'] = df['multiple_victims'].map({'T': 1, 'F': 0})
  return df

def convert_Shooting_to_boolean(df: pd.DataFrame) -> pd.DataFrame:
  """Converts the SHOOTING Type column in a DataFrame to boolean (True for 'FATAL', False otherwise).

  Args:
      df (pd.DataFrame): The DataFrame containing the Shooting_type column.

  Returns:
      pd.DataFrame: The DataFrame with the Shooting_type column converted to boolean.
  """
  df['Shooting_type'] = df['Shooting_type'].str.upper()
  df['Shooting_type'] = df['Shooting_type'].map({'FATAL': 1, 'NON-FATAL': 0})
  return df

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
        df = df[(df["multiple_victims"] == 1) & (df["Gender"] == "Female") ]
        return df
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise

def save_dataframe_to_csv(df: pd.DataFrame, filepath: str) -> None:
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
        df.to_csv(filepath, index=False,mode='w')
        logging.info(f"Result has been saved to the file: {filepath}")
    except Exception as e:
        logging.error(f"Failed to save the result to CSV file: {e}")
        raise

def replace_nan_with_unknown(df: pd.DataFrame, column: list) -> pd.DataFrame:
    """
    Replace NaN values in a specified column of a DataFrame with 'unknown'.

    Args:
        df (pd.DataFrame): DataFrame containing the data.
        column (str): Name of the column to replace NaN values.

    Returns:
        pd.DataFrame: DataFrame with NaN values replaced by 'unknown' in the specified column.
    """
    df[column] = df[column].fillna('unknown')
    return df

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

def main():
    file = 'mohamed-souhail-moughel/Apache Airflow Automation/dags/ShootingsBostonGOV.csv'
    column_DICT = {
        'shooting_type_v2': 'Shooting_type',
        'victim_gender': 'Gender',
        'victim_race': 'Race',
        'victim_ethnicity_NIBRS': 'Ethnicity',
        'multi_victim': 'multiple_victims'
    }
    column_unk = ["Ethnicity", "Race", "Gender", "district"]
    output_file_path = 'output_data.csv'
    
    df_Shot = load_csv_to_dataframe(file)
    df_Shot = rename_columns(df_Shot, column_DICT)
    df_Shot = convert_victims_to_boolean(df_Shot)
    df_Shot = convert_Shooting_to_boolean(df_Shot)
    df_Shot=replace_nan_with_unknown(df_Shot, column_unk)
    save_dataframe_to_csv(df_Shot, "Shootings.csv")


if __name__ == "__main__":
    main()