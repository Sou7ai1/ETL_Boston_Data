import json
import logging
import sys
from typing import Any, Dict
import pandas as pd
from psycopg2 import Error, connect
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def read_config_file(config_file: str) -> Dict[str, Any]:
    """
    Read database connection parameters from a JSON configuration file.

    Args:
        config_file (str): Path to the JSON configuration file.

    Returns:
        dict: Dictionary containing database connection parameters.
    """
    try:
        with open(config_file, "r") as file:
            config = json.load(file)
    except FileNotFoundError:
        logging.error(f"Config file '{config_file}' not found.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading config file: {e}")
        raise
    return config


def read_data_from_file(file_path: str) -> pd.DataFrame:
    """
    Read data from a CSV file using Pandas.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data.
    """
    try:
        data_df = pd.read_csv(file_path, dtype=str)
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading data from file: {e}")
        raise
    return data_df


def execute_ddl(conn_params: Dict[str, Any], ddl_statement: str) -> None:
    """
    Create, drop or alter the table.

    Args:
        conn_params (dict): Connection parameters for the database.
        ddl_statement (str): SQL query to create, drop or alter the table.
    """
    try:
        conn = connect(**conn_params)
        cur = conn.cursor()

        cur.execute(ddl_statement)
        conn.commit()
    except Error as e:
        logging.error(f"Error altering table: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def execute_insert(conn_params: Dict[str, Any], insert_query: str, data: pd.DataFrame) -> None:
    """
    Execute insertion of data into the database.

    Args:
        conn_params (dict): Connection parameters for the database.
        insert_query (str): SQL query for insertion.
        data (pd.DataFrame): DataFrame containing data to be inserted.
    """
    try:
        conn = connect(**conn_params)
        cur = conn.cursor()

        # Convert DataFrame to list of tuples

        # Execute insertion using execute_values
        execute_values(cur, insert_query, data.values)

        conn.commit()
    except Error as e:
        logging.error(f"Error inserting data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


class DimRegionsQueries:
    """
    Contains SQL queries related to the dim_regions table.
    """

    drop_table_crimes_weather_query = """
    DROP TABLE IF EXISTS crimes_weather CASCADE;
    """
    drop_table_Shootings_query = """
    DROP TABLE IF EXISTS shooting CASCADE;
    """
    drop_table_district_query = """
    DROP TABLE IF EXISTS district CASCADE;
    """
    drop_table_offense_query = """
    DROP TABLE IF EXISTS offense CASCADE;
    """
    drop_table_location_query = """
    DROP TABLE IF EXISTS location CASCADE;
    """

    create_table_Shootings_query = """
    CREATE TABLE shooting (
    incident_ID SERIAL,
    incident_num VARCHAR(100) ,
    shooting_date VARCHAR(100),
    district varchar(100),
    Shooting_type INTEGER,
    Gender VARCHAR(100),
    Race VARCHAR(100),
    Ethnicity VARCHAR(100),
    multiple_victims INTEGER
    );
    """
    create_table_district_query = """
    CREATE TABLE district (
    district_key INTEGER ,
    district VARCHAR(250)
    );
    """
    create_table_offense_query = """
    CREATE TABLE offense (
    offense_code INTEGER ,
    offense_code_group VARCHAR(250)
    );
    """
    create_table_location_query = """
    CREATE TABLE location (
    reporting_area INTEGER ,
    Lat FLOAT,
    Long FLOAT,
    Location VARCHAR(250) 
    );
    """
    create_table_crimes_weather_query = """
    CREATE TABLE crimes_weather (
        CRIME_ID SERIAL PRIMARY KEY,
        INCIDENT_NUMBER VARCHAR(200),
        Occurred_on_date VARCHAR(200),
        OFFENSE_CODE INTEGER,
        OFFENSE_DESCRIPTION VARCHAR(255),
        SHOOTING INTEGER,
        HOUR INTEGER,
        UCR_PART VARCHAR(255),
        STREET VARCHAR(255),
        DISTRICT_KEY INTEGER,
        AVG_Temp FLOAT,
        MIN_Temp FLOAT,
        MAX_Temp FLOAT,
        Precipitation FLOAT,
        wspd FLOAT,
        pres FLOAT,
        REPORTING_AREA INTEGER
    );
    """

    insert_crimes_weather_query = """
    INSERT INTO crimes_weather (
    Occurred_on_date,
    AVG_Temp,
    MIN_Temp,
    MAX_Temp,
    Precipitation,
    wspd,
    pres,
    INCIDENT_NUMBER,
    OFFENSE_CODE,
    OFFENSE_DESCRIPTION,
    REPORTING_AREA,
    SHOOTING,
    HOUR,
    UCR_PART,
    STREET,
    DISTRICT_KEY,
    CRIME_ID)
    VALUES %s;
    """

    insert_shootings_query = """
    INSERT INTO shooting (incident_num, shooting_date, district,Shooting_type,Gender,Race,Ethnicity,multiple_victims)
    VALUES %s;
    """
    insert_district_query = """
    INSERT INTO district (district_key, district)
    VALUES %s;
    """
    insert_offense_query = """
    INSERT INTO offense (offense_code, offense_code_group)
    VALUES %s;
    """
    insert_location_query = """
    INSERT INTO location (reporting_area,Lat,Long,Location)
    VALUES %s;
    """
    alter_shooting_query = """
    ALTER TABLE shooting
    ADD CONSTRAINT incident_pk PRIMARY KEY (incident_ID);
    """

    alter_district_query = """
    ALTER TABLE district 
    ADD CONSTRAINT district_pk PRIMARY KEY (district_key);
    """

    alter_offense_query = """
    ALTER TABLE offense 
    ADD CONSTRAINT offense_pk PRIMARY KEY (offense_code);
    """
    alter_location_query = """
    ALTER TABLE location 
    ADD CONSTRAINT reporting_area_pk PRIMARY KEY (reporting_area);
    """

    alter_crimes_weather_query = """
    ADD CONSTRAINT CRIME_pk PRIMARY KEY (CRIME_ID),
    ADD CONSTRAINT fk_offense_code FOREIGN KEY (OFFENSE_CODE) REFERENCES offense(offense_code),
    ADD CONSTRAINT fk_district_key FOREIGN KEY (DISTRICT_KEY) REFERENCES district(district_key);
    
    """


def main():
    conn_params = read_config_file("Assignement1/connection.json")
    data_Shootings = read_data_from_file("Assignement1/Output/Shootings.csv")
    execute_ddl(conn_params, DimRegionsQueries.drop_table_Shootings_query)
    execute_ddl(conn_params, DimRegionsQueries.create_table_Shootings_query)
    execute_insert(
        conn_params, DimRegionsQueries.insert_shootings_query, data_Shootings)
    execute_ddl(conn_params, DimRegionsQueries.alter_shooting_query)
    data_district = read_data_from_file(
        "Assignement1/crimes-in-boston/District.csv")
    execute_ddl(conn_params, DimRegionsQueries.drop_table_district_query)
    execute_ddl(conn_params, DimRegionsQueries.create_table_district_query)
    execute_insert(
        conn_params, DimRegionsQueries.insert_district_query, data_district)
    data_offense = read_data_from_file(
        "Assignement1/crimes-in-boston/offense_data.csv")
    execute_ddl(conn_params, DimRegionsQueries.drop_table_offense_query)
    execute_ddl(conn_params, DimRegionsQueries.create_table_offense_query)
    execute_insert(
        conn_params, DimRegionsQueries.insert_offense_query, data_offense)
    data_location = read_data_from_file(
        "Assignement1/crimes-in-boston/Location_Reporting.csv")
    execute_ddl(conn_params, DimRegionsQueries.drop_table_location_query)
    execute_ddl(conn_params, DimRegionsQueries.create_table_location_query)
    execute_insert(
        conn_params, DimRegionsQueries.insert_location_query, data_location)
    data_crimes_weather = read_data_from_file(
        "Assignement1/Output/Crimes_weather.csv")
    execute_ddl(conn_params, DimRegionsQueries.drop_table_crimes_weather_query)
    execute_ddl(conn_params, DimRegionsQueries.create_table_crimes_weather_query)
    execute_insert(
        conn_params, DimRegionsQueries.insert_crimes_weather_query, data_crimes_weather)
