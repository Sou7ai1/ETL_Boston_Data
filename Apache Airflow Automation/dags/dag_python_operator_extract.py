from Transform_Shootings import (convert_victims_to_boolean, convert_Shooting_to_boolean, add_primary_key,
                                 load_csv_to_dataframe, select_interesting_rows, replace_nan_with_unknown,
                                 rename_columns, save_dataframe_to_csv)
from airflow.utils.decorators import apply_defaults
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Extract_BotonGOV import fetch_csv_content_BostonGOV, save_as_csv
from bulk import DimRegionsQueries
from Extract_Kaggle import download_dataset_kaggle
from Transform_crimesboston import *
from Extract_Weather import *



def extract_shootings_dataset(url: str, ti: TaskInstance):
    output_file_path = f"dataset_regions_{ti.run_id}.csv"
    try:
        html_content = fetch_csv_content_BostonGOV(url)
        save_as_csv(html_content, output_file_path)
        ti.xcom_push(key="initial_dataset", value=output_file_path)
        logging.info("File was successfully saved as " + output_file_path)
    except Exception:
        logging.error("Failed to download file content.")


def extract_kaggle_dataset(dataset_name: str, ti: TaskInstance):
    output_file_path = f"dataset_kaggle_{ti.run_id}.csv"
    try:
        download_dataset_kaggle(dataset_name)
        ti.xcom_push(key="initial_kaggledataset", value=output_file_path)
        logging.info("File was successfully saved  ")
    except Exception:
        logging.error("Failed to download file content.")


def extract_weather(meteostation: str, ti: TaskInstance):
    output_file_path = f"weather_{ti.run_id}.csv"
    try:
        start = datetime(2013, 3, 1)
        end = datetime(2024, 1, 1)
        meteostation = "72509"
        content = fetch_weather_data(meteostation, start, end)
        save_as_csv(content, output_file_path)
        ti.xcom_push(key="weatherdataset", value=output_file_path)
        logging.info("File was successfully saved as " + output_file_path)
    except Exception:
        logging.error("Failed to download file content.")


def transform_kaggle_dataset(ti: TaskInstance):
    input_file_path = ti.xcom_pull(
        task_ids="extractkaggle_task", key="initial_kaggledataset"
    )
    input_file_path_weather = ti.xcom_pull(
        task_ids="extractweather_task", key="initial_weatherdatset"
    )
    output_file_path1 = f"offense_data_{ti.run_id}.csv"
    output_file_path2 = f"location_{ti.run_id}.csv"
    output_file_path3 = f"dsitrict_{ti.run_id}.csv"
    output_file_path4 = f"Crimes_weather_{ti.run_id}.csv"
    try:
        list = ["INCIDENT_NUMBER", "OCCURRED_ON_DATE", "OFFENSE_CODE", "OFFENSE_CODE_GROUP", "OFFENSE_DESCRIPTION", "DISTRICT",
                "REPORTING_AREA", "SHOOTING", "YEAR", "MONTH", "DAY_OF_WEEK", "HOUR", "UCR_PART", "STREET", "Lat", "Long", "Location"]
        columns_toclean = ["INCIDENT_NUMBER",
                           "Lat", "Long", "Location", "STREET"]
        columns_newCSV1 = ["OFFENSE_CODE", "OFFENSE_CODE_GROUP"]
        columns_newCSV2 = ["REPORTING_AREA", "Lat", "Long", "Location"]
        columns_newCSV3 = ["DISTRICT_KEY", "DISTRICT"]
        column_weather = {
            "time": "OCCURRED_ON_DATE",
            "tavg": "AVG_Temp",
            "tmin": "MIN_Temp",
            "tmax": "MAX_Temp",
            "prcp": "Precipitation"
        }
        columns_inner_del = ["YEAR", "MONTH", "DAY_OF_WEEK",
                             "Lat", "Long", "Location", "DISTRICT"]
        dataframe = load_csv_to_dataframe(input_file_path)
        dataframe = project_columns(dataframe, list)
        dataframe = convert_Shooting_to_boolean(dataframe)
        dataframe = clean_crime_data(dataframe, columns_toclean)
        dataframe = add_surrogate_key(dataframe)
        dataframe_offense = new_csv(dataframe, columns_newCSV1)
        save_dataframe_to_csv(dataframe_offense, output_file_path1)
        ti.xcom_push(key="offense", value=output_file_path1)
        logging.info(
            "Transformed dataset was successfully saved as " + output_file_path1
        )
        dataframe = drop_nan_and_empty(dataframe, "REPORTING_AREA")
        dataframe_location = new_csv(dataframe, columns_newCSV2)
        save_as_csv(dataframe_location, output_file_path2)
        ti.xcom_push(key="location", value=output_file_path2)
        logging.info(
            "Transformed dataset was successfully saved as " + output_file_path2
        )
        dataframe = add_surrogate_key(dataframe)
        dataframe_district = new_csv(dataframe_location, columns_newCSV3)
        save_dataframe_to_csv(dataframe_district, output_file_path3)
        ti.xcom_push(key="district", value=output_file_path3)
        logging.info(
            "Transformed dataset was successfully saved as " + output_file_path2
        )
        logging.info(
            "Transformed dataset was successfully saved as " + output_file_path2
        )
        dataframe_initial_weather = load_csv_to_dataframe(
            input_file_path_weather)
        dataframe['OCCURRED_ON_DATE'] = pd.to_datetime(
            dataframe['OCCURRED_ON_DATE']).dt.strftime('%Y-%m-%d')
        dataframe_initial_weather = rename_columns(
            dataframe_initial_weather, column_weather)
        join_dfs = join_dataframes(
            dataframe_initial_weather, dataframe, "OCCURRED_ON_DATE", "inner")
        join_dfs = select_interesting_rows(join_dfs)
        join_dfs = remove_slccolumn(join_dfs, columns_inner_del)
        join_dfs = add_primary_key(join_dfs, "Crime_ID")
        join_dfs = remove_slccolumn(join_dfs, columns_inner_del)
        save_as_csv(join_dfs, output_file_path4)
    except Exception as e:
        logging.error(f"Failed to process the data: {e}")


def transform_shootings_dataset(ti: TaskInstance):
    input_file_path = ti.xcom_pull(
        task_ids="extract_shootings_task", key="initial_dataset"
    )
    output_file_path = f"dim_regions_{ti.run_id}.csv"

    try:
        column_DICT = {
            'shooting_type_v2': 'Shooting_type',
            'victim_gender': 'Gender',
            'victim_race': 'Race',
            'victim_ethnicity_NIBRS': 'Ethnicity',
            'multi_victim': 'multiple_victims'
        }
        column_unk = ["Ethnicity", "Race", "Gender", "district"]
        dataframe = load_csv_to_dataframe(
            input_file_path)
        dataframe = rename_columns(dataframe, column_DICT)
        dataframe = convert_victims_to_boolean(dataframe)
        dataframe = convert_Shooting_to_boolean(dataframe)
        dataframe = select_interesting_rows(dataframe)
        dataframe = replace_nan_with_unknown(dataframe, column_unk)
        save_dataframe_to_csv(dataframe, output_file_path)
        ti.xcom_push(key="crimes_weather", value=output_file_path)
        logging.info(
            "Transformed dataset was successfully saved as " + output_file_path
        )
        print(dataframe)
    except Exception as e:
        logging.error(f"Failed to process the data: {e}")


class PostgresBulkLoadOperator(BaseOperator):
    """
    Custom PostgresOperator for bulk loading data into PostgreSQL.
    """

    template_fields = ("table_name", "file_path")

    @apply_defaults
    def __init__(
        self,
        *,
        postgres_conn_id: str,
        table_name: str,
        file_path: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.file_path = file_path

    def execute(self, context):
        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with open(self.file_path, "r") as f:
                columns = f.readline().strip().split(",")
                copy_sql = f"COPY {self.table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER"
                hook.copy_expert(copy_sql, f.name)
        except FileNotFoundError:
            logging.error(f"File '{self.file_path}' not found.")
            raise
        except Exception as ex:
            logging.error(
                f"An error occurred while reading data from file: {ex}")
            raise


default_args = {"owner": "moughel", "retries": 3,
                "retry_delay": timedelta(minutes=5)}


with DAG(
    dag_id="dag_postgre_operator",
    default_args=default_args,
    start_date=datetime(2024, 3, 22),
    schedule_interval="@daily",
) as dag:
    extract_shootings_task = PythonOperator(
        task_id="extract_shootings_task",
        python_callable=extract_kaggle_dataset,
        op_kwargs={
            "url": "https://data.boston.gov/dataset/e63a37e1-be79-4722-89e6-9e7e2a3da6d1/resource/73c7e069-701f-4910-986d-b950f46c91a1/download/tmp8mntlmrz.csv",
        },
    )
    extract_crimes_task = PythonOperator(
        task_id="extractkaggle_task",
        python_callable=extract_kaggle_dataset,
        op_kwargs={
            "dataset_name": "AnalyzeBoston/crimes-in-boston",
        },
    )
    extract_weather_task = PythonOperator(
        task_id="extractweather_task",
        python_callable=extract_weather,
        op_kwargs={
            "meteostation": "",
        },
    )

transform_Shootings_task = PythonOperator(
    task_id="transform_Shootings_task",
    python_callable=transform_shootings_dataset,
)

transform_kaggle_task = PythonOperator(
    task_id="transform_kaggle_dataset_task",
    python_callable=transform_kaggle_dataset,
)


drop_table_task = PostgresOperator(
    task_id="drop_table_task",
    sql=DimRegionsQueries.drop_table_Shootings_query,
    postgres_conn_id="postgres_webik",
)

create_table_task = PostgresOperator(
    task_id="create_table_task",
    sql=DimRegionsQueries.create_table_Shootings_query,
    postgres_conn_id="postgres_webik",
)

alter_table_task = PostgresOperator(
    task_id="alter_table_task",
    sql=DimRegionsQueries.alter_shooting_query,
    postgres_conn_id="postgres_webik",
)


drop_tabledistrict_task = PostgresOperator(
    task_id="drop_tabledistrict_task",
    sql=DimRegionsQueries.drop_table_district_query,
    postgres_conn_id="postgres_webik",
)

create_tabledistrict_task = PostgresOperator(
    task_id="create_tabledistrict_task",
    sql=DimRegionsQueries.create_table_district_query,
    postgres_conn_id="postgres_webik",
)

alter_tabledistrict_task = PostgresOperator(
    task_id="alter_tabledistrict_task",
    sql=DimRegionsQueries.alter_district_query,
    postgres_conn_id="postgres_webik",
)


drop_tablelocation_task = PostgresOperator(
    task_id="drop_tablelocation_task",
    sql=DimRegionsQueries.drop_table_location_query,
    postgres_conn_id="postgres_webik",
)

create_tablelocation_task = PostgresOperator(
    task_id="create_tablelocationt_task",
    sql=DimRegionsQueries.create_table_location_query,
    postgres_conn_id="postgres_webik",
)

alter_tablelocation_task = PostgresOperator(
    task_id="alter_tablelocation_task",
    sql=DimRegionsQueries.alter_location_query,
    postgres_conn_id="postgres_webik",
)


drop_tableoffense_task = PostgresOperator(
    task_id="drop_tableoffense_task",
    sql=DimRegionsQueries.drop_table_offense_query,
    postgres_conn_id="postgres_webik",
)

create_tableoffense_task = PostgresOperator(
    task_id="create_tableoffense_task",
    sql=DimRegionsQueries.create_table_offense_query,
    postgres_conn_id="postgres_webik",
)

alter_tableoffense_task = PostgresOperator(
    task_id="alter_offense_task",
    sql=DimRegionsQueries.alter_offense_query,
    postgres_conn_id="postgres_webik",
)


drop_tablecrimesweather_task = PostgresOperator(
    task_id="drop_tablecrimesweather_task",
    sql=DimRegionsQueries.drop_table_crimes_weather_query,
    postgres_conn_id="postgres_webik",
)

create_tablecrimesweather_task = PostgresOperator(
    task_id="create_tablecrimesweather_task",
    sql=DimRegionsQueries.create_table_crimes_weather_query,
    postgres_conn_id="postgres_webik",
)

alter_tablecrimesweather_task = PostgresOperator(
    task_id="alter_tablecrimesweather_task",
    sql=DimRegionsQueries.alter_crimes_weather_query,
    postgres_conn_id="postgres_webik",
)   


insert_data_task = PostgresBulkLoadOperator(
    task_id="insert_data_task",
    postgres_conn_id="postgres_webik",
    table_name="shooting",
    file_path="{{ ti.xcom_pull(task_ids='transform_Shootings_task', key='dim_regions') }}",
)

insert_datadistrict_task = PostgresBulkLoadOperator(
    task_id="insert_datadistrict_task",
    postgres_conn_id="postgres_webik",
    table_name="district",
    file_path="{{ ti.xcom_pull(task_ids='transform_kaggle_task', key='district') }}",
)

insert_dataoffense_task = PostgresBulkLoadOperator(
    task_id="insert_datadistrict_task",
    postgres_conn_id="postgres_webik",
    table_name="offense",
    file_path="{{ ti.xcom_pull(task_ids='transform_kaggle_task', key='offense') }}",
)

insert_datalocation_task = PostgresBulkLoadOperator(
    task_id="insert_datalocation_task",
    postgres_conn_id="postgres_webik",
    table_name="location",
    file_path="{{ ti.xcom_pull(task_ids='transform_kaggle_task', key='location') }}",
)

insert_datacrimesweather_task = PostgresBulkLoadOperator(
    task_id="insert_datacrimesweather_task",
    postgres_conn_id="postgres_webik",
    table_name="crimes_weather",
    file_path="{{ ti.xcom_pull(task_ids='transform_kaggle_task', key='crimes_weather') }}",
)




(
    extract_shootings_task
    >> extract_crimes_task
    >> extract_weather_task
    >> transform_kaggle_task
    >> transform_Shootings_task
    >> drop_table_task
    >> drop_tablecrimesweather_task
    >> drop_tabledistrict_task
    >> drop_tablelocation_task    
    >> drop_tableoffense_task
    >> create_table_task
    >> create_tablecrimesweather_task
    >> create_tabledistrict_task
    >> create_tablelocation_task
    >> create_tableoffense_task
    >> insert_data_task
    >> insert_datadistrict_task
    >> insert_dataoffense_task
    >> insert_dataoffense_task
    >> insert_datacrimesweather_task
    >> alter_table_task
    >> alter_tabledistrict_task
    >> alter_tableoffense_task
    >> alter_tablelocation_task
    >> alter_tablecrimesweather_task    
)
