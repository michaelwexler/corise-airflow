from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

from common.week_3.config import DATA_TYPES, normalized_columns


PROJECT_ID = 'airflow-week1-377216'
DESTINATION_BUCKET = 'corise-airflow-wexler'
BQ_DATASET_NAME = 'corise_test'


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "owner": "wexler", # This defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 2, # If a task fails, it will retry 2 times.
    }
    ) 
def data_warehouse_transform_dag_wexler():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """


    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned


        """
        from zipfile import ZipFile
        filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"  # mapped drive
        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]  # wrapping in a list to make a list of dataframes
        return dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        
        client = GCSHook().get_conn()       
        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")  # smart conversion!
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            bucket.blob(f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(df.to_parquet(), "text/parquet")  # using the names from the imported "config.py" file; parquet as string.
            print(df.dtypes)


    @task_group
    def create_bigquery_dataset():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
        # EmptyOperator(task_id='placeholder')
        # TODO Modify here to create a BigQueryDataset if one does not already exist
        # This is where your tables and views will be created
        # exists_ok setting true, ignore error (assuming error is dropped, not new dataset erases existin data!)
        #
        # Returns what?  Hardcoded dataset name at top, so nothing to return?
        # do we need gcp_conn_id=GCS_CONN_ID
        # https://registry.astronomer.io/providers/google/modules/bigquerycreateemptydatasetoperator 

        print("before")
        BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", exists_ok=True, dataset_id=BQ_DATASET_NAME, project_id=PROJECT_ID)
        print("after")




    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        EmptyOperator(task_id='placeholder')

        # TODO Modify here to produce two external tables, one for each data type, referencing the data stored in GCS

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.  

        ### Note that this is __create or replace__: no errors about existing, just overwrote existing defn!!!  OUCHHHHH!
        ###  TODO: This should be a list of ["generation", "weather"] with a loop, or even just normalized_columns.keys()
        
        BigQueryCreateExternalTableOperator(task_id="create_external_tables_1",
                    bucket=DESTINATION_BUCKET,
                    #source_objects="corise-airflow-wexler/week-3/generation.parquet",
                    #destination_project_dataset_table=f"{BQ_DATASET_NAME}.generation", 
                    #source_format="PARQUET", 
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BQ_DATASET_NAME,
                            "tableId": "generation"
                            },
                        "externalDataConfiguration": {
                            "sourceFormat": "PARQUET",
                            "sourceUris":f"gs://corise-airflow-wexler/week-3/generation.parquet",
                            }
                        }
                    )
        
        BigQueryCreateExternalTableOperator(task_id="create_external_tables_2",
                    bucket=DESTINATION_BUCKET,
                    #source_objects="corise-airflow-wexler/week-3/generation.parquet",
                    #destination_project_dataset_table=f"{BQ_DATASET_NAME}.weather", 
                    #source_format="PARQUET", 
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BQ_DATASET_NAME,
                            "tableId": "weather"
                            },
                        "externalDataConfiguration": {
                            "sourceFormat": "PARQUET",
                            "sourceUris":f"gs://corise-airflow-wexler/week-3/weather.parquet",
                            }
                        }
                    )
        


    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to 
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        # pass
        
        first_part=f"SELECT CAST({timestamp_column} AS TIMESTAMP) as {timestamp_column}, "
        second_part =", ".join(columns)
        return  first_part + second_part

        
        


    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant 
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a 
        # select statement ptogrammatically, which can then be passed to the Airflow Operators.
        # EmptyOperator(task_id='placeholder')

        ### Why is the produce_select_statement not in this function?  
        ### TODO: This should be a list of ["generation", "weather"] with a loop, or even just normalized_columns.keys()

        """
        From Scott:
        Looks good, Michael! For the task groups that involved creating multiple Operators -- you can use a loop to create a list of operators:

        tasks = [] for i in DATA_TYPES: < define a task_resource template > tasks.append( < an operator that calls task_resource > )

        and then @task_group can pick up on tasks as it's a list of Operators. :)
        """
        
        #first build the selects for the view for generation.
        view_sql_gen = produce_select_statement(timestamp_column = normalized_columns["generation"]["time"], columns=normalized_columns["generation"]["columns"])
        view_sql_gen = view_sql_gen + f" FROM {BQ_DATASET_NAME}.generation;"

        BigQueryCreateEmptyTableOperator(task_id="create_view_gen",
                                         dataset_id=BQ_DATASET_NAME,
                                         table_id="gen_ts_view",
                                         view={
                                                "query": view_sql_gen,
                                                "useLegacySql":False,
                                            }
                                         )

        #Now build the selects for the view for weather.
        view_sql_gen = produce_select_statement(timestamp_column = normalized_columns["weather"]["time"], columns=normalized_columns["weather"]["columns"])
        view_sql_gen = view_sql_gen + f" FROM {BQ_DATASET_NAME}.weather;"

        BigQueryCreateEmptyTableOperator(task_id="create_view_wea",
                                         dataset_id=BQ_DATASET_NAME,
                                         table_id="wea_ts_view",
                                         view={
                                                "query": view_sql_gen,
                                                "useLegacySql":False,
                                            }
                                         )





    @task_group
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time
        # EmptyOperator(task_id='placeholder')
   
        joined_view_sql = """
        select w.*, g.* 
        from corise_test.wea_ts_view w join corise_test.gen_ts_view g on w.dt_iso=g.time
        """

        BigQueryCreateEmptyTableOperator(task_id="create_joined_view_wea_gen",
                                         dataset_id=BQ_DATASET_NAME,
                                         table_id="wea_gen_view",
                                         view={
                                                "query": joined_view_sql,
                                                "useLegacySql":False,
                                            }
                                         )



    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task

# https://corise.com/course/effective-data-orchestration-with-airflow/v2/module/week-3-project-instructions 
data_warehouse_transform_dag = data_warehouse_transform_dag_wexler()

"""
Ingest data to GCS.

Create a BigQueryDataset programmatically if one doesn't already exist.

Build an external table on top of each of the raw data sources that are stored in GCS.

Produce normalized views on top of the external tables capturing specific columns.

Produce views representing joins on the normalized views.
""" 


"""
normalized_columns.keys() //['generation', 'weather']
normalized_columns["generation"].keys() // dict_keys(['time', 'columns'])
normalilized_columns["generations"]["time"] returns time, the intended type
normalilized_columns["generations"]["columns"] returns a list of columns

Generation format: 

time                                            object
generation_biomass                             float64
generation_fossil_brown_coal_lignite           float64
generation_fossil_coal_derived_gas             float64
generation_fossil_gas                          float64
generation_fossil_hard_coal                    float64
generation_fossil_oil                          float64
generation_fossil_oil_shale                    float64
generation_fossil_peat                         float64
generation_geothermal                          float64
generation_hydro_pumped_storage_aggregated     float64
generation_hydro_pumped_storage_consumption    float64
generation_hydro_run_of_river_and_poundage     float64
generation_hydro_water_reservoir               float64
generation_marine                              float64
generation_nuclear                             float64
generation_other                               float64
generation_other_renewable                     float64
generation_solar                               float64
generation_waste                               float64
generation_wind_offshore                       float64
generation_wind_onshore                        float64
forecast_solar_day_ahead                       float64
forecast_wind_offshore_eday_ahead              float64
forecast_wind_onshore_day_ahead                float64
total_load_forecast                            float64
total_load_actual                              float64
price_day_ahead                                float64
price_actual                                   float64

Weather format
dt_iso                  object
city_name               object
temp                   float64
temp_min               float64
temp_max               float64
pressure                 int64
humidity                 int64
wind_speed               int64
wind_deg                 int64
rain_1h                float64
rain_3h                float64
snow_3h                float64
clouds_all               int64
weather_id               int64
weather_main            object
weather_description     object
weather_icon            object
dtype: object

"""