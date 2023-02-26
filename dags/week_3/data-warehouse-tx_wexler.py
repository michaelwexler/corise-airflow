from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

from common.week_3.config import DATA_TYPES, normalized_columns


PROJECT_ID = 'airflow-week1-377216'
DESTINATION_BUCKET = 'corise-airflow-wexler'
BQ_DATASET_NAME = 'corise-test'


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "owner": "wexler", # This defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 2, # If a task fails, it will retry 2 times.
    }
    ) 
def data_warehouse_transform_dag():
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

    @task
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
        BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", exists_ok=False, dataset_id=BQ_DATASET_NAME, project_id=PROJECT_ID)




    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        EmptyOperator(task_id='placeholder')

        # TODO Modify here to produce two external tables, one for each data type, referencing the data stored in GCS

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.


    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to 
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        pass

    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant 
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a 
        # select statement ptogrammatically, which can then be passed to the Airflow Operators.
        EmptyOperator(task_id='placeholder')


    @task
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time
        EmptyOperator(task_id='placeholder')


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
data_warehouse_transform_dag = data_warehouse_transform_dag()

"""
Ingest data to GCS.

Create a BigQueryDataset programmatically if one doesn't already exist.

Build an external table on top of each of the raw data sources that are stored in GCS.

Produce normalized views on top of the external tables capturing specific columns.

Produce views representing joins on the normalized views.
""" 
