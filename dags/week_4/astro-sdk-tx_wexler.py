import pandas as pd
from airflow.models.dag import DAG
from airflow.utils import timezone

import astro.sql as aql
from astro.files import File
from astro.table import Metadata, Table



BQ_DATASET_NAME = "timeseries_energy"

# https://corise.com/course/effective-data-orchestration-with-airflow/v2/module/week-4-project-instructions

time_columns = {
    "generation": "time",
    "weather": "dt_iso"
}

filepaths = {
     "generation": "gs://corise-airflow-wexler/week-3/generation.parquet",
     "weather"  : "gs://corise-airflow-wexler/week-3/weather.parquet"
}


@aql.dataframe
def extract_nonzero_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out columns that have only 0 or null values by 
    calling fillna(0) and only selecting columns that have any non-zero elements
    Fill null values with 0 for filtering and extract only columns that have
    """

    # TODO Modify here
    df=input_df.fillna(0)
    new_df=df.loc[:, (df != 0).any(axis=0)]
    #pass
    return new_df

@aql.transform
def convert_timestamp_columns(input_table: Table, data_type: str):
    """
    Return a SQL statement that selects the input table elements, 
    casting the time column specified in 'time_columns' to TIMESTAMP
    """
    
    # TODO Modify here
    # pass

    # Since we are passing an additional field to parametrize the query, we need
    # to enclose the jinja templated 'input_table' in 4 curly braces !!!!!!!!!!!!
    # return  f"SELECT length_m * {scalar} as length_mm, * except (length_m) FROM {{{{input_table}}}}"

    timestamp_column=time_columns[data_type]
    first_part=f"SELECT CAST({timestamp_column} AS TIMESTAMP) as {timestamp_column}, * EXCEPT ({timestamp_column}) from {{{{input_table}}}}"
    return first_part


    

@aql.transform
def join_tables(generation_table: Table, weather_table: Table):  # skipcq: PYL-W0613
    """
    Join `generation_table` and `weather_table` tables on time to create an output table
    """

    # TODO Modify here    
    #pass
    # Note no need to 4 bracket the tables, since no other parameter insertions...
    print("This is gen")
    print (generation_table)
    print (type(generation_table))
    print("this is wea" )
    print(weather_table)
    print(type(weather_table))
    return "select w.*, g.* from {{weather_table}} w join {{generation_table}} g on w.dt_iso=g.time"


              
with DAG(
    dag_id="astro_sdk_transform_dag_wexler",
    schedule_interval=None,
    start_date=timezone.datetime(2022, 1, 1),
    default_args={
        "owner": "wexler", # This defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 2, # If a task fails, it will retry 2 times.
    }
) as dag:
    """
    ### Astro SDK Transform DAG
    This DAG performs four operations:
        1. Loads parquet files from GCS into BigQuery, referenced by a Table object using aql.load_file
        2. Extracts nonzero columns from that table, using a custom Python function extending aql.dataframe
        3. Converts the timestamp column from that table, using a custom SQL statement extending aql.transform
        4. Joins the two tables produced at step 3 for each datatype on time

    Note that unlike other projects, the relations between objects is left out for you so you can get a more intuitive
    sense for how to work with Astro SDK. For some examples of how it can be used, check out 
    # https://github.com/astronomer/astro-sdk/blob/main/python-sdk/example_dags/example_google_bigquery_gcs_load_and_save.py
    """

    # TODO Modify here
    # TODO:  once it works the long way, make a loop!  Use a for with filepath, gives the index into timecolumns too...


    gen_table = aql.load_file(
        input_file = File(path=filepaths["generation"]),
        output_table = Table(
            # Name of table in BigQuery; no name means temp
            #name="output_table",
            # Metadata object enables a consistent interface across different database backends. In this case, the specified schema corresponds to the 'dataset' concept in BigQuery
            # metadata=Metadata(schema="sample_dataset"), 
            # Specified connection tells the SDK both which connection to use AND what type of database system will be queried since each connection corresponds to a specific system
            conn_id="google_cloud_default", ),
        )

    wea_table = aql.load_file(
        input_file = File(path=filepaths["weather"]),
        output_table = Table(
            # Name of table in BigQuery; no name means temp
            #name="output_table",
            # Metadata object enables a consistent interface across different database backends. In this case, the specified schema corresponds to the 'dataset' concept in BigQuery
            # metadata=Metadata(schema="sample_dataset"), 
            # Specified connection tells the SDK both which connection to use AND what type of database system will be queried since each connection corresponds to a specific system
            conn_id="google_cloud_default", ),
        )

    # Need named parameters, and need output tables!
    gen1_table=extract_nonzero_columns(input_df=gen_table, output_table=Table(conn_id="google_cloud_default",),)
    gen2_table=convert_timestamp_columns(input_table=gen1_table, data_type="generation", output_table=Table(conn_id="google_cloud_default",),)
    wea1_table=extract_nonzero_columns(input_df=wea_table, output_table=Table(conn_id="google_cloud_default",),)
    wea2_table=convert_timestamp_columns(input_table=wea1_table, data_type="weather", output_table=Table(conn_id="google_cloud_default",),)
    
    # @aql.dataframe
    # def testy(df:pd.DataFrame):
    #     print(df.info())
    #     print(df.head())
    # testy(gen_table)
    # testy(gen2_table)
    # testy(wea2_table)

    final_table=join_tables(
        generation_table = gen2_table, 
        weather_table    = wea2_table, 
        output_table = Table(
            conn_id = "google_cloud_default",    
            name = "week_4_final",
            metadata=Metadata(schema="corise_test") # not really schema, just dataset
            )
        )

        
    # Cleans up all temporary tables produced by the SDK
    aql.cleanup()

