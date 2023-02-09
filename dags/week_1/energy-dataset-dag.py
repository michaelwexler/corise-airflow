from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@daily",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "owner": "community", # This defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using two simple tasks to extract data from a zipped folder
    and load it to GCS.

    """

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned.

        """
        from zipfile import ZipFile
        # TODO Unzip files into pandas dataframes
        zip = ZipFile("/Users/michaelwexler/Documents/Code/corise-airflow/dags/data/energy-consumption-generation-prices-and-weather.zip")
        frames=[]

        # open zipped dataset
        with zip as z:
            for name in z.namelist():
                # open the csv file in the dataset
                with z.open(name) as f:
                    # read the dataset
                    dataset = pd.read_csv(f)
                    # display dataset
                    frames.append(dataset)
        
        return frames

    @task
    def transform(unzip_result: List[pd.DataFrame]) -> List[pd.DataFrame]:
        return unzip_result


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then writes the data into GCS as parquet files.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        import tempfile
        import os

        data_types = ['generation', 'weather']

        # GCSHook uses google_cloud_default connection by default, so we can easily create a GCS client using it
        # https://github.com/apache/airflow/blob/207f65b542a8aa212f04a9d252762643cfd67a74/airflow/providers/google/cloud/hooks/gcs.py#L133

        # The google cloud storage github repo has a helpful example for writing from pandas to GCS:
        # https://github.com/googleapis/python-storage/blob/main/samples/snippets/storage_fileio_pandas.py
        
        client = GCSHook()     \
        # TODO Add GCS upload code
        c=0
        bucket_name='corise-airflow-wexler'
        with tempfile.TemporaryDirectory() as tmpdir:
          print(f"directory {tmpdir.name} created")
          for obj in data_types:
            obj_name=obj+'.parquet'
            df=unzip_result[c]   # This was the passed in list
            print(df.info()) # prints schema
            local_name=os.path.join(tmpdir, obj_name)
            df.to_parquet(local_name)
            client.upload(bucket_name, obj_name, local_name)
            c=c+1
        


        

    # TODO Add task linking logic here
    energy_raw_data = extract()
    energy_transformed_data = transform(energy_raw_data)
    load(energy_transformed_data)

energy_dataset_dag = energy_dataset_dag()