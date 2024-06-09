import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def DIM_youbike_bike_station_create(dataset_name: str, source_dataset_name: str, create_table_name: str, ods_table_name: str, client: bigquery.Client):
    """create dimention table for bike station information"""
    query_job = client.query(
        f"""
    CREATE OR REPLACE TABLE `{dataset_name}.{create_table_name}` AS
    SELECT 
        `bike_station_id`,
        `station_name`,
        `total_space`,
        `lat`,
        `lng`,
        `district` ,
        `address`,
        `disable`,
         TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR) AS `create_time`,
        `source_time` 
    FROM
        (SELECT 
            * ,
            ROW_NUMBER() OVER (PARTITION by `bike_station_id` ORDER BY `source_time` DESC) AS `row_num`
        FROM `{source_dataset_name}.{ods_table_name}`) AS t1
    WHERE t1.row_num=1
    ;
    """
    )
    query_job.result()
    print(f"{dataset_name}.{create_table_name} has been created")


def FACT_youbike_bike_realtime_create(dataset_name: str, source_dataset_name: str, create_table_name: str, ods_table_name: str, client: bigquery.Client):
    """create or update ods_bike_realtime"""
    query_job = client.query(
        f"""
    CREATE OR REPLACE TABLE `{dataset_name}.{create_table_name}` AS
    SELECT 
        `bike_station_id`,
        `aval_bike`,
        `aval_space`,
         TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR) AS `create_time`,
        `source_time` 
    FROM `{source_dataset_name}.{ods_table_name}`;
    """
    )
    query_job.result()
    print(f"{dataset_name}.{create_table_name} has been created")


if __name__ == "__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\dev_TIR_group2\Taipei-transit-data_hub\airflow\dags\andy-gcs_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    DIM_youbike_bike_station_create(dataset_name="ANDY_ETL_DIM",
                                    source_dataset_name="ANDY_ETL_ODS",
                                    create_table_name="DIM_bike_station",
                                    ods_table_name="ODS_youbike_realtime",
                                    client=BQ_CLIENT)
    FACT_youbike_bike_realtime_create(dataset_name="ANDY_ETL_FACT",
                                      source_dataset_name="ANDY_ETL_ODS",
                                      create_table_name="FACT_bike_realtime",
                                      ods_table_name="ODS_youbike_realtime",
                                      client=BQ_CLIENT)
