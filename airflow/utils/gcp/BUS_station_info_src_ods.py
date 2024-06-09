from google.cloud import bigquery
import os

def BUS_static_to_BQ_SRC(client: bigquery.Client , dataset_name:str,table_name:str) -> None:
    """create external table (GCS to BQ) for bus raw data(tpe_bus_station_info_after_api_call)"""
    query_job = client.query(
    f"""
        CREATE OR REPLACE EXTERNAL TABLE `{dataset_name}.{table_name}`
        (
            `bus_station_id` INT,
            `station_name` STRING,
            `address` STRING,
            `city_code` STRING,
            `district` STRING,
            `lat` FLOAT64,
            `lng` FLOAT64,
            `bearing` STRING,
            `subarea` STRING
        )
        OPTIONS 
        (
            format = 'CSV',
            uris = ['gs://static_reference_andy/bus/tpe_bus_station_info_after_api_call.csv'],
            skip_leading_rows = 1,
            max_bad_records = 1
        )
    """
    )
    query_job.result()
    print(f"{dataset_name}.{table_name} is created.")

def BUS_static_SRC_to_ODS(client: bigquery.Client , 
                        source_dataset_name:str,
                        source_table_name:str,
                        create_dataset_name:str,
                        create_table_name:str) -> None:
    """Transforming the SRC layer of bus stations into the ODS layer of bus stations."""
    query_job = client.query(
    f"""
    CREATE OR REPLACE TABLE `{create_dataset_name}.{create_table_name}` AS
    SELECT
        `bus_station_id`,
        TRIM(`station_name`) AS `station_name`,
        TRIM(`address`) AS `address`,
        TRIM(`city_code`) AS `city_code`,
        TRIM(`district`) AS `district`,
        `lat`,
        `lng`,
        TRIM(`bearing`) AS `bearing`,
        CURRENT_TIMESTAMP() AS update_time
    from 
        `{source_dataset_name}.{source_table_name}`
    """
    )
    query_job.result()
    print(f"{create_dataset_name}.{create_table_name} is created")


def BUS_static_ODS_to_DIM(client: bigquery.Client, 
                            source_dataset_name:str,
                            source_table_name:str,
                            create_dataset_name:str,
                            create_table_name:str) -> None:
    """Transforming the ODS layer of bus stations into the DIM layer of bus stations."""
    query_job = client.query(
    f"""
    CREATE OR REPLACE TABLE `{create_dataset_name}.{create_table_name}` AS
    SELECT *
    from `{source_dataset_name}.{source_table_name}`
    """
    )
    query_job.result()
    print(f"{create_dataset_name}.{create_table_name} is created")

if __name__=="__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\TIR_101_group2_project_andy\Taipei-transit-data_hub\airflow\gcp_credentials\andy-gcs_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()  
    BUS_static_to_BQ_SRC(client=BQ_CLIENT,
                        dataset_name="ANDY_ETL_SRC",
                        table_name="SRC_Bus_static_data")
    BUS_static_SRC_to_ODS(client=BQ_CLIENT , 
                            source_dataset_name="ANDY_ETL_SRC",
                            source_table_name="SRC_Bus_static_data",
                            create_dataset_name="ANDY_ETL_ODS",
                            create_table_name="ODS_Bus_static_data")
    BUS_static_ODS_to_DIM(client=BQ_CLIENT, 
                            source_dataset_name="ANDY_ETL_ODS",
                            source_table_name="ODS_Bus_static_data",
                            create_dataset_name="ANDY_ETL_DIM",
                            create_table_name="DIM_Bus_static_data")