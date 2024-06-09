import os
from google.cloud import bigquery


def SRC_bus_gcs_to_bq(create_dataset_name: str, create_table_name: str, client: bigquery.Client) -> None:
    """create external table (GCS to BQ) for bus raw data(tpe_bus_station_info_after_api_call)"""
    query_job = client.query(
        f"""
        CREATE OR REPLACE EXTERNAL TABLE `{create_dataset_name}.{create_table_name}`
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
    print("SRC_Bus_static_data is created.")


def BQ_SRC_to_ODS_bus_station(client: bigquery.Client) -> None:
    """Transforming the SRC layer of bus stations into the ODS layer of bus stations."""
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `BUS_GCS_to_BQ_SRC_ODS_DIM.ODS_Bus_static_data` AS
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
        BUS_GCS_to_BQ_SRC_ODS_DIM.SRC_Bus_static_data
    """
    )
    query_job.result()
    print("ODS_Bus_static_data is created")


def BQ_ODS_to_DIM_bus_station(client: bigquery.Client) -> None:
    """Transforming the SRC layer of bus stations into the ODS layer of bus stations."""
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `BUS_GCS_to_BQ_SRC_ODS_DIM.DIM_Bus_static_data` AS
    SELECT *
    from 
        BUS_GCS_to_BQ_SRC_ODS_DIM.ODS_Bus_static_data
    """
    )
    query_job.result()
    print("DIM_Bus_static_data is created")


BQ_ODS_to_DIM_bus_station()

if __name__ == "__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\dev_TIR_group2\Taipei-transit-data_hub\airflow\dags\andy-gcs_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
