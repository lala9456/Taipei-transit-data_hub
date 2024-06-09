from google.cloud import bigquery
import os

def MRT_static_data_to_BQ_SRC(client: bigquery.Client , dataset_name:str,table_name:str) -> None:
    """create external table (GCS to BQ) for MRT raw data(mrt_station)"""
    query_job = client.query(
    f"""
        CREATE OR REPLACE EXTERNAL TABLE `{dataset_name}.{table_name}`
        (
            `StationUID` STRING,
            `StationID` STRING,
            `StationAddress` STRING,
            `BikeAllowOnHoliday` BOOL,
            `SrcUpdateTime` TIMESTAMP,
            `UpdateTime` TIMESTAMP,
            `VersionID` STRING,
            `LocationCity` STRING,
            `LocationCityCode` STRING,
            `LocationTown` STRING,
            `LocationTownCode` STRING,
            `StationName_Zh_tw` STRING,
            `StationName_En` STRING,
            `StationPosition_PositionLon` FLOAT64,
            `StationPosition_PositionLat` FLOAT64,
            `StationPosition_GeoHash` STRING            
        )
        OPTIONS 
        (
            format = 'CSV',
            uris = ['gs://static_reference_andy/mrt/mrt_station.csv'],
            skip_leading_rows = 1,
            max_bad_records = 1
        )
    """
    )
    query_job.result()
    print(f"{dataset_name}.{table_name} is created.")

def MRT_static_data_SRC_to_ODS(client: bigquery.Client , 
                               source_dataset_name:str,
                               source_table_name:str,
                               create_dataset_name:str,
                               create_table_name:str) -> None:
    """Transforming the SRC layer of MRT stations into the ODS layer."""
    query_job = client.query(
    f"""
    CREATE OR REPLACE TABLE `{create_dataset_name}.{create_table_name}` AS
        SELECT
            `StationID` AS `mrt_station_id`,
            `StationName_Zh_tw` AS `station_name`,
            `StationName_En` AS `station_en`,
            `StationAddress` AS `station_address`,
            `StationPosition_PositionLat` AS `lat`,
            `StationPosition_PositionLon` AS `lng`,
            `LocationCityCode` AS `city_code`,
            `LocationTown` AS `district`,
            `BikeAllowOnHoliday` AS `bike_allow_on_holiday`,
            `UpdateTime` AS `update_time`         
        FROM 
            `{source_dataset_name}.{source_table_name}`;
    """
    )
    query_job.result()
    print(f"{create_dataset_name}.{create_table_name} is created.")


def MRT_static_ODS_to_DIM(client: bigquery.Client, 
                            source_dataset_name:str,
                            source_table_name:str,
                            create_dataset_name:str,
                            create_table_name:str) -> None:
    """Transforming the ODS layer of bus stations into the DIM layer of bus stations."""
    query_job = client.query(
    f"""
    CREATE OR REPLACE TABLE `{create_dataset_name}.{create_table_name}` AS
    SELECT *
    from `{source_dataset_name}.{source_table_name}`;
    """
    )
    query_job.result()
    print(f"{create_dataset_name}.{create_table_name} is created")

if __name__=="__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\TIR_101_group2_project_andy\Taipei-transit-data_hub\airflow\gcp_credentials\andy-gcs_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()  
    MRT_static_data_to_BQ_SRC(client=BQ_CLIENT,
                            dataset_name="ANDY_ETL_SRC",
                            table_name="SRC_MRT_static_data")
    MRT_static_data_SRC_to_ODS(client=BQ_CLIENT , 
                               source_dataset_name="ANDY_ETL_SRC",
                               source_table_name="SRC_MRT_static_data",
                               create_dataset_name="ANDY_ETL_ODS",
                               create_table_name="ODS_MRT_static_data")
    MRT_static_ODS_to_DIM(client=BQ_CLIENT, 
                        source_dataset_name="ANDY_ETL_ODS",
                        source_table_name="ODS_MRT_static_data",
                        create_dataset_name="ANDY_ETL_DIM",
                        create_table_name="DIM_MRT_static_data")