from utils.gcp import gcs
# from utils.discord_notifications import DiscordNotifier
# from dotenv import load_dotenv
from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import pandas as pd
import logging
import pendulum
import os
from pathlib import Path
import sys
from utils.discord_notify_function import notify_failure, notify_success, dag_success_alert, task_failure_alert
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/gcp_credentials/andy-gcs_key.json'

def create_time_table(client:bigquery.Client,dataset_name:str,table_name:str,start_date:str,end_date:str):
    query_job = client.query(
        f""" 
        CREATE OR REPLACE TABLE `{dataset_name}.{table_name}`
        (   `date` DATE,
            `year` INT,
            `month` INT,
            `day` INT ,
            `day_of_week` INT,
            `week_of_year` INT,
            `month_name` STRING,
            `day_of_week_name` STRING
        );
        """
    )
    query_job.result()
    print(f"create time table with schema in {dataset_name}.{table_name}")
    query_job = client.query(
        f""" 
        INSERT INTO `{dataset_name}.{table_name}`(
            date,year,month,day,day_of_week,week_of_year,month_name,day_of_week_name
        )
        SELECT
            `date`,
            EXTRACT(YEAR FROM `date`),
            EXTRACT(MONTH FROM `date`),
            EXTRACT(DAY FROM `date`),
            EXTRACT(DAYOFWEEK FROM `date`),
            EXTRACT(WEEK FROM `date`),
            FORMAT_DATE('%B',`date`),
            FORMAT_DATE('%A',`date`)
        FROM 
            UNNEST(GENERATE_DATE_ARRAY('{start_date}', '{end_date}')) AS `date`;
        """
    )
    query_job.result()


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_success_callback': notify_success,
    'on_failure_callback': notify_failure
}


@dag(
    # basic setting for all dags
    dag_id='cteate_time_table_dim',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 4, 10),
    tags=["timetable", "one_time", "dim" ],
    on_success_callback=dag_success_alert,  # 在 DAG 成功時調用
    on_failure_callback=task_failure_alert,   # 在 DAG 失敗時調用
    catchup=False)


def DAG_create_time_table_dim():
    # setup the client that will be use in the dags

    BQ_CLIENT = bigquery.Client()

    @python_task
    def Task_DIM_time_table_create():
        create_time_table(
            client=BQ_CLIENT,
            dataset_name="ANDY_ETL_DIM",
            table_name="DIM_time_table",
            start_date="2020-01-01",
            end_date="2030-01-01"
        )
        logging.info("DIM_time_table has been create")


    Task_DIM_time_table_create()


# this actually runs the whole DAG
DAG_create_time_table_dim()
