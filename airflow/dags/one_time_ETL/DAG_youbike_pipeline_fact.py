from utils.gcp import gcs
# from utils.discord_notifications import DiscordNotifier
# from dotenv import load_dotenv
from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import requests
import pandas as pd
import logging
import pendulum
import os
from pathlib import Path
import sys
from utils.discord_notify_function import notify_failure, notify_success, dag_success_alert, task_failure_alert
from utils.gcp.youbike_create_fact_and_dim_table import FACT_youbike_bike_realtime_create
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/gcp_credentials/andy-gcs_key.json'

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_success_callback': notify_success,
    'on_failure_callback': notify_failure
}


@dag(
    # basic setting for all dags
    dag_id='youbike_pipeline_create_fact',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 4, 10),
    tags=["Youbike", "one_time", "fact" ],
    on_success_callback=dag_success_alert,  # 在 DAG 成功時調用
    on_failure_callback=task_failure_alert,   # 在 DAG 失敗時調用
    catchup=False)
def DAG_bike_pipeline_fact():
    # setup the client that will be use in the dags

    BQ_CLIENT = bigquery.Client()

    @python_task
    def Task_FACT_bike_realtime_create():
        FACT_youbike_bike_realtime_create(dataset_name="ANDY_ETL_FACT",
                                      source_dataset_name="ANDY_ETL_ODS",
                                      create_table_name="FACT_bike_realtime",
                                      ods_table_name="ODS_youbike_realtime",
                                      client=BQ_CLIENT)
        logging.info("FACT_bike_realtime has been create")


    Task_FACT_bike_realtime_create()


# this actually runs the whole DAG
DAG_bike_pipeline_fact()
