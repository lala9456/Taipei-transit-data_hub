
from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum
import os
from pathlib import Path
import sys
from utils.discord_notify_function import notify_failure, notify_success, dag_success_alert, task_failure_alert
from utils.gcp.gcs import upload_df_to_gcs
from utils.etl.mrt_usage_history_update_everymonth import get_usage_history_csvfilelist, get_gcs_filenames_set, mrt_usage_history_one_month
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
    dag_id='mrt_usage_history_update_everymonth',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 4, 10),
    tags=["MRT", "one_time", "history", "GCS"],
    on_success_callback=dag_success_alert,  # 在 DAG 成功時調用
    on_failure_callback=task_failure_alert,   # 在 DAG 失敗時調用
    catchup=False)
def DAG_mrt_usage_history_update_everymonth():
    # setup the client that will be use in the dags

    GCS_CLIENT = storage.Client()

    @python_task
    def Task_get_usage_history_csvfilelist():
        return (get_usage_history_csvfilelist())

    @python_task
    def Task_get_gcs_filenames_set():
        return (get_gcs_filenames_set(
            client=GCS_CLIENT, bucket_name="mrt_history_usage_andy"))

    @python_task
    def Task_check_if_update_necessary(api_url_df, gcs_filenames_set):
        df_need_update = api_url_df.loc[~api_url_df["年月"].isin(
            gcs_filenames_set),]
        return (df_need_update if len(df_need_update) > 0 else False)

    @python_task
    def Task_update_data_not_in_GCS(df_need_update):
        if df_need_update == False:
            logging("No data is necessary updated now")
        else:
            for i in range(len(df_need_update)):
                month = df_need_update.loc[i, "年月"]
                logging(f"we need to update {month}")
                url = df_need_update.loc[i, "URL"]
                df_download_form_api = mrt_usage_history_one_month(url=url)
                upload_df_to_gcs(client=GCS_CLIENT,
                                 bucket_name="mrt_history_usage_andy",
                                 blob_name=f"{month}_mrt_history_usage.csv",
                                 df=df_download_form_api)
                logging(f"{month}'s data has been uploaded to GCS")

    api_url_df = Task_get_usage_history_csvfilelist()
    gcs_filenames_set = Task_get_gcs_filenames_set()
    df_need_update = Task_check_if_update_necessary(
        api_url_df, gcs_filenames_set)
    Task_update_data_not_in_GCS(df_need_update)


DAG_mrt_usage_history_update_everymonth()
