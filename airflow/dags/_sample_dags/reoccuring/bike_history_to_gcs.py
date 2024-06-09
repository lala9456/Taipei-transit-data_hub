from datetime import timedelta
from io import BytesIO
import zipfile
import requests
import pandas as pd
from airflow.decorators import dag, python_task, branch_python
from google.cloud import storage
from utils.gcp import gcs
import re
import os
import pendulum
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


CLIENT = storage.Client()
BUCKET_TYPE = os.environ['BUCKET_TYPE']
# BUCKET_TYPE = ''

# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    # basic setting for all dags
    default_args=default_args,
    schedule_interval='0 0 1 * *',  # run first day of every month
    start_date=pendulum.datetime(2024, 3, 10, tz='Asia/Taipei'),
    tags=["bike", "reoccuring"],
    catchup=False)
def bike_history_to_gcs():

    @python_task
    def get_data_listing() -> pd.DataFrame:
        url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike_second_ticket_opendata/YouBikeHis.csv"
        df = pd.read_csv(url)
        return df

    @python_task
    def list_history_files_in_gcs() -> dict:
        bucket_name = f'{BUCKET_TYPE}static_reference'
        bucket = CLIENT.bucket(bucket_name)
        file_list = [file.name for file in bucket.list_blobs(
            prefix='bike_history')]
        return {'filenames': file_list}

    @branch_python.branch_task
    def new_ingestion_check(data):
        '''
        check if there are existing historical data.
        '''
        file_list = data['filenames']
        if len(file_list) == 0:
            return 'extract_all_data'
        else:
            return 'get_most_recent_file_date'

    @python_task
    def get_most_recent_file_date(ti=None) -> dict:
        file_list = ti.xcom_pull(task_ids='list_history_files_in_gcs')[
            'filenames']
        print(file_list)
        dates = []
        for file_name in file_list:
            match = re.search(r'year=(\d+)/month=(\d+)', file_name)
            if match:
                year, month = match.groups()
                dates.append((int(year), int(month)))

        # Find the most recent year and month
        most_recent_date = max(dates)
        most_recent_year, most_recent_month = most_recent_date

        return {'year': most_recent_year,
                'month': most_recent_month}

    @python_task
    def extract_new_rows(df, target_date=None) -> list[str]:
        df['year'] = df['fileinfo'].apply(
            lambda x: int(re.search(r'(\d{4})年', x).group(1)))
        df['month'] = df['fileinfo'].apply(
            lambda x: int(re.search(r'(\d{1,2})月', x).group(1)))

        if target_date is None:
            return df[['fileURL', 'year', 'month']].to_dict(orient='records')

        target_year = target_date['year']
        target_month = target_date['month']

        def is_larger_than(row, target_year, target_month):
            current_year = row['year']
            current_month = row['month']
            if current_year > target_year:
                return True
            elif current_year == target_year and current_month > target_month:
                return True
            else:
                return False
        df['is_larger'] = df.apply(
            is_larger_than, axis=1,
            target_year=target_year,
            target_month=target_month)
        df = df[df['is_larger'] == True]  # noqa
        logging.info(f'there are {len(df)} set of data to process...')
        if len(df) == 0:
            return [None]
        return df[['fileURL', 'year', 'month']].to_dict(orient='records')

    @python_task
    def zip_url_to_gcs(data):
        if data is None:
            logging.info('No data processed...')
            return
        fileURL = data['fileURL']
        filename = f"bike_history/year={data['year']}/month={data['month']}/bike_usage_history.csv"  # noqa
        logging.info(f'ready to process {filename}....')
        try:
            response = requests.get(fileURL)
            zip_content = BytesIO(response.content)

            with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                data = zip_ref.namelist()
                for file in data:
                    if '.csv' in file:
                        file_in_zip = file
                with zip_ref.open(file_in_zip) as csv_file:
                    transform_df = pd.read_csv(
                        csv_file, header=None, encoding_errors='replace')

            first_cell = transform_df.iloc[0, 0]
            is_rent_time = first_cell == 'rent_time'
            is_rent_time
            if is_rent_time:
                transform_df.columns = transform_df.iloc[0]
                transform_df = transform_df.drop(0)
        except Exception as e:
            logging.error(f"data from {filename} can't be processed...")
            logging.error(e)
            raise ConnectionError

        bucket_name = f'{BUCKET_TYPE}static_reference'

        gcs.upload_df_to_gcs(CLIENT,
                             bucket_name,
                             filename,
                             transform_df.reset_index())
        return

    trigger = TriggerDagRunOperator(
        task_id="trigger_etl",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="bike_history_recoccuring_src_ods_fact",
        trigger_rule='one_success'
    )

    file_listing = list_history_files_in_gcs()
    full_df = get_data_listing()
    newest_date = get_most_recent_file_date()
    no_filter_df = extract_new_rows.override(
        task_id='extract_all_data')(full_df, None)
    new_ingestion_check(file_listing) >> [no_filter_df, newest_date]
    extracted_df = extract_new_rows(full_df, newest_date)
    zip_url_to_gcs.override(
        task_id='unzip_all_url').expand(data=no_filter_df) >> trigger
    zip_url_to_gcs.override(
        task_id='unzip_selected_url').expand(data=extracted_df) >> trigger


bike_history_to_gcs()
