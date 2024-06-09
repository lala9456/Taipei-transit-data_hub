import requests
import pandas as pd
import os
from datetime import datetime
import re
from io import StringIO
from google.cloud import storage
from .utils.gcp.gcs import list_blobs, upload_df_to_gcs

# mrt_usage_history
# get csv download of every month's data
# each url can get one month's data


def get_usage_history_csvfilelist():
    url = "https://data.taipei/api/dataset/63f31c7e-7fc3-418b-bd82-b95158755b4d/resource/eb481f58-1238-4cff-8caa-fa7bb20cb4f4/download"
    response = requests.get(url=url)
    response_list = response.text.split("\r")

    col_name = response_list[0].split(",")
    url_df = pd.concat([pd.DataFrame([response_list[i].split(
        ",")[1:]], columns=col_name[1:]) for i in range(1, len(response_list))], axis=0)
    url_df.reset_index(drop=True, inplace=True)
    print("get MRT usage history csvfilelist")
    return (url_df)

def get_gcs_filenames_set(client:storage.Client,bucket_name:str):
    blobs_lists = list_blobs(client=client,bucket_name=bucket_name)  #"mrt_history_usage_andy"
    gcs_filenames_set = set([lst.name.split("_")[0] for lst in blobs_lists])
    return(gcs_filenames_set)



# def T_mrt_usage_history_one_month_apply_reduce(url):
#     response = requests.get(url=url)
#     StringIO_df = StringIO(response.content.decode("utf-8-sig"))
#     df = pd.read_csv(StringIO_df)
#     pattern = re.compile(r"[A-Za-z]+")
#     df["進站"] = df["進站"].str.replace(pattern, "", regex=True)
#     df["出站"] = df["出站"].str.replace(pattern, "", regex=True)
#     df_enter = pd.DataFrame(df.groupby(["日期", "時段", "進站"])[
#                             "人次"].sum()).reset_index(drop=False)
#     df_out = pd.DataFrame(df.groupby(["日期", "時段", "出站"])[
#         "人次"].sum()).reset_index(drop=False)
#     df_enter.rename(columns={
#         "日期": "date",
#         "時段": "hour",
#         "進站": "mrt_station_name",
#         "人次": "enter_count"
#     }, inplace=True)

#     df_out.rename(columns={
#         "日期": "date",
#         "時段": "hour",
#         "出站": "mrt_station_name",
#         "人次": "exit_count"
#     }, inplace=True)
#     df = df_enter.merge(df_out,
#                         left_on=["date", "hour", "mrt_station_name"],
#                         right_on=["date", "hour", "mrt_station_name"],
#                         how="outer")
#     print("T_mrt_usage_history_one_month finished")
#     return (df)


def mrt_usage_history_one_month(url: str):
    response = requests.get(url=url)
    StringIO_df = StringIO(response.content.decode("utf-8-sig"))
    df = pd.read_csv(StringIO_df)
    pattern = re.compile(r"[A-Za-z]+")
    df["進站"] = df["進站"].str.replace(pattern, "", regex=True)
    df["出站"] = df["出站"].str.replace(pattern, "", regex=True)
    df.rename(columns={
        "日期": "date",
        "時段": "hour",
        "進站": "mrt_station_name_enter",
        "出站": "mrt_station_name_exit",
        "人次": "visitors_num"
    }, inplace=True)
    print(f"{url} has been download")
    return (df)


# def T_mrt_usage_history_one_month_recuce(df: pd.DataFrame):
#     df_enter = pd.DataFrame(df.groupby(["date", "hour", "mrt_station_name_enter"])[
#                             "visitors_num"].sum()).reset_index(drop=False)
#     df_out = pd.DataFrame(df.groupby(["date", "hour", "mrt_station_name_exit"])[
#         "visitors_num"].sum()).reset_index(drop=False)
#     df = df_enter.merge(df_out,
#                         left_on=["date", "hour", "mrt_station_name_enter"],
#                         right_on=["date", "hour", "mrt_station_name_exit"],
#                         how="outer", suffixes=["_enter", "_exit"])
#     df["mrt_station_name"] = df["mrt_station_name_exit"].combine_first(
#         df["mrt_station_name_enter"])
#     df = df.loc[:, ["date", "hour", "mrt_station_name",
#                     "visitors_num_enter", "visitors_num_exit"]]
#     return (df)


# def L_mrt_usage_history(df: pd.DataFrame):
#     username_sql = os.getenv("ANDY_USERNAME_SQL")
#     password_sql = os.getenv("ANDY_PASSWORD_SQL")
#     # server = "host.docker.internal:3306"  #docker用
#     server = "localhost:3306"
#     db_name = "group2_db"
#     try:
#         with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
#             df.to_sql(
#                 name="mrt_usage_history",
#                 con=conn,
#                 if_exists="append",
#                 index=False
#             )
#         print(f"L_mrt_usage_history finished")
#         return ("L_mrt_usage_history finished")
#     except:
#         print("loading to sql fail")


if __name__ == "__main__":
    STORAGE_CREDENTIALS_FILE_PATH = r"D:\TIR_101_group2_project_andy\Taipei-transit-data_hub\airflow\gcp_credentials\andy-gcs_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = STORAGE_CREDENTIALS_FILE_PATH
    STORAGE_CLIENT = storage.Client()
    api_url_df = get_usage_history_csvfilelist()
    gcs_filenames_set = get_gcs_filenames_set(client=STORAGE_CLIENT,bucket_name="mrt_history_usage_andy")
    df_need_update = api_url_df.loc[~api_url_df["年月"].isin(gcs_filenames_set),]

    for i in range(len(df_need_update)):
        month = df_need_update.loc[i, "年月"]
        print(f"we need to update {month}")
        url = df_need_update.loc[i, "URL"]
        df_download_form_api = mrt_usage_history_one_month(url=url)
        upload_df_to_gcs(client=STORAGE_CLIENT, 
                         bucket_name="mrt_history_usage_andy", 
                         blob_name=f"{month}_mrt_history_usage.csv", 
                         df=df_download_form_api)
