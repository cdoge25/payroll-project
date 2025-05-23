import os
from datetime import datetime

from scripts.extractors import (
    _get_google_drive_file_content,
    _get_google_drive_file_list,
    _ingest_file_to_s3,
)
from scripts.processors import S3CsvFileProcessor

from airflow.decorators import dag, task

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

DBT_USER = os.getenv("DBT_USER")
DBT_ROLE = os.getenv("DBT_ROLE")
DBT_PASSWORD = os.getenv("DBT_PASSWORD")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["payroll"],
)
def payroll_dag():
    @task
    def get_google_drive_file_list():
        gd_file_list = _get_google_drive_file_list(
            folder_id=GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_API_KEY
        )
        return gd_file_list

    @task
    def ingest_file(gd_file):
        gd_file_content = _get_google_drive_file_content(
            file_id=gd_file["id"], api_key=GOOGLE_API_KEY
        )
        s3_raw_file_url = _ingest_file_to_s3(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            object_name=gd_file["name"],
            data=gd_file_content,
        )
        return s3_raw_file_url

    @task
    def process_file(s3_raw_file_url):
        processor = S3CsvFileProcessor(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3_processed_file_url = processor._process_file(s3_raw_file_url)
        return s3_processed_file_url

    gd_file_list = get_google_drive_file_list()
    ingested_file_list = ingest_file.expand(gd_file=gd_file_list)
    processed_file_list = process_file.expand(s3_raw_file_url=ingested_file_list)


payroll_dag()
