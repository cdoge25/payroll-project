import os
from datetime import datetime

from scripts.extractors import _get_google_drive_file_list

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
        file_list = _get_google_drive_file_list(
            folder_id=GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_API_KEY
        )
        return file_list

    file_list = get_google_drive_file_list()


payroll_dag()
