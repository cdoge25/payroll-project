# Import the SparkSession module
import json
import os

from pyspark.sql import SparkSession

MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

PROCESSED_FILE_PATH_LIST_STR = os.getenv("PROCESSED_FILE_PATH_LIST_STR")


def _load_to_postgres():
    # Create a SparkSession
    spark = (
        SparkSession.builder.appName("LoadToPostgres")
        .config(
            "spark.jars",
            "spark/jars/postgresql-9.4.1207.jar,spark/jars/aws-java-sdk-bundle-1.11.1026.jar,spark/jars/hadoop-aws-3.3.2.jar",
        )
        .config("fs.s3a.access.key", MINIO_ACCESS_KEY_ID)
        .config("fs.s3a.secret.key", MINIO_SECRET_ACCESS_KEY)
        .config(
            "fs.s3a.endpoint",
            MINIO_ENDPOINT,
        )
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.attempts.maximum", "1")
        .config("fs.s3a.connection.establish.timeout", "50")
        .config("fs.s3a.connection.timeout", "100")
        .getOrCreate()
    )
    processed_file_path_list = json.loads(
        PROCESSED_FILE_PATH_LIST_STR.replace("'", '"')
    )
    for file_path in processed_file_path_list:
        print("=" * 100)
        print(f"Reading: {file_path}")
        df = spark.read.parquet(f"s3a://{file_path}")
        url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
        properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
        }
        table_name = f"LND_{file_path.split('/')[-1].split('.')[0]}"
        table = f"{POSTGRES_SCHEMA}.{table_name}"
        df.write.jdbc(url=url, table=table, mode="overwrite", properties=properties)
        print("=" * 100)
    os.system("kill %d" % os.getpid())


def _load_to_snowflake():
    spark = (
        SparkSession.builder.appName("LoadToSnowflake")
        .config(
            "spark.jars",
            "spark/jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar,spark/jars/snowflake-jdbc-3.13.30.jar,spark/jars/aws-java-sdk-bundle-1.11.1026.jar,spark/jars/hadoop-aws-3.3.2.jar",
        )
        .config("fs.s3a.access.key", MINIO_ACCESS_KEY_ID)
        .config("fs.s3a.secret.key", MINIO_SECRET_ACCESS_KEY)
        .config(
            "fs.s3a.endpoint",
            MINIO_ENDPOINT,
        )
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.attempts.maximum", "1")
        .config("fs.s3a.connection.establish.timeout", "50")
        .config("fs.s3a.connection.timeout", "100")
        .getOrCreate()
    )
    processed_file_path_list = json.loads(
        PROCESSED_FILE_PATH_LIST_STR.replace("'", '"')
    )
    for file_path in processed_file_path_list:
        print(f"Reading: {file_path}")
        df = spark.read.parquet(f"s3a://{file_path}")
        sf_options = {
            "sfURL": f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
            "sfWarehouse": SNOWFLAKE_WAREHOUSE,
            "sfDatabase": SNOWFLAKE_DATABASE,
            "sfSchema": SNOWFLAKE_SCHEMA,
            "sfUser": SNOWFLAKE_USER,
            "sfPassword": SNOWFLAKE_PASSWORD,
            "sfRole": SNOWFLAKE_ROLE,
        }
        table_name = f"LND_{file_path.split('/')[-1].split('.')[0]}"
        table = f"{SNOWFLAKE_SCHEMA}.{table_name}"
        df.write.format("snowflake").options(**sf_options).option(
            "dbtable", table
        ).mode("overwrite").save()
        print("=" * 100)
    os.system("kill %d" % os.getpid())


_load_to_snowflake()
