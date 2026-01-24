import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task

from ETL.extract import extract_data
from ETL.transform import transform_data
from ETL.load import load_data
from ETL.write_to_s3 import write_parquet_partitioned


@task
def extract_task():
    return extract_data()

@task
def transform_task(df):
    return transform_data(df)

@task
def load_task(df):
    load_data(df)


@flow
def etl_flow():
    df = extract_task()
    clean_df = transform_task(df)

    write_parquet_partitioned(clean_df)
    load_task(clean_df)


if __name__ == "__main__":
    etl_flow()
