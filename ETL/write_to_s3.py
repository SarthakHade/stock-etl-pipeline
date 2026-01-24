import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


def write_parquet_partitioned(df):
    print(">>> Writing Parquet to S3 (partitioned) <<<")

    bucket = "de-stock-data-sarthak"   # 🔴 replace with your bucket name
    base_prefix = "processed/stocks"

    s3 = boto3.client("s3")

    # Ensure Date is datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Add partition columns
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'].dt.month

    for (year, month), group_df in df.groupby(['year', 'month']):
        partition_key = (
            f"{base_prefix}/year={year}/month={month}/data.parquet"
        )

        table = pa.Table.from_pandas(
            group_df.drop(columns=['year', 'month']),
            preserve_index=False
        )

        buffer = BytesIO()
        pq.write_table(table, buffer)

        s3.put_object(
            Bucket=bucket,
            Key=partition_key,
            Body=buffer.getvalue()
        )

        print(f"Written → s3://{bucket}/{partition_key}")

    print(">>> Parquet write complete <<<")
