#!/usr/bin/env python3
"""Download a sample NYC Taxi dataset, write partitioned parquet to S3/MinIO,
then load raw tables into the local warehouse for dbt transformations."""
from __future__ import annotations

import os
import pathlib
import tempfile
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

DATASET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

RAW_SCHEMA = "raw"
RAW_TABLE = "yellow_taxi_trips"
ZONE_TABLE = "taxi_zone_lookup"

DEFAULT_ROW_GROUP = 0


def download_file(url: str, destination: pathlib.Path) -> pathlib.Path:
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    with destination.open("wb") as file_handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file_handle.write(chunk)
    return destination


def load_parquet_sample(path: pathlib.Path, row_group: int = DEFAULT_ROW_GROUP) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(path)
    table = parquet_file.read_row_group(row_group)
    return table.to_pandas()


def add_pickup_date(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["pickup_date"] = pd.to_datetime(frame["tpep_pickup_datetime"]).dt.date
    return frame


def write_partitioned_parquet(frame: pd.DataFrame, output_dir: pathlib.Path) -> list[pathlib.Path]:
    table = pa.Table.from_pandas(frame)
    pq.write_to_dataset(table, root_path=str(output_dir), partition_cols=["pickup_date"])
    return list(output_dir.rglob("*.parquet"))


def upload_files_to_s3(files: list[pathlib.Path], bucket: str, prefix: str, s3_client) -> None:
    for path in files:
        relative_key = path.relative_to(path.parents[1])
        s3_key = f"{prefix}/{relative_key.as_posix()}"
        s3_client.upload_file(str(path), bucket, s3_key)


def ensure_raw_schema(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))


def load_dataframe(engine, frame: pd.DataFrame, table_name: str) -> None:
    frame.to_sql(table_name, engine, schema=RAW_SCHEMA, if_exists="replace", index=False)


def main() -> None:
    load_dotenv()

    s3_bucket = os.environ.get("S3_BUCKET", "analytics-demo")
    s3_prefix = os.environ.get("S3_PREFIX", "raw/yellow_taxi")
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    warehouse_uri = os.environ.get(
        "WAREHOUSE_DB_URI",
        "postgresql+psycopg2://analytics:analytics@localhost:5434/analytics",
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)
        parquet_path = temp_path / "yellow_tripdata.parquet"
        zone_path = temp_path / "taxi_zone_lookup.csv"

        print("Downloading trip data...")
        download_file(DATASET_URL, parquet_path)
        print("Downloading taxi zone lookup...")
        download_file(ZONE_LOOKUP_URL, zone_path)

        print("Loading sample row group...")
        trips = load_parquet_sample(parquet_path)
        trips = add_pickup_date(trips)

        output_dir = temp_path / "partitioned"
        output_dir.mkdir(parents=True, exist_ok=True)
        files = write_partitioned_parquet(trips, output_dir)

        print("Uploading parquet files to S3...")
        upload_files_to_s3(files, s3_bucket, s3_prefix, s3_client)

        print("Loading raw tables into warehouse...")
        engine = create_engine(warehouse_uri)
        ensure_raw_schema(engine)
        load_dataframe(engine, trips, RAW_TABLE)
        zones = pd.read_csv(zone_path)
        load_dataframe(engine, zones, ZONE_TABLE)

        print(
            f"Ingestion complete at {datetime.utcnow().isoformat()}Z with {len(trips)} rows."
        )


if __name__ == "__main__":
    main()
