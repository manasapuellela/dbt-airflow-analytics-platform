import os
import pathlib
import urllib.request
from typing import Iterable

import boto3
import pandas as pd

DATASET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)
DEFAULT_WAREHOUSE_DB_URI = "postgresql://warehouse-db:5432/warehouse"
DEFAULT_S3_BUCKET = "analytics-raw"
DEFAULT_S3_PREFIX = "nyc_taxi/yellow"
DEFAULT_OUTPUT_DIR = pathlib.Path("data/nyc_taxi/yellow")
SOURCE_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID",
]
RENAME_COLUMNS = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
}
SELECT_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID",
]


def get_warehouse_db_uri() -> str:
    """Return the warehouse database URI for containerized runs."""
    return os.getenv("WAREHOUSE_DB_URI", DEFAULT_WAREHOUSE_DB_URI)


def add_pickup_date(
    dataframe: pd.DataFrame,
    column: str = "pickup_datetime",
    fallback_column: str = "tpep_pickup_datetime",
) -> pd.DataFrame:
    """Add a pickup_date column derived from the provided datetime column."""
    dataframe = dataframe.copy()
    if column not in dataframe.columns:
        if column == "pickup_datetime" and fallback_column in dataframe.columns:
            column = fallback_column
        else:
            raise KeyError(f"Missing expected pickup datetime column: {column}")
    dataframe[column] = pd.to_datetime(dataframe[column])
    dataframe["pickup_date"] = dataframe[column].dt.date
    return dataframe


def list_parquet_files(output_dir: pathlib.Path) -> list[pathlib.Path]:
    return list(output_dir.rglob("*.parquet"))


def upload_files_to_s3(
    files: Iterable[pathlib.Path],
    bucket: str,
    prefix: str,
    s3_client,
    base_dir: pathlib.Path,
) -> None:
    for path in files:
        relative_key = path.relative_to(base_dir)
        s3_key = f"{prefix}/{relative_key.as_posix()}"
        s3_client.upload_file(str(path), bucket, s3_key)


def download_dataset(url: str, destination: pathlib.Path) -> pathlib.Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading dataset from {url}...")
    urllib.request.urlretrieve(url, destination)
    return destination


def load_and_transform_data(parquet_path: pathlib.Path) -> pd.DataFrame:
    dataframe = pd.read_parquet(parquet_path, columns=SOURCE_COLUMNS)
    dataframe = dataframe.rename(columns=RENAME_COLUMNS)
    dataframe = dataframe[SELECT_COLUMNS]
    dataframe = add_pickup_date(dataframe, column="pickup_datetime")
    return dataframe


def write_partitioned_parquet(dataframe: pd.DataFrame, output_dir: pathlib.Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_count = 0
    for pickup_date, partition_df in dataframe.groupby("pickup_date"):
        partition_dir = output_dir / f"pickup_date={pickup_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / "data.parquet"
        partition_df.to_parquet(output_path, index=False)
        file_count += 1
    return file_count


def build_s3_client():
    endpoint_url = os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT_URL")
    return boto3.client("s3", endpoint_url=endpoint_url)


def main() -> None:
    bucket = os.getenv("S3_BUCKET", DEFAULT_S3_BUCKET)
    prefix = os.getenv("S3_PREFIX", DEFAULT_S3_PREFIX)
    output_dir = DEFAULT_OUTPUT_DIR
    download_dir = output_dir.parent / "source"
    parquet_path = download_dir / "yellow_tripdata_2023-01.parquet"
    downloaded_path = download_dataset(DATASET_URL, parquet_path)
    dataframe = load_and_transform_data(downloaded_path)
    print(f"Loaded {len(dataframe):,} rows.")

    file_count = write_partitioned_parquet(dataframe, output_dir)
    print(f"Wrote {file_count} parquet file(s) to {output_dir}.")

    s3_client = build_s3_client()
    files = list_parquet_files(output_dir)
    upload_files_to_s3(files, bucket, prefix, s3_client, output_dir)
    print(f"Uploaded {len(files)} parquet file(s) to s3://{bucket}/{prefix}.")


if __name__ == "__main__":
    main()
