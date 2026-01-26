import os
import pathlib
from typing import Iterable

import pandas as pd

DEFAULT_WAREHOUSE_DB_URI = "postgresql://warehouse-db:5432/warehouse"


def get_warehouse_db_uri() -> str:
    """Return the warehouse database URI for containerized runs."""
    return os.getenv("WAREHOUSE_DB_URI", DEFAULT_WAREHOUSE_DB_URI)


def add_pickup_date(dataframe: pd.DataFrame, column: str = "pickup_datetime") -> pd.DataFrame:
    """Add a pickup_date column derived from the provided datetime column."""
    dataframe = dataframe.copy()
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
