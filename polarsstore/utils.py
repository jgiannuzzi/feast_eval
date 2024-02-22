import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

from datetime import timedelta
from feast import Entity, FeatureView, FileSource, Field, ValueType
import s3fs
from feast.repo_config import RepoConfig
from feast.types import Int64
import os
import time

s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
s3_endpoint = os.environ.get("FEAST_S3_ENDPOINT_URL")


def generate_feast_repository_definitions(num_columns, parquet_file_path):
    """
    Generates Feast repository definitions for accessing the Parquet file.

    :param num_columns: The number of columns in the Parquet file.
    :param parquet_file_path: The S3 path to the Parquet file.

    :return: A list containing the Feast entity, source, and FeatureView definitions.
    """
    # Define the entity. Assume 'id' is the join key in your Parquet files
    dummy_entity = Entity(name="id", value_type=ValueType.INT64, description="Dummy ID")

    # Define the source pointing to the Parquet file in S3
    dummy_source = FileSource(
        path=parquet_file_path,
        event_timestamp_column="event_timestamp",  # Adjust if your Parquet file has a different timestamp column
        s3_endpoint_override=s3_endpoint,
    )

    # Define the complete schema (including both entities and features)
    schema = [
        Field(name="id", dtype=Int64),  # Entity column
        # Add feature columns
        *[Field(name=f"feature_{i}", dtype=Int64) for i in range(1, num_columns + 1)],
    ]

    # Define the feature view
    dummy_fv = FeatureView(
        name="dummy_feature_view",
        entities=[dummy_entity],
        ttl=timedelta(days=1),
        source=dummy_source,
        schema=schema,
        online=False,
    )

    return [dummy_entity, dummy_source, dummy_fv]


def create_parquet_file(
    num_columns: int,
    num_rows: int,
    bucket_name: str,
    s3_filepath: str,
):
    """
    Creates a Parquet file with specified dimensions and uploads it to S3.

    :param num_columns: Number of columns in the DataFrame.
    :param num_rows: Number of rows in the DataFrame.
    :param bucket_name: S3 bucket name.
    :param s3_filepath: Path to save the Parquet file in S3.
    """

    # Create a DataFrame with random data
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(num_rows, num_columns)),
        columns=[f"feature_{i}" for i in range(1, num_columns + 1)],
    )
    df["id"] = np.arange(1, num_rows + 1)  # Adding a user_id column for entity
    df["event_timestamp"] = [
        datetime.now() - timedelta(minutes=i) for i in range(num_rows)
    ]

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    begin = time.perf_counter_ns()

    fs = s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": s3_endpoint,
            "aws_access_key_id": s3_access_key,
            "aws_secret_access_key": s3_secret_key,
        }
    )

    # Write Table to a Parquet file in S3
    with fs.open(f"{bucket_name}/{s3_filepath}", "wb") as f:
        pq.write_table(table, f)

    end = time.perf_counter_ns()

    return (end - begin) / 1_000_000
