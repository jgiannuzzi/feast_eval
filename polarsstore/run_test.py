from feast import FeatureStore
from utils import (
    generate_feast_repository_definitions,
    create_parquet_file,
)
import time
from datetime import datetime, timedelta
import csv
import sys
import polars as pl


def write_results_to_csv_file(results, outfilename):
    """Writes the results of tests to a csv file.

    :param results: The results of testing as a list of lists.
    :param outfilename: The name of the file to write results.
    """
    with open(outfilename, "w", newline="\n") as csvfile:
        testwriter = csv.writer(csvfile, delimiter=",")
        for row in results:
            testwriter.writerow(row)


def run_tests():
    # Test configurations
    test_cases = [
        (10, 10),
        (10, 100),
        (10, 1000),
        (10, 10000),
        (100, 10),
        (100, 100),
        (100, 1000),
        (100, 10000),
        (1000, 10),
        (1000, 100),
        (1000, 1000),
        (1000, 10000),
        (10000, 10),
        (10000, 100),
        (10000, 1000),
        (10000, 10000),
    ]

    print("Running tests....")
    results = []
    results.append(
        ["Number of columns", "Number of rows", "get_historical_feature read in ms", "polarsstore read in ms", "overhead"]
    )

    for num_columns, num_rows in test_cases:
        # Initialize Feature Store
        fs = FeatureStore(".")

        # Define the path for the Parquet file in S3
        bucket_name = "my-bucket"
        s3_filepath = f"test_data_{num_columns}_{num_rows}.parquet"

        # Create and upload the Parquet file to S3
        write_time = create_parquet_file(
            num_columns, num_rows, bucket_name, s3_filepath
        )

        # Generate Feast repository definitions
        parquet_file_path = f"s3://{bucket_name}/{s3_filepath}"
        defs = generate_feast_repository_definitions(num_columns, parquet_file_path)

        # Apply the Feast definitions (feature views) to the FeatureStore,  usually  you only need to apply once
        fs.apply(defs)  # Uncomment this if you need to apply the definitions

        # Create an entity DataFrame for historical feature retrieval
        entity_df = pl.DataFrame(
            {
                "id": list(range(1, num_rows + 1)),
                "event_timestamp": [
                    datetime.now() - timedelta(days=i) for i in range(num_rows)
                ],
            }
        )

        # Define the feature references to be retrieved
        feature_refs = [
            f"dummy_feature_view:feature_{i}" for i in range(1, num_columns + 1)
        ]

        # Time the historical feature retrieval
        sys.stderr.write(f"Test with {num_columns} columns and {num_rows} rows")
        sys.stderr.flush()
        elapsed_times = []
        overhead_times = []
        for i in range(20):
            begin = time.perf_counter_ns()
            feature_df = fs.get_historical_features(
                entity_df=entity_df, features=feature_refs
            )
            feature_df.to_df()  # Use .to_df() to materialize the result into a DataFrame
            end = time.perf_counter_ns()
            elapsed_times.append((end - begin) / 1e6)
            overhead_times.append((end - begin) / 1e6 - feature_df.elapsed_time_ms)
            sys.stderr.write('.')
            sys.stderr.flush()

        elapsed_time_ms = int(sum(elapsed_times) / len(elapsed_times))
        overhead_time_ms = int(sum(overhead_times) / len(overhead_times))
        print(
            f"\rTest with {num_columns} columns and {num_rows} rows took {elapsed_time_ms}ms (overhead: {overhead_time_ms}ms)"
        )

        results.append([num_columns, num_rows, elapsed_time_ms, feature_df.elapsed_time_ms, overhead_time_ms])

    # write results to csv file
    write_results_to_csv_file(results, "results.csv")


if __name__ == "__main__":
    run_tests()
