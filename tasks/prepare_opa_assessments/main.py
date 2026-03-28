"""
Cloud Function to prepare OPA Assessments data for BigQuery.

This function reads the raw OPA Assessments CSV from Cloud Storage,
processes the data in chunks, and writes to Parquet format.

Usage:
    Deploy as a Cloud Function named "prepare-opa-assessments"
"""

import functions_framework
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

CHUNK_SIZE = 200_000
NUMERIC_COLUMNS = [
    "market_value",
    "taxable_building",
    "taxable_land",
    "exempt_building",
    "exempt_land",
    "year",
]


def process_chunk(chunk_df, parquet_writer, output_file):
    """Process a DataFrame chunk and append to Parquet.

    Returns the ParquetWriter (created on first call).
    """
    # Lowercase column names.
    chunk_df.columns = [c.lower() for c in chunk_df.columns]

    # Convert numeric columns.
    for col in NUMERIC_COLUMNS:
        if col in chunk_df.columns:
            chunk_df[col] = pd.to_numeric(chunk_df[col], errors="coerce").astype(
                "float64"
            )

    table = pa.Table.from_pandas(chunk_df, preserve_index=False)

    if parquet_writer is None:
        parquet_writer = pq.ParquetWriter(output_file, table.schema)
    else:
        table = table.cast(parquet_writer.schema)

    parquet_writer.write_table(table)
    return parquet_writer


@functions_framework.http
def prepare_opa_assessments(request):
    """HTTP Cloud Function to prepare OPA Assessments data.

    Reads the raw CSV from Cloud Storage, converts to Parquet in
    chunks, and uploads to the prepared data bucket.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")
        prepared_data_bucket = os.getenv(
            "PREPARED_DATA_BUCKET", "musa5090s26-team5-prepared_data"
        )

        storage_client = storage.Client()
        raw_blob = storage_client.bucket(raw_data_bucket).blob(
            "opa_assessments/data.csv"
        )

        # Download CSV to a temp file.
        temp_csv = "/tmp/opa_assessments.csv"
        temp_parquet = "/tmp/opa_assessments.parquet"

        print("Downloading raw OPA Assessments CSV.")
        raw_blob.download_to_filename(temp_csv)

        # Read CSV in chunks and write to Parquet.
        print("Processing CSV into Parquet.")
        parquet_writer = None
        total_rows = 0

        for chunk_df in pd.read_csv(temp_csv, chunksize=CHUNK_SIZE):
            parquet_writer = process_chunk(chunk_df, parquet_writer, temp_parquet)
            total_rows += len(chunk_df)
            print(f"Processed {total_rows} rows.")

        if parquet_writer:
            parquet_writer.close()

        print(f"Processed {total_rows} rows total.")

        # Upload to prepared data bucket.
        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "opa_assessments/data.parquet"
        )
        print("Uploading to Cloud Storage.")
        prepared_blob.upload_from_filename(temp_parquet)

        print(f"Uploaded to gs://{prepared_data_bucket}/opa_assessments/data.parquet")

        return (
            f"Successfully prepared {total_rows} OPA assessment records as Parquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
