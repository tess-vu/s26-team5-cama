"""
Cloud Function to prepare Philadelphia Neighborhoods data.

This function reads the raw Philadelphia Neighborhoods Parquet from Cloud
Storage and writes it to the prepared data bucket. Since it's already in
Parquet format, this is essentially a copy operation.

Usage:
    Deploy as a Cloud Function named "prepare-neighborhoods"
"""

import functions_framework
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


@functions_framework.http
def prepare_neighborhoods(request):
    """HTTP Cloud Function to prepare Philadelphia Neighborhoods data.

    Reads the raw neighborhoods Parquet and copies to the prepared
    data bucket.

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

        # Read neighborhoods Parquet from raw data bucket.
        print("Reading neighborhoods Parquet from raw data bucket.")
        raw_blob = storage_client.bucket(raw_data_bucket).blob(
            "neighborhoods/data.parquet"
        )
        parquet_data = raw_blob.download_as_bytes()

        # Write to prepared data bucket.
        print("Writing neighborhoods Parquet to prepared data bucket.")
        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "neighborhoods/data.parquet"
        )
        prepared_blob.upload_from_string(
            parquet_data, content_type="parquet"
        )

        print(
            f"Prepared neighborhoods Parquet: gs://{prepared_data_bucket}/neighborhoods/data.parquet"
        )

        return (
            "Successfully prepared Philadelphia Neighborhoods Parquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
