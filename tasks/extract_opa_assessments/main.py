"""
Cloud Function to extract OPA Assessments data.

This function downloads the OPA Assessment History dataset as a CSV
from the City of Philadelphia and uploads to Cloud Storage.

Usage:
    Deploy as a Cloud Function named "extract-opa-assessments"
"""

import functions_framework
import requests
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Direct CSV download link for OPA Assessments.
OPA_ASSESSMENTS_URL = "https://opendata-downloads.s3.amazonaws.com/assessments.csv"


@functions_framework.http
def extract_opa_assessments(request):
    """HTTP Cloud Function to extract OPA Assessments data.

    Downloads the OPA Assessment History CSV file and uploads
    to Cloud Storage.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")

        # Download the assessments CSV file.
        print("Downloading OPA Assessments CSV.")
        response = requests.get(OPA_ASSESSMENTS_URL, timeout=1800)
        response.raise_for_status()

        # Upload to Cloud Storage.
        storage_client = storage.Client()
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob("opa_assessments/data.csv")

        print("Uploading CSV to Cloud Storage.")
        blob.upload_from_string(response.content, content_type="text/csv")

        print(f"Uploaded to gs://{raw_data_bucket}/opa_assessments/data.csv")

        return (
            "Successfully extracted OPA Assessments as CSV.",
            200,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}.")
        return (f"Error fetching data: {e}.", 500)
    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
