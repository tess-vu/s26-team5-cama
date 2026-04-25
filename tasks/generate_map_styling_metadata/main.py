"""
HTTP Cloud Function to export map styling metadata to Cloud Storage.

This function is meant to query derived.current_assessments_model_training_data
and export the relevant map styling metadata to the public GCS bucket.

Usage:
    Deploy as a Cloud Function named "generate-map-styling-metadata"
"""

import functions_framework
import json
from google.cloud import bigquery
from google.cloud import storage
import os

# year_built is commented out for now, in case we want to add it later.
SQL_QUERY = """
    SELECT
        MIN(sale_price) AS min,
        MAX(sale_price) AS max,
        APPROX_QUANTILES(sale_price, 4) AS breakpoints,
        MIN(year_built) AS year_built_min,
        MAX(year_built) AS year_built_max,
        APPROX_QUANTILES(year_built, 4) AS year_built_breakpoints
    FROM `derived.current_assessments_model_training_data`
"""


@functions_framework.http
def export_map_styling(request):
    try:
        public_bucket = os.getenv("PUBLIC_BUCKET", "musa5090s26-team5")

        # Initialize BQ client and execute
        bq_client = bigquery.Client()
        job = bq_client.query(SQL_QUERY)
        results = job.result()
        print("Executed SQL query to get map styling metadata.")

        # Process results into a dictionary.
        styling_metadata = {}
        for row in results:
            styling_metadata = {
                "sale_price": {
                    "min": row.min,
                    "max": row.max,
                    "breakpoints": list(row.breakpoints)
                },
                "year_built": {
                    "min": row.year_built_min,
                    "max": row.year_built_max,
                    "breakpoints": list(row.year_built_breakpoints)
                }
            }
            print(f"Processed row: { styling_metadata}")

        # Upload to public GCS bucket.
        storage_client = storage.Client()
        bucket = storage_client.bucket(public_bucket)
        blob = bucket.blob("configs/map_styling_metadata.json")
        blob.upload_from_string(
            json.dumps(styling_metadata),
            content_type="application/json"
        )

        blob.make_public()
        print(f"Uploaded to gs://{public_bucket}/configs/map_styling_metadata.json")

        return ("Successfully exported map styling metadata.", 200)

    except Exception as e:
        print(f"Error exporting map styling metadata: {e}.")
        return (f"Error: {e}.", 500)
