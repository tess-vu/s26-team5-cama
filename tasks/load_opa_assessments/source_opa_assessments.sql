-- Create or replace the external table for OPA Assessments.
-- This table is backed by the Parquet file in Cloud Storage.
-- Schema is auto-detected from Parquet metadata.

CREATE OR REPLACE EXTERNAL TABLE `{project_id}.source.opa_assessments`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/opa_assessments/data.parquet']
);
