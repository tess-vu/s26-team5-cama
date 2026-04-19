-- Issue #27: Export GeoJSON File with Assessment Values for Residential Properties
-- Completed By: Joshua Rigsby
-- Language: SQL (obvi)
-- 
-- Description:
-- Script queries residential property data for export as a
-- GeoJSON file to be converted into vector tiles for the assessment dashboard.
-- 
-- Used:
-- core.pwd_parcels
-- core.opa_properties
-- core.opa_assessments
-- derived.current_assessments
-- 
-- Summary:
-- 1. Query the most recent official tax year assessment value per property
-- 2. Transform to get assessment history to get 2023 and 2024 assessed values
-- 3. Join parcel geometry (pwd_parcels) with property characteristics
--    (opa_properties), official assessments (opa_assessments), and
--    ML-predicted values (derived.current_assessments)
-- 4. Calculate percent change between predicted and last official value
-- 5. Filter to residential properties only
-- 6. Output geometry and selected fields for hover tooltip and
--    click-popup panel in dashboard



WITH latest_assessment AS (
    SELECT
        parcel_number,
        market_value AS tax_year_assessed_value,
        year AS tax_year
    FROM (
        SELECT
            parcel_number,
            market_value,
            year,
            ROW_NUMBER() OVER (PARTITION BY parcel_number ORDER BY year DESC) AS rn
        FROM `{project_id}.core.opa_assessments`
        WHERE
            market_value IS NOT NULL
            AND market_value > 0
    )
    WHERE rn = 1
),

assessments_pivot AS (
    SELECT
        parcel_number,
        MAX(CASE WHEN year = 2023.0 THEN market_value END) AS assessed_value_2023,
        MAX(CASE WHEN year = 2024.0 THEN market_value END) AS assessed_value_2024
    FROM `{project_id}.core.opa_assessments`
    WHERE
        market_value IS NOT NULL
        AND market_value > 0
    GROUP BY parcel_number
)

SELECT
    -- Geometry
    ST_AsGeoJSON(pwd.geometry) AS geometry,

    -- Id
    p.parcel_number AS property_id,

    -- Hover tooltip fields
    p.location AS address,
    ca.predicted_value AS current_assessed_value,
    la.tax_year_assessed_value,
    la.tax_year,
    ROUND(
        SAFE_DIVIDE(ca.predicted_value - la.tax_year_assessed_value, la.tax_year_assessed_value),
        4
    ) AS pct_change,
    p.zip_code,

    -- Click-popup panel fields
    ap.assessed_value_2023,
    ap.assessed_value_2024,
    SAFE_CAST(p.total_livable_area AS FLOAT64) AS total_livable_area,
    SAFE_CAST(p.number_of_bathrooms AS FLOAT64) AS number_of_bathrooms,
    SAFE_CAST(p.interior_condition AS FLOAT64) AS interior_condition,
    EXTRACT(YEAR FROM SAFE_CAST(p.sale_date AS TIMESTAMP)) AS sale_year

FROM `{project_id}.core.opa_properties` AS p
JOIN `{project_id}.core.pwd_parcels` AS pwd
    ON CAST(pwd.brt_id AS STRING) = p.parcel_number
LEFT JOIN latest_assessment AS la
    ON la.parcel_number = p.parcel_number
LEFT JOIN `{project_id}.derived.current_assessments` AS ca
    ON ca.property_id = p.parcel_number
LEFT JOIN assessments_pivot AS ap
    ON ap.parcel_number = p.parcel_number
WHERE
    p.category_code = '1'
