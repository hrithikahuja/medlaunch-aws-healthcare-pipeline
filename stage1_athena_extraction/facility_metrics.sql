-- Stage 1: Extract facility metrics from nested JSON and write to S3 as Parquet

CREATE TABLE healthcare.facility_metrics
WITH (
  format = 'PARQUET',
  external_location = 's3://healthcare-pipeline-2026/processed/facility_metrics/',
  parquet_compression = 'SNAPPY'
) AS
SELECT
  facility_id,
  facility_name,
  employee_count,
  cardinality(services) AS number_of_offered_services,
  (
    SELECT a.valid_until
    FROM UNNEST(accreditations) AS t(a)
    ORDER BY a.valid_until
    LIMIT 1
  ) AS expiry_date_of_first_accreditation
FROM healthcare.facilities_raw;

