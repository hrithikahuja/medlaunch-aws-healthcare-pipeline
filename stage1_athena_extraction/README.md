# Stage 1: Data Extraction with Athena

## Objective
Extract facility metrics from nested JSON in S3 using Athena SQL and save results back to S3.

## Inputs
- Source S3 prefix: s3://healthcare-pipeline-2026/raw/clean_dataset/
- Athena table: healthcare.facilities_raw

## Output
- Parquet dataset written to:
  s3://healthcare-pipeline-2026/processed/facility_metrics/

## Metrics Extracted
- facility_id
- facility_name
- employee_count
- number_of_offered_services (cardinality of services array)
- expiry_date_of_first_accreditation (earliest valid_until among accreditations)

## How to run
1. Open Athena (us-east-2)
2. Ensure database `healthcare` and external table `facilities_raw` exist
3. Run `facility_metrics_ctas.sql`
4. Verify output Parquet files exist in the processed prefix

## Evidence
See `/evidence/screenshots/` for:
- Athena query succeeded
- Output location in S3
- Preview of resulting table

