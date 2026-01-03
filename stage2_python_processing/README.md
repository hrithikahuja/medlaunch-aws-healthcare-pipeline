# Stage 2: Data Processing with Python (boto3)

## Objective
Read facility JSON records from S3, filter facilities with any accreditation expiring within 6 months, and write filtered records to a different S3 prefix.

## Inputs
- Bucket: healthcare-pipeline-2026
- Prefix: raw/clean_dataset/
- Format: NDJSON (one JSON object per line)

## Output
- Bucket: healthcare-pipeline-2026
- Prefix: results/expiring_facilities/
- Format: NDJSON

## How to run (CloudShell)
1. Upload this script to CloudShell
2. Run:
   python3 filter_expiring_facilities.py

Optional env vars:
- INPUT_BUCKET, INPUT_PREFIX
- OUTPUT_BUCKET, OUTPUT_PREFIX
- MONTHS_WINDOW (default 6)

## Error handling & logging
- Logs input files processed
- Warns on bad date formats
- Raises on AWS errors

## Evidence
See `/evidence/screenshots/` for:
- CloudShell run output
- Result file created in S3

