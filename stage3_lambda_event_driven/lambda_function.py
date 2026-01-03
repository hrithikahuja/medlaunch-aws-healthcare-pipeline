import os
import json
import boto3
from botocore.exceptions import ClientError

athena = boto3.client("athena")

ATHENA_DB = os.environ["ATHENA_DB"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]  # s3://bucket/prefix/

QUERY = """
SELECT
  location.state AS state,
  COUNT(DISTINCT facility_id) AS accredited_facility_count
FROM healthcare.facilities_raw
WHERE accreditations IS NOT NULL AND cardinality(accreditations) > 0
GROUP BY 1
ORDER BY accredited_facility_count DESC;
""".strip()


def parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    no_scheme = uri.replace("s3://", "", 1)
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    # 1) Start Athena query (do not wait here; Step Functions will poll)
    try:
        qid = athena.start_query_execution(
            QueryString=QUERY,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )["QueryExecutionId"]
    except ClientError as e:
        print("start_query_execution failed:", str(e))
        raise

    # 2) Compute the expected Athena result object key: <prefix>/<QueryExecutionId>.csv
    out_bucket, out_prefix = parse_s3_uri(ATHENA_OUTPUT)
    out_prefix = out_prefix.rstrip("/") + "/" if out_prefix else ""
    result_key = f"{out_prefix}{qid}.csv"

    print("Started Athena query_execution_id:", qid)
    print("Expected Athena output:", f"s3://{out_bucket}/{result_key}")

    # 3) Return info Step Functions needs
    return {
        "status": "submitted",
        "query_execution_id": qid,
        "output_bucket": out_bucket,
        "output_key": result_key
    }
