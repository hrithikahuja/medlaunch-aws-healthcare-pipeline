import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

INPUT_BUCKET = os.environ.get("INPUT_BUCKET", "healthcare-pipeline-2026")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "raw/clean_dataset/")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "healthcare-pipeline-2026")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "results/expiring_facilities/")

MONTHS_WINDOW = int(os.environ.get("MONTHS_WINDOW", "6"))


def parse_date(date_str: str):
    # expected format: YYYY-MM-DD
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def is_expiring_within_window(accreditations, cutoff_dt):
    if not accreditations:
        return False

    for a in accreditations:
        valid_until = a.get("valid_until")
        if not valid_until:
            continue
        try:
            dt = parse_date(valid_until)
            if dt <= cutoff_dt:
                return True
        except ValueError:
            logger.warning("Bad date format: %s", valid_until)
            continue

    return False


def list_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith((".json", ".ndjson")):
                continue
            yield key


def read_ndjson(bucket, key):
    logger.info("Reading s3://%s/%s", bucket, key)
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")

    records = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def write_ndjson(bucket, key, records):
    payload = "\n".join(json.dumps(r) for r in records) + "\n"
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))
    logger.info("Wrote %d records to s3://%s/%s", len(records), bucket, key)


def main():
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=MONTHS_WINDOW * 30)

    expiring = []
    processed_files = 0

    try:
        for key in list_objects(INPUT_BUCKET, INPUT_PREFIX):
            processed_files += 1
            records = read_ndjson(INPUT_BUCKET, key)
            for r in records:
                if is_expiring_within_window(r.get("accreditations"), cutoff):
                    expiring.append(r)

        out_key = f"{OUTPUT_PREFIX}expiring_facilities_{now.strftime('%Y%m%dT%H%M%SZ')}.ndjson"
        write_ndjson(OUTPUT_BUCKET, out_key, expiring)

        logger.info("Done. Input files=%d, expiring facilities=%d", processed_files, len(expiring))

    except ClientError as e:
        logger.exception("AWS error: %s", str(e))
        raise
    except Exception as e:
        logger.exception("Unexpected error: %s", str(e))
        raise


if __name__ == "__main__":
    main()
