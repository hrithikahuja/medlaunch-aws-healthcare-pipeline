# Stage 3: Event-Driven Processing with Lambda

## Objective
Automatically process new S3 data uploads by triggering a Lambda function that executes an Athena query to count accredited facilities per state and stores the query results in S3.

## What I built
- An AWS Lambda function (`stage3_s3_athena_state_counts`) that:
  - Starts an Athena query to count accredited facilities per state
  - Returns the Athena `query_execution_id`
  - Returns the expected Athena output location (`output_bucket`, `output_key`) so downstream orchestration can track and copy results
- This Lambda is invoked by Stage 4 Step Functions (and can also be connected to an S3 upload trigger if required)

## Athena Query Used
The query counts distinct accredited facilities per state (accreditations present):

```sql
SELECT
  location.state AS state,
  COUNT(DISTINCT facility_id) AS accredited_facility_count
FROM healthcare.facilities_raw
WHERE accreditations IS NOT NULL AND cardinality(accreditations) > 0
GROUP BY 1
ORDER BY accredited_facility_count DESC;

