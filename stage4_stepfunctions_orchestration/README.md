# Stage 4: Workflow Orchestration with Step Functions

## Objective
Create a Step Functions state machine for the complete pipeline:
- Trigger on new S3 data arrival
- Invoke Lambda to run Athena query
- Wait for query completion
- On success: copy results to production location
- On failure: send SNS notification
- Include retries and error handling

## What I built
- A Step Functions state machine (`healthcare-pipeline-orchestrator`) that:
  1. Invokes the Stage 3 Lambda to start an Athena query
  2. Waits and polls Athena `getQueryExecution` until completion
  3. If SUCCEEDED: copies the Athena output CSV into a stable production prefix
  4. If FAILED/CANCELLED (or any error): publishes an SNS notification

- An SNS topic for alerting:
  - `healthcare-pipelines-failures`

## State Machine Flow (high level)
1. `RunAthenaQueryLambda` (Lambda invoke with retries)
2. `WaitBeforePoll` (Wait 10 seconds)
3. `GetAthenaQueryExecution` (Athena getQueryExecution)
4. `CheckAthenaStatus` (Choice: SUCCEEDED / FAILED / CANCELLED / RUNNING)
5. `CopyResultsToProduction` (S3 copyObject)
6. `NotifyFailureSNS` (SNS publish)
7. `Success` (Succeed)

## Production Output
On success, the CSV is copied to:

- `s3://healthcare-pipeline-2026/production/state_counts/<execution-name>.csv`

## SNS Alerts
On any failure, the workflow publishes the full execution context to SNS:

- Topic ARN: `arn:aws:sns:us-east-2:280646578422:healthcare-pipelines-failures`

## IAM Permissions (high level)
Step Functions execution role requires:
- `lambda:InvokeFunction` for Stage 3 Lambda
- `athena:GetQueryExecution`
- `s3:GetObject` and `s3:PutObject` for copying within the bucket
- `sns:Publish` for alerting

Note: S3 "copy" is authorized via `s3:GetObject` (source) and `s3:PutObject` (destination).

## How to test
### Success path
1. Step Functions → Start execution with input `{}`  
2. Verify execution succeeds  
3. Verify output CSV exists in `production/state_counts/`

### Failure path
1. Temporarily change the copy destination bucket name to an invalid value
2. Run execution again
3. Confirm workflow routes to `NotifyFailureSNS`
4. Confirm you receive an SNS email notification
5. Revert the bucket name back and retest success

## Evidence
Screenshots are stored in `/evidence/screenshots/`:
- Step Functions definition
- Successful execution graph
- Failure execution graph reaching SNS state
- SNS topic + confirmed subscription
- Email alert received
- S3 production output file

