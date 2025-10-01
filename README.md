# flexcollect

Repo for collecting data and add it to dataset

## Run as ECS task (no Lambda)

This project now runs directly as an ECS Fargate task. The container executes `src.lambda_function` (which calls `lambda_handler` via its `main()` function) once.

### Steps

1. Build and push the image:
   - Authenticate to ECR and set your `IMAGE_URI`.
   - Build and push:
     ```bash
     docker build -t "$IMAGE_URI" .
     docker push "$IMAGE_URI"
     ```
2. Update `infra/ecrtask.json`:
   - Set `containerDefinitions[0].image` to your `IMAGE_URI`.
   - Keep `entryPoint` as `["python","-u","-m","src.lambda_function"]`.
3. Register the task definition:
   - ```bash
     aws ecs register-task-definition --cli-input-json file://infra/ecrtask.json | cat
     ```
4. Run a one-off task:
   - ```bash
     aws ecs run-task \
       --cluster <your-cluster> \
       --launch-type FARGATE \
       --task-definition flexcollect-task \
       --network-configuration "awsvpcConfiguration={subnets=[subnet-xxxx,subnet-yyyy],securityGroups=[sg-zzzz],assignPublicIp=ENABLED}" | cat
     ```

Optional: To pass parameters to the task, set the env var `FC_EVENT_JSON` as a container override when calling `run-task`. The runner will decode it and forward it to your handler.

### Using S3 for TaxRecords.csv

By default the container reads and writes `data/TaxRecords.csv` inside the image. To use Amazon S3 instead, set the environment variable `TAX_CSV_S3_URI` to the object URI, for example:

```
TAX_CSV_S3_URI=s3://my-bucket/path/TaxRecords.csv
```

When set:
- The task downloads the CSV from S3 to a temp directory, enriches it, then uploads the result back to the same key.
- A timestamped backup of the original object is created alongside the key, e.g. `TaxRecords.csv.bak-20250101T120000Z`.

Required IAM permissions on the task role (minimum):

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:CopyObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/path/TaxRecords.csv",
        "arn:aws:s3:::my-bucket/path/TaxRecords.csv.bak-*"
      ]
    }
  ]
}
```

You also need `s3:ListBucket` on the bucket if your policies require it for `GetObject` path enforcement:

```
{
  "Effect": "Allow",
  "Action": ["s3:ListBucket"],
  "Resource": "arn:aws:s3:::my-bucket"
}
```

Notes:
- Ensure the container has network access to S3 (public endpoints or VPC endpoint).
- `boto3` is included in `requirements.txt`.

### Merge external dataset onto TaxRecords via ABN

You can merge additional columns from a separate dataset (CSV or Parquet) by matching on the `abn` field. Set these environment variables:

```
DATASET_S3_URI=s3://my-bucket/path/external_dataset.csv   # or .parquet, or a local path
DATASET_PREFIX=DS_                                       # optional, prefix for merged columns (default: DS_)
DATASET_COLUMNS=col1,col2,col3                           # optional, restrict merged columns; defaults to all except `abn`
DATASET_ONLY=true                                        # when true, perform only the merge and skip Gemini enrichment (default: true)
```

Requirements and behavior:
- The dataset must contain a column named `abn` (case-insensitive). ABNs are normalized to digits-only before joining.
- Supported formats: CSV (built-in) and Parquet (requires `pyarrow`).
- The merged columns are added to `TaxRecords.csv` with the configured prefix.
- When `TAX_CSV_S3_URI` is set, the merged CSV is uploaded back to the same key and a timestamped backup of the original is created.

Minimum IAM permissions for the dataset object (in addition to the `TaxRecords.csv` object above):

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/path/external_dataset.csv"
      ]
    }
  ]
}
```

Example ECS task env entries (see `infra/ecrtask.json`):

```
{ "name": "DATASET_S3_URI", "value": "s3://lambdadependency1/external_dataset.csv" },
{ "name": "DATASET_PREFIX", "value": "DS_" },
{ "name": "DATASET_COLUMNS", "value": "" },
{ "name": "DATASET_ONLY", "value": "true" }
```

### Resume and Idempotency

- ABN inserts are idempotent. Table `abn` has a unique index on `Abn` and inserts use `ON CONFLICT (Abn) DO NOTHING`.
- Postcode progress is persisted in Postgres table `kv_store` with keys `last_postcode_y` and `last_postcode_n` (per GST flag).
- By default, the task resumes from the next postcode after the last recorded one for each GST flag.

Environment variables:

- `RESUME_FROM_DB` (default: `true`): when true, resume from DB progress; set to `false` to always start from the beginning (or use `POSTCODE_START_INDEX`).
- `POSTCODE_START_INDEX`: optional zero-based index into the CSV to define a manual starting point.
- `MAX_POSTCODES`: optional cap on how many postcodes to process this run.

Resetting progress:

```sql
DELETE FROM kv_store WHERE k IN ('last_postcode_y','last_postcode_n');
```
