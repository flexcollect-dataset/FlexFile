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

### Enrichment fields and concurrency

- The task enriches each CSV row by looking up ABN details from Postgres and adding these columns: `contact`, `website`, `address`, `email`, `sociallink`, `review`, `industry`, `documents`.
- Concurrency is controlled by env var `ABN_DETAILS_CONCURRENCY` (default: 5). Increase it cautiously based on your DB instance size and connection limits.
- Batch size is not used by the enrichment flow currently but you can keep `BATCH_SIZE` for future extensions.

Required database structure:

- A table `abn` with at least one of the following merge keys: `abn` (lowercase) or quoted `"Abn"`. The enrichment attempts both.
- The enrichment will map the requested output columns from any matching column names in `abn` (case-insensitive best-effort). Missing fields are written as empty strings.

Environment variables summary:

- `TAX_CSV_S3_URI`: when set to an S3 URI, the task will download, enrich, back up, and upload the CSV in-place. Otherwise it reads and writes `data/TaxRecords.csv` inside the container.
- `ABN_DETAILS_CONCURRENCY`: number of threads for parallel lookups (default 5).
- `BATCH_SIZE`: unused by current flow; reserved.

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
