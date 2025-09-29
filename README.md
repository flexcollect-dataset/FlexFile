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
