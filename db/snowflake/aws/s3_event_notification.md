# S3 → Snowpipe event notification

Configure the project S3 bucket to push `ObjectCreated:*` events to
Snowflake's SQS queue whenever a parquet file lands under `parsed/`.
Snowpipe consumes those messages and routes each file to the matching
pipe based on the COPY INTO stage path.

This file uses placeholders for deployment-specific identifiers
(bucket name, AWS account ID, Snowflake SQS queue ARN). The real
values live in the operator's private runbook / secrets store, never
in the public repo.

## Values

| Field | Value |
|---|---|
| Bucket | `<S3 bucket name from .env S3_BUCKET>` |
| Prefix filter | `parsed/` |
| Suffix filter | `.parquet` (optional but recommended — skips edge cases) |
| Event types | `s3:ObjectCreated:*` (all create variants) |
| Destination | SQS queue (external account) |
| Destination ARN | `<notification_channel from SHOW PIPES output in 05_file_format_and_pipes.sql>` |

The queue lives in Snowflake's AWS account, not ours. Snowflake
auto-configures the queue's resource policy to accept events from our
bucket the moment STORAGE INTEGRATION is granted — no SQS work
needed on our side.

## AWS Console steps

1. AWS Console → **S3** → click the project bucket.
2. **Properties** tab → scroll to *Event notifications* → **Create event notification**.
3. Fill in:
   - *Event name:* `snowpipe-parsed-trigger`
   - *Prefix:* `parsed/`
   - *Suffix:* `.parquet`
   - *Event types:* ✓ **All object create events**
4. *Destination:* select **SQS queue** → **Enter SQS queue ARN**.
5. Paste the queue ARN above.
6. **Save changes**.

S3 may complain "AccessDenied: not authorized to perform: SQS:SendMessage"
on save if the queue policy isn't ready. That's a sign Snowflake's
auto-policy hasn't propagated; retry after ~1 minute.

## Verify (one-shot, destructive — empty environment only)

> **Warning:** the smoke test below writes a synthetic row directly
> into `NEM.RAW.REGION_5MIN` to prove the S3 → SQS → Snowpipe chain
> works end-to-end. Only run during initial bring-up against an empty
> raw table, OR in a maintenance window. Until the cleanup query at
> the bottom runs, do **not** trigger `dbt build`, recompute
> downstream marts, or refresh the public dashboard — the fake row
> would pollute aggregates and Streamlit's row-count panel until it's
> deleted.

```powershell
# Local smoke test — upload one tiny parquet to parsed/dispatch/
$env:S3_BUCKET = "<your bucket name>"
$env:AWS_PROFILE = "nem"
conda run -n nem_demand --no-capture-output python -c "
import sys; sys.path.insert(0, 'pipeline')
import pandas as pd
from _common import upload_parquet_to_s3
df = pd.DataFrame({
    'SETTLEMENTDATE': [pd.Timestamp('2026-05-30 10:00')],
    'REGIONID':       ['VIC1'],
    'INTERVENTION':   [0],
    'TOTALDEMAND':    [5500.0],
})
key, size = upload_parquet_to_s3(df, source='dispatch')
print('uploaded', key)
"
```

Then in Snowsight, within ~30 seconds:

```sql
SELECT COUNT(*) FROM NEM.RAW.REGION_5MIN WHERE REGIONID = 'VIC1';
-- expect: 1 (the smoke-test row)

-- Force-flush check if 30 s passes with no row:
ALTER PIPE NEM_PIPE_DISPATCH REFRESH;

-- Pipe-level diagnostics:
SELECT SYSTEM$PIPE_STATUS('NEM_PIPE_DISPATCH');
-- look for lastIngestedTimestamp matching the upload time

-- Per-file COPY history:
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'NEM.RAW.REGION_5MIN',
    START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

If the row appears, the chain works end-to-end. **Clean up before any
further pipeline work:**

```sql
DELETE FROM NEM.RAW.REGION_5MIN
 WHERE REGIONID = 'VIC1' AND TOTALDEMAND = 5500;
```

```powershell
aws s3 rm s3://<your bucket name>/parsed/dispatch/<key-from-upload> --profile nem
```
