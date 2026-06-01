# Operational runbook

This is the operational reference for the live cloud pipeline: where every
object lives, how to rebuild from scratch, what daily costs look like, and
the known sharp edges. The architecture overview is in the project
[`README.md`](../README.md); this document is the cold-restart manual.

---

## Daily loop

```
GitHub Actions cron (0 15 * * * UTC = 01:00 AEST next day)
  │
  ├── fetch_aemo_current.py     → S3 raw/dispatch/* + parsed/dispatch/*.parquet
  ├── fetch_rooftop_current.py  → S3 raw/rooftop/*  + parsed/rooftop/*.parquet
  ├── fetch_open_meteo.py       → S3 parsed/weather/*.parquet
  │   (S3 event → SQS → Snowpipe auto-ingest, ~30 s end-to-end)
  │
  ├── sleep 60                  (Snowpipe queue drain buffer)
  │
  ├── dbt build --target snowflake
  │   (11 models, 50 schema tests; all `materialized=view`, ~10 s)
  │
  ├── predict.py --target snowflake --date <today AEST>
  │   (snowflake-sqlalchemy → MERGE into NEM.ANALYTICS.PREDICTIONS)
  │
  └── check_pipe_status.py --max-stale-hours 26
      (SYSTEM$PIPE_STATUS for the 3 pipes; non-zero exit on stale)
```

Streamlit Community Cloud reads `NEM.ANALYTICS` continuously and renders
the public dashboard at the deployment URL.

---

## Object inventory

This is the **template** shape of the deployment. Concrete identifiers
(your AWS account, Snowflake account, bucket name, admin user) are
deployment-specific and should be kept out of the public repo; record
them in your own local notes / secrets manager.

| Layer | Object | Path / identifier |
|---|---|---|
| AWS | S3 bucket | `<YOUR_S3_BUCKET>` |
| AWS | OIDC role (CI) | `arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/github-actions-nem-demand` |
| AWS | Snowflake storage role | `arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-nem-storage` |
| AWS | Local-dev IAM user | `local-dev-nem` (profile `nem` in `~/.aws/credentials`) |
| Snowflake | Account | `<YOUR_SNOWFLAKE_ACCOUNT>` (ap-southeast-2 / AWS) |
| Snowflake | Warehouse | `WH_NEM` — XSMALL, AUTO_SUSPEND 60 s |
| Snowflake | Database / schemas | `NEM.RAW`, `NEM.ANALYTICS` |
| Snowflake | Working role | `R_NEM_RW` (granted to the admin user) |
| Snowflake | Read-only role | `R_NEM_READ` (granted to the Streamlit user) |
| Snowflake | Streamlit user | `STREAMLIT_DASHBOARD` (DEFAULT_ROLE = R_NEM_READ) |
| Snowflake | Storage integration | `S3_NEM` (read-only on bucket) |
| Snowflake | External stage | `NEM.RAW.S3_NEM_STAGE` (mounts bucket root) |
| Snowflake | Snowpipes | `NEM_PIPE_DISPATCH`, `NEM_PIPE_ROOFTOP`, `NEM_PIPE_WEATHER` |
| Snowflake | Raw tables | `REGION_5MIN`, `ROOFTOP_PV_30MIN`, `WEATHER_DAILY` |
| Snowflake | Analytics tables | `PREDICTIONS` + 11 dbt views |
| GitHub | Workflow | `.github/workflows/daily.yml` |
| GitHub | Secrets | `AWS_ROLE_ARN`, `AWS_S3_BUCKET`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` |
| Streamlit | App | `streamlit_app.py` at repo root |

---

## Bit-identical sanity gates

Two gates catch any silent drift from the frozen leak-free LR pipeline:

1.  **Smoke test** — `pipeline/smoke_test_predict.py` replays the
    notebook 02 held-out window through `predict.py` and asserts the
    realised AUC equals the artefact-pickled value within 1e-6. Passes on
    **both** Postgres and Snowflake targets:

    ```
    target=postgres   AUC replay = 0.754907  AUC artefact = 0.754907  |gap| = 0
    target=snowflake  AUC replay = 0.754907  AUC artefact = 0.754907  |gap| = 0
    ```

2.  **Row-count parity** — after the one-shot
    `pipeline/backfill_to_snowflake.py` (93 monthly parquet files,
    ~120 MB, Snowpipe drained in < 10 min), Snowflake matches Postgres
    bit-for-bit:

    | Source | Postgres | Snowflake |
    |---|---:|---:|
    | dispatch | 1,977,135 | 1,977,135 |
    | rooftop | 332,710 | 332,710 |
    | weather | 6,980 | 6,980 |

---

## Cost

Snowflake's $400 trial credit covered the first 30 days. Steady-state
running cost (per AWS + Snowflake's published rates, Standard edition,
ap-southeast-2):

| Service | Daily usage | $ / year |
|---|---|---|
| Snowpipe (serverless) | ~3 files / day | ~$0.30 |
| WH_NEM (XSMALL × ~90 s / day for dbt + predict) | ~7 credits / year | ~$21 |
| S3 (≤ 2 GB year 2 onward) | 1.3 GB | < $1 |
| S3 PUT + SQS | well inside free tier | $0 |
| GitHub Actions | ~5 min / day = 150 min / month | $0 (free tier) |
| **Steady-state total** | | **~$25 / year** (~$2 / month) |

---

## Cold rebuild from scratch

If the entire cloud side were lost, the rebuild order:

1.  **AWS** — open account; create S3 bucket; set up OIDC role for
    GitHub Actions; (optional) create `local-dev-nem` IAM user for laptop
    pushes. JSON for both policies: [`db/snowflake/aws/`](../db/snowflake/aws/).
2.  **Snowflake** — open trial; run `db/snowflake/01_account_setup.sql`
    in Snowsight (db / wh / role / grants); run `02_storage_integration.sql`
    (returns `STORAGE_AWS_IAM_USER_ARN` + `STORAGE_AWS_EXTERNAL_ID`).
3.  **AWS again** — create IAM role `snowflake-nem-storage` with the
    trust + permission policies in `db/snowflake/aws/`; configure S3
    event notification on `parsed/` prefix pointing at the per-pipe SQS
    queue Snowflake provisioned automatically.
4.  **Snowflake again** — run `03_stage_and_test.sql` (validates
    `LIST @S3_NEM_STAGE` returns objects); run `04_raw_schema.sql`,
    `05_file_format_and_pipes.sql`, `06_predictions_schema.sql`.
5.  **Local config** — edit `~/.dbt/profiles.yml` to add the `snowflake`
    target (template below); `.env` gets `SNOWFLAKE_PASSWORD` for local
    + Streamlit.
6.  **Backfill** — `python pipeline/backfill_to_snowflake.py` repopulates
    the raw layer.
7.  **dbt build** — `dbt build --project-dir dbt --target snowflake`
    rebuilds analytics.
8.  **Smoke tests** — both `--target postgres` and `--target snowflake`
    must pass.
9.  **GitHub repo Secrets** — set `AWS_ROLE_ARN`, `AWS_S3_BUCKET`,
    `SNOWFLAKE_PASSWORD`; push `daily.yml`; trigger
    `workflow_dispatch` once to confirm green.
10. **Streamlit Cloud** — connect repo at share.streamlit.io; paste
    Snowflake creds into the deployment's Secrets tab; deploy.

### dbt profile template

`~/.dbt/profiles.yml`:

```yaml
nem_demand:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', 'localhost') }}"
      port: "{{ env_var('DB_PORT', '5432') | int }}"
      user: "{{ env_var('DB_USER', 'postgres') }}"
      password: "{{ env_var('DB_PWD') }}"
      dbname: "{{ env_var('DB_NAME', 'nem') }}"
      schema: analytics
      threads: 4
      sslmode: prefer

    snowflake:
      type: snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      password:  "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role:      R_NEM_RW
      warehouse: WH_NEM
      database:  NEM
      schema:    ANALYTICS
      threads: 4
      client_session_keep_alive: false
```

---

## Known issues / nice-to-haves

1.  **Parquet writer stores timestamps as ISO strings.** Snowflake's
    parquet reader silently renders native `TIMESTAMP[us]` as
    "Invalid date" through `MATCH_BY_COLUMN_NAME`; the workaround
    (`upload_parquet_to_s3` strftimes datetimes before write) is robust
    but means parquet on S3 carries strings, not native parquet
    timestamps. Revisit once Snowflake fixes the codec.
2.  **NEMWeb CURRENT retention is ~2 days for dispatch.** If the cron
    misses 48 h consecutively, those intervals are unrecoverable from
    CURRENT — would need either NEMWeb `Reports/Archive/` scraping or
    waiting for the next MMSDM monthly publish. Cron failure alerts
    (GitHub email on red runs + `check_pipe_status` exit code) keep this
    a theoretical risk.
3.  **MMSDM / CURRENT data gap.** MMSDM has ~10-day publication lag;
    CURRENT has ~2-day retention. Between them is a multi-day window
    where neither source has data. The historical backfill ran from
    Postgres (which already had MMSDM-derived rows), so this gap doesn't
    affect current Snowflake state; it would matter for a future
    cold-restart backfill of the same multi-day window from scratch.
4.  **Monthly reversal_pct dips on the trailing partial month.** Current
    `v_monthly_reversal_rate` doesn't filter for complete months, so the
    rightmost month in the Streamlit chart usually drops sharply until
    the calendar month ends. Filtering on `n_trading_days >= 25` in the
    chart query is the cosmetic fix.
