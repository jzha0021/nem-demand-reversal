# Snowflake ↔ S3 IAM bridge

Two JSON policies that AWS IAM needs in order for Snowflake's storage
integration `S3_NEM` to assume a role and read the project S3 bucket.

| File | Role in setup |
|---|---|
| `trust_policy.json` | Who Snowflake authenticates as (their IAM user) + the external-ID gate that prevents a confused-deputy attack from another Snowflake tenant. |
| `permission_policy.json` | What that role can do once assumed — list + read on the project S3 bucket (placeholder `<YOUR_S3_BUCKET>` — fill before applying). No write, no delete. |

The principal ARN and external ID inside `trust_policy.json` came from
Snowflake's `DESC STORAGE INTEGRATION S3_NEM` output. **They change if
the storage integration is recreated** — re-run DESC and regenerate this
JSON if you ever drop and recreate it.

## Before you apply

Replace the placeholders in both JSON files with values from your
deployment:

- `trust_policy.json` — paste `STORAGE_AWS_IAM_USER_ARN` and
  `STORAGE_AWS_EXTERNAL_ID` from the `DESC STORAGE INTEGRATION S3_NEM`
  output (run `db/snowflake/02_storage_integration.sql` first).
- `permission_policy.json` — substitute `<YOUR_S3_BUCKET>` with the
  bucket name from `.env`'s `S3_BUCKET`.

## How to apply (AWS Console — recommended)

1. Sign in to AWS Console as an admin user (MFA recommended).
2. **IAM → Roles → Create role**.
3. *Trusted entity type:* **Custom trust policy**.
4. Paste the entire content of `trust_policy.json` (with placeholders filled) → Next.
5. *Permissions:* skip → Next (we'll attach an inline policy after).
6. *Role name:* `snowflake-nem-storage`.
7. *Description:* `Snowflake storage integration — read S3 bucket`.
8. Create role.
9. Open the new role → **Permissions** tab → **Add permissions → Create inline policy**.
10. Switch to **JSON** view → paste `permission_policy.json` content → Next.
11. *Policy name:* `s3-read-nem-demand-reversal` → Create policy.

## How to apply (AWS CLI)

```powershell
aws iam create-role `
  --role-name snowflake-nem-storage `
  --assume-role-policy-document file://db/snowflake/aws/trust_policy.json `
  --description "Snowflake storage integration -> S3 bucket" `
  --profile <YOUR_ADMIN_PROFILE>

aws iam put-role-policy `
  --role-name snowflake-nem-storage `
  --policy-name s3-read-nem-demand-reversal `
  --policy-document file://db/snowflake/aws/permission_policy.json `
  --profile <YOUR_ADMIN_PROFILE>
```

CLI route assumes the admin profile is configured in
`~/.aws/credentials` with IAM-write permissions; MFA-protected admins
may need an `aws sts get-session-token` step first.

## Verify

After the role is created, run `db/snowflake/03_stage_and_test.sql` in
Snowsight. The `LIST @S3_NEM_STAGE` query should return ~995 rows
matching the bucket contents.

If `LIST` returns an empty result or an error mentioning trust /
assumed-role, double-check:
- The principal ARN in `trust_policy.json` matches the current
  `STORAGE_AWS_IAM_USER_ARN` from `DESC STORAGE INTEGRATION S3_NEM`.
- The external ID matches `STORAGE_AWS_EXTERNAL_ID` exactly (the value
  contains `/` and `=` which must not be URL-encoded).
- The role name is exactly `snowflake-nem-storage` (matches the
  `STORAGE_AWS_ROLE_ARN` baked into the integration).
