# Delivery & Visualization Handoff

Covers the two final pipeline components: the **delivery Lambda**
(gold Parquet → PostgreSQL) and the **visualization EC2 instance**
(PostgreSQL + Apache Superset in Docker). Everything is provisioned by
Terraform; the only manual steps left are inside the Superset UI (§5).

---

## 1. What was added

| Path | What |
|---|---|
| `modules/visualization/` | New module: Ubuntu 24.04 `t3.micro`, 20 GB gp3, public subnet, existing `ec2` SG, bootstrapped via cloud-init `user_data` |
| `modules/visualization/templates/user_data.sh.tpl` | Installs Docker, writes `docker-compose.yml` + Postgres init + `superset_config.py`, starts everything |
| `src/delivery/lambda_function.py` | Real implementation (was a `NotImplementedError` stub) |
| `src/layers/delivery_common/` | New Lambda Layer: `pg8000` (pure-Python PostgreSQL driver — no compiled psycopg2 wheels to match against the Lambda runtime) |
| `modules/compute/main.tf` | `delivery_common` layer resource; delivery Lambda now uses the AWS `AWSSDKPandas` layer + `delivery_common`, 512 MB / 300s, DB_* env vars |
| root `main.tf` / `variables.tf` / `outputs.tf` / `terraform.tfvars.example` | `visualization` module wiring, new variables, `superset_url`/IP outputs |

Not changed: `security` (delivery's IAM was already correct — the Postgres
connection is controlled by SG rules, not IAM), `networking` (the 5432
rules between `lambda_vpc` and `ec2` SGs already existed), `orchestration`
(delivery's error alarm already existed).

## 2. Why user_data instead of Ansible

Single host, one-time bootstrap. Ansible would add a control machine, SSH
inventory, and a second tool *outside* Terraform — and IaC is the
eliminatory requirement, so keeping the bootstrap inside `terraform apply`
means the graders see one command producing a fully working instance.
Ansible earns its keep for ongoing configuration management of fleets of
hosts; that's not this project.

## 3. Deploying

1. Add the new variables to `terraform.tfvars` (see
   `terraform.tfvars.example`): `db_password`, `superset_admin_password`,
   `superset_secret_key` (generate with `openssl rand -base64 42`), and
   optionally `ssh_public_key`.
2. `terraform apply`. Note the outputs: `superset_url`, `ec2_public_ip`,
   `ec2_private_ip`.
3. Wait ~3–5 minutes after apply — cloud-init is still installing Docker
   and pulling images. Progress log on the instance:
   `/var/log/user-data.log`. Then open `superset_url` and log in with the
   admin credentials from tfvars.

The instance runs three containers (`docker compose ps` in
`/opt/pipeline`): `postgres` (databases `analytics` for gold data and
`superset_meta` for Superset's own state), `superset-init` (one-shot
bootstrap, exits after `db upgrade` / `create-admin` / `init`), and
`superset` (UI on 8088).

**Instance recreation warning:** `user_data_replace_on_change = true`
means editing the bootstrap template recreates the instance, and Postgres
data lives on its root disk — rerun the delivery Lambda with
`{"full_refresh": true}` afterwards. The public IP also changes whenever
the instance is stopped/started (no Elastic IP, they're billed hourly
now); the *private* IP the Lambda uses survives stop/start, only
recreation changes it (and Terraform updates the Lambda env var
automatically on apply).

## 4. Delivery Lambda

Event contract, mirroring the rest of the pipeline:

```jsonc
{"date": "2026-07-01"}    // reload just that date (DELETE WHERE date=X, INSERT)
{}                        // same, defaulting to yesterday UTC
{"full_refresh": true}    // TRUNCATE every table, reload all of gold/
```

- One PostgreSQL table per Gold table, `CREATE TABLE IF NOT EXISTS` on
  every run — no separate DDL step to forget.
- `top_x_users_followers` has no date column (static snapshot) and is
  fully replaced every run regardless of mode.
- Empty Gold results leave the existing Postgres rows untouched (same
  skip-empty behavior as `write_gold`).
- Per-table transaction + try/except: one table failing rolls back and is
  reported in the returned `errors` dict without blocking the others —
  same isolation pattern as the analytics Lambda's metrics.
- Reads use awswrangler's `partition_filter` (values arrive as the raw
  Hive folder strings), *not* `filters` — deliberately avoiding the
  partition-dtype `TypeError` class of bug that bit the analytics Lambda.
- Connect timeout is 10s so an SG/host misconfiguration fails fast with a
  clear error instead of eating the whole Lambda timeout silently.

**Recommended workflow after the Gold backfill:** loop `analytics` once
per date over the historical range as planned, then invoke `delivery`
once with `{"full_refresh": true}` — one call loads everything.

Verified: the loader ran against a real PostgreSQL 16 with realistic
awswrangler dtypes (Categorical partition columns, numpy int64/float64,
NaN) — three consecutive runs (date mode twice + full refresh) produced
zero duplicate rows, NaN round-tripped to SQL NULL, and partition date
strings landed as proper `DATE` values.

## 5. Superset — manual UI steps (the only ones)

1. **Connect the analytics database:** Settings → Database Connections →
   `+ Database` → PostgreSQL, SQLAlchemy URI:

   ```
   postgresql+psycopg2://<db_username>:<db_password>@postgres:5432/analytics
   ```

   Host is the Docker service name `postgres` — Superset and PostgreSQL
   share a compose network on the same instance.
2. **Add datasets:** Datasets → `+ Dataset` → pick the connection, schema
   `public`, one dataset per table (`daily_post_counts`,
   `daily_users_metric`, `top_x_users_followers`, `top_hn_jobs_score`,
   `top_hn_stories_score`, `data_quality_score`; the two karma tables will
   stay empty — karma isn't in the Algolia payload, documented Gold
   caveat).
3. **Charts:** time-series bar for `daily_post_counts` (x=date, series by
   post_type, filter platform), time-series line for `daily_users_metric`
   (total_users/new_users by platform), plain tables or bar charts for the
   top-10 tables, and a table or big-number-per-table for
   `data_quality_score`. Assemble into one dashboard.

## 6. LocalStack

LocalStack Community cannot emulate EC2, so `terraform.tfvars` for local
runs sets `enable_ec2 = false` — the visualization module is skipped
(module `count = 0`) and the delivery Lambda's `DB_HOST` becomes an empty
string (invoking it then fails fast on connect, which is expected: there
is nothing to deliver to locally).

## 7. Troubleshooting

- **Superset 502 / connection refused right after apply** — cloud-init is
  still running; check `/var/log/user-data.log` and `docker compose ps`.
- **Delivery Lambda times out after ~10s per connect attempt** — DB_HOST
  wrong (stale private IP after instance recreation → `terraform apply`
  refreshes it) or the SG rules got modified. Test from the instance:
  `docker exec -it pipeline-postgres-1 psql -U <user> -d analytics`.
- **`create-admin` errors in superset-init logs on restart** — expected
  and harmless (`|| true`): the admin already exists.
- **Instance stopped/started and Superset URL dead** — public IP changed;
  `terraform refresh` + `terraform output superset_url` for the new one.
