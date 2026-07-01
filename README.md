# Social Media Data Pipeline — AWS Medallion Architecture

A university project for the **Cloud Computing** course. A platform for collecting, processing, storing, and analyzing data from Hacker News and X (Twitter), built on AWS following the Medallion architecture.

## Architecture

```
[EventBridge Timer] (daily at 02:00 UTC)
        │
        ▼
┌──────────────────┐
│  1. Ingestion    │ → S3 bronze/  (raw JSON, one file per item type)
│     Lambda       │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│ 2. Normalization │ → S3 silver/  (Parquet, partitioned)
│  HN Lambda + X   │
│     Lambda       │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  3. Analytics    │ → S3 gold/    (Parquet, metrics/KPIs)
│     Lambda       │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  4. Delivery     │ → PostgreSQL  (EC2)
│     Lambda       │
└──────────────────┘
        │
        ▼
  Apache Superset  (EC2, Docker)

  ── On any Lambda error: CloudWatch Alarm → SNS → Notification Lambda → Discord ──
```

## Project Structure

```
social-media-pipeline/
│
├── modules/
│   ├── networking/      # VPC, subnets, IGW, S3 Gateway Endpoint, security groups
│   ├── storage/         # S3 Data Lake (bronze / silver / gold)
│   ├── security/        # IAM roles and policies (one role per Lambda)
│   ├── compute/         # Lambda function definitions
│   └── orchestration/   # EventBridge schedule, SNS topic, CloudWatch alarms
│
├── src/
│   ├── ingestion/         # Bronze: Hacker News API → S3
│   ├── normalization_hn/  # Silver: bronze/hacker_news → Parquet
│   ├── normalization_x/   # Silver: bronze/x (covid/bitcoin/congress) → Parquet
│   ├── analytics/         # Gold: 8 metrics + Data Quality KPI → Parquet
│   ├── delivery/          # Delivery: gold → PostgreSQL (not yet implemented)
│   └── notification/      # Discord notifications via SNS
│
├── main.tf
├── variables.tf
├── outputs.tf
├── terraform.tfvars.example
├── docker-compose.localstack.yml
├── start.sh
└── .gitignore
```

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Infrastructure — all Terraform modules | ✅ Done |
| 2 | Bronze — Hacker News ingestion Lambda | ✅ Done |
| 3 | Notifications — Discord via SNS + CloudWatch alarms | ✅ Done |
| 4 | Silver — normalization_hn + normalization_x Lambdas (awswrangler, Parquet) | ✅ Done (untested end-to-end on LocalStack) |
| 5 | Gold — analytics Lambda (8 metrics + Data Quality KPI) | ✅ Done (untested end-to-end on LocalStack) |
| 6 | Delivery — PostgreSQL + EC2 + Superset | 🔜 Next |
| 7 | Orchestration — Step Functions for the full pipeline | ⏳ Pending |

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5.0
- [AWS CLI](https://aws.amazon.com/cli/) configured (`aws configure`)
- AWS account (Free Tier is sufficient)
- Python 3.12+ with a virtual environment (`python -m venv .venv`)
- [Docker](https://www.docker.com/) (for local testing with LocalStack)

## Running on AWS

```bash
# 1. Clone the repo
git clone https://github.com/MihajloMilojevic/ruo-team-20-aws-medalion
cd ruo-team-20-aws-medalion

# 2. Create terraform.tfvars from the example
cp terraform.tfvars.example terraform.tfvars
# Fill in discord_webhook_url in terraform.tfvars

# 3. Initialize Terraform
terraform init

# 4. Review the plan
terraform plan

# 5. Apply
terraform apply
```

## Running Locally (LocalStack)

```bash
# 1. Install dependencies
pip install terraform-local localstack

# 2. Create a .env file
cp terraform.tfvars.example terraform.tfvars
cat > .env << EOF
LOCALSTACK_AUTH_TOKEN=your_token_here   # optional, community version works without it
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=eu-central-1
EOF

# 3. Start LocalStack and apply Terraform
./start.sh
```

To manually invoke the ingestion Lambda:
```bash
aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name social-media-pipeline-dev-ingestion \
  --payload '{}' output.json && cat output.json
```

To backfill a specific date:
```bash
aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name social-media-pipeline-dev-ingestion \
  --payload '{"date": "2026-01-15"}' output.json
```

To manually invoke the normalization Lambdas (before Step Functions wiring):
```bash
# HN — defaults to yesterday, same as ingestion, or pass a date
aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name social-media-pipeline-dev-normalization-hn \
  --payload '{"date": "2026-01-15"}' output.json

# X — no default date; pass one of date / prefix / full_scan
aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name social-media-pipeline-dev-normalization-x \
  --payload '{"full_scan": true}' output.json
```

Before invoking the normalization Lambdas, install their dependencies (no Lambda Layer support on LocalStack Community — see `KNOWLEDGE.md` 6.7):
```bash
pip install -r src/normalization_hn/requirements.txt -t src/normalization_hn/
pip install -r src/normalization_x/requirements.txt -t src/normalization_x/
pip install -r src/analytics/requirements.txt -t src/analytics/
```

To manually invoke the analytics Lambda (after normalization has run for the same date):
```bash
aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name social-media-pipeline-dev-analytics \
  --payload '{"date": "2026-01-15"}' output.json && cat output.json
```

## AWS Free Tier Resources

| Service | Free Tier limit | Usage |
|---------|----------------|-------|
| S3 | 5 GB | Data Lake (bronze / silver / gold) |
| Lambda | 1M requests/month | 6 functions × ~daily |
| SNS | 1M publishes/month | Error notifications |
| CloudWatch | 10 alarms | One per Lambda function |
| EC2 t2.micro | 750 h/month | Superset + PostgreSQL (Phase 6) |
| EventBridge | Free | Daily trigger |

## Key Design Decisions

**No NAT Gateway** — Lambdas that need internet access (ingestion, notification) run outside the VPC. Lambdas that only need S3 or PostgreSQL run inside the VPC and use a free S3 Gateway Endpoint. This avoids the ~$32/month NAT Gateway cost.

**One IAM role per Lambda** — Each Lambda has exactly the S3 permissions it needs (`bronze/*` write for ingestion, `bronze/hacker_news/*` read + `silver/*` read/write for normalization_hn, `bronze/x/*` read + `silver/*` read/write for normalization_x, etc.). No shared roles.

**_SUCCESS marker pattern** — The ingestion Lambda writes one JSON file per item type and only writes `_SUCCESS` after all files succeed (X datasets get the same marker from `build_chunks.py`). Neither normalization Lambda will process a partition without this marker, preventing partial data from entering the silver layer.

**Two normalization Lambdas, not one** — `normalization_hn` and `normalization_x` are split because their Bronze formats have nothing in common (per-type JSON vs. filename-dispatched CSV/NDJSON). Both write the same Silver tables via `awswrangler`'s `overwrite_partitions`, so they never conflict. See `KNOWLEDGE.md` section 2.12.

**Step Functions planned** — The current EventBridge → Lambda chain will be replaced with a Step Functions state machine in Phase 7. This will also remove the `notify_on_error` decorator, which currently cannot reach SNS from inside the VPC.
