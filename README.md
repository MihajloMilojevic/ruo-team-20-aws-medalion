# Social Media Data Pipeline — AWS Medalion Arhitektura

Projekat iz predmeta **Računarstvo u oblaku**. Platforma za prikupljanje, procesiranje, čuvanje i analizu podataka sa Hacker News i X (Twitter) platformi, implementirana na AWS-u prateći Medalion arhitekturu.

## Arhitektura

```
[EventBridge Timer] (jednom dnevno)
        │
        ▼
┌─────────────────┐
│  1. Ingestion   │ → S3 bronze/  (sirovi JSON)
│     Lambda      │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ 2. Normalization│ → S3 silver/ (Parquet, particionisano)
│     Lambda      │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ 3. Transformation│ → S3 gold/  (Parquet, metrike/KPI)
│     Lambda      │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  4. Delivery   │ → PostgreSQL (EC2)
│     Lambda     │
└─────────────────┘
        │
        ▼
  Apache Superset (EC2, Docker)

  ─── Svaka Lambda: Greška → Notification Lambda → Discord ───
```

## Struktura projekta

```
social-media-pipeline/
│
├── modules/
│   ├── networking/      # VPC, Subnets, Security Groups, NAT Gateway
│   ├── storage/         # S3 Data Lake (bronze/silver/gold)
│   ├── compute/         # Lambda funkcije, IAM Role-ovi, EC2
│   └── orchestration/   # Step Functions, EventBridge
│
├── src/
│   ├── ingestion/       # Lambda: Hacker News API → S3 bronze
│   ├── normalization/   # Lambda: bronze → silver (Parquet, awswrangler)
│   ├── analytics/       # Lambda: silver → gold (metrike, KPI)
│   └── delivery/        # Lambda: gold → PostgreSQL
│
├── main.tf
├── variables.tf
├── outputs.tf
├── terraform.tfvars.example
└── .gitignore
```

## Redosled implementacije

| Korak | Modul | Status |
|-------|-------|--------|
| 1 | `modules/networking` + `modules/storage` | ✅ Implementirano |
| 2 | `src/ingestion` + `modules/compute` (Bronze Lambda) | 🔜 Sledeće |
| 3 | `src/normalization` (Silver Lambda + awswrangler) | ⏳ Na čekanju |
| 4 | `src/analytics` (Gold Lambda) | ⏳ Na čekanju |
| 5 | `src/delivery` + EC2 + Superset | ⏳ Na čekanju |
| 6 | `modules/orchestration` (Step Functions + Discord) | ⏳ Na čekanju |

## Pokretanje

### Preduslovi
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5.0
- [AWS CLI](https://aws.amazon.com/cli/) konfigurisan (`aws configure`)
- AWS nalog (Free Tier dovoljan)

### Setup

```bash
# 1. Klonirajte repo
git clone <repo-url>
cd social-media-pipeline

# 2. Kreirajte terraform.tfvars
cp terraform.tfvars.example terraform.tfvars
# Popunite discord_webhook_url u terraform.tfvars

# 3. Inicijalizujte Terraform
terraform init

# 4. Pregled plana
terraform plan

# 5. Primenite infrastrukturu
terraform apply
```

## AWS Free Tier resursi koji se koriste

| Servis | Free Tier limit | Korišćenje |
|--------|----------------|-----------|
| S3 | 5 GB | Data Lake (bronze/silver/gold) |
| Lambda | 1M zahteva/mesec | 4 funkcije × 1/dan |
| Step Functions | 4.000 tranzicija/mesec | 1 workflow/dan |
| EC2 t2.micro | 750 h/mesec | Superset + PostgreSQL |
| EventBridge | Besplatno | Dnevni okidač |
