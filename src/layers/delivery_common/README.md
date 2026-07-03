# delivery_common Lambda Layer

PostgreSQL driver (`pg8000`) for the delivery Lambda, packaged as a Lambda
Layer following the same pattern as `silver_common`.

## Why pg8000 and not psycopg2

`psycopg2` ships compiled C extensions, so the wheel installed on a dev
machine (Windows/macOS) does not run on Lambda's Linux runtime — it would
need a Linux-targeted install or a Docker build step. `pg8000` is pure
Python: the same files work everywhere, which fits this project's
no-build-step approach. Its dependencies (`scramp`, `asn1crypto`,
`python-dateutil`, `six`) are pure Python as well.

Parquet reading in the delivery Lambda does **not** come from this layer —
that comes from the AWS-hosted `AWSSDKPandas-Python312` layer (pandas +
awswrangler), the same one the analytics Lambda uses.

## Structure required by AWS

```
layers/delivery_common/
└── python/
    ├── pg8000/, scramp/, asn1crypto/, ...  <- pip-installed packages
    └── requirements.txt
```

AWS Lambda Layers for Python must have the importable code directly under a
top-level `python/` folder. Terraform (`modules/compute/main.tf`) zips the
whole `layers/delivery_common/` tree (so the zip contains `python/...`) into
the layer.

## Reinstalling / upgrading

From this directory:

```bash
pip install "pg8000>=1.31,<2" -t python/
```

Then `terraform apply` — the `archive_file` hash changes and a new layer
version is published automatically.
