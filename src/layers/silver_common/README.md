# silver_common Lambda Layer

Shared Silver-layer helper module (`silver_common.py`) plus its one third-party
dependency (`beautifulsoup4`), packaged as a Lambda Layer instead of being
copied into every Silver Lambda's source folder.

## Structure required by AWS

```
layers/silver_common/
└── python/
    ├── silver_common.py   <- already here, do not move
    └── bs4/, soupsieve/, ... <- installed by you, see below
```

AWS Lambda Layers for Python must have the importable code directly under a
top-level `python/` folder (or `python/lib/python3.12/site-packages/`) — both
work, `python/` is used here for simplicity.

## Manual step (do this yourself, per the instructions)

From the `layers/silver_common/` directory:

```bash
pip install beautifulsoup4>=4.12,<5 -t python/
```

This installs `bs4/`, `soupsieve/`, and their metadata folders next to
`silver_common.py` inside `python/`. Terraform (`modules/compute/main.tf`)
zips this whole `layers/silver_common/python/` tree into the
`aws_lambda_layer_version.silver_common` resource.

## Note on LocalStack

LocalStack Community does not support Lambda Layers (see `KNOWLEDGE.md` 6.2).
This layer will attach and work correctly on real AWS. If you still need to
run `normalization_hn` / `normalization_x` locally against LocalStack
Community, you'll need to fall back to copying `silver_common.py` (and a
`pip install -t .` of `beautifulsoup4`) into the Lambda's own source folder
for local testing only — don't commit that copy, it's a local-only workaround.
