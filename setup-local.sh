#!/bin/bash

set -e
set -a        # auto-export sve varijable koje se ucitaju
source .env
set +a        # iskljuci auto-export

source .venv/bin/activate

echo "Starting LocalStack..."
localstack start -d

echo "Waiting for LocalStack to be ready..."
localstack wait -t 60

echo "LocalStack is ready. Applying Terraform..."
tflocal apply --auto-approve

echo "Done!"