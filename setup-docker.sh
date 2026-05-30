#!/bin/bash

set -e

source .venv/bin/activate  # samo ovo treba za tflocal

echo "Starting LocalStack..."
docker compose -f docker-compose.localstack.yml up -d

echo "Waiting for LocalStack to be ready..."
localstack wait -t 60

echo "LocalStack is ready. Applying Terraform..."
tflocal apply --auto-approve

echo "Done!"