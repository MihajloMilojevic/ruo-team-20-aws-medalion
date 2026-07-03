#!/bin/bash
set -euxo pipefail
exec > /var/log/user-data.log 2>&1

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl
curl -fsSL https://get.docker.com | sh

mkdir -p /opt/pipeline
cd /opt/pipeline

# ----------------------------
# Postgres init
# ----------------------------
cat > init.sql <<'SQL'
CREATE DATABASE superset_meta;
SQL

# ----------------------------
# Superset config
# ----------------------------
cat > superset_config.py <<'PYCFG'
SECRET_KEY = "${superset_secret_key}"

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://${db_username}:${db_password}@postgres:5432/superset_meta"
)
PYCFG

# ----------------------------
# Docker Compose
# ----------------------------
cat > docker-compose.yml <<'COMPOSE'
services:
  postgres:
    image: postgres:16
    restart: unless-stopped
    environment:
      POSTGRES_USER: "${db_username}"
      POSTGRES_PASSWORD: "${db_password}"
      POSTGRES_DB: "${db_name}"
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${db_username} -d ${db_name}"]
      interval: 5s
      timeout: 5s
      retries: 30

  superset-init:
    image: apache/superset:4.1.2
    restart: "no"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SUPERSET_CONFIG_PATH: /app/superset_config.py
      ADMIN_USERNAME: "${superset_admin_username}"
      ADMIN_PASSWORD: "${superset_admin_password}"
    volumes:
      - ./superset_config.py:/app/superset_config.py:ro
    command:
      - /bin/bash
      - -c
      - |
        set -e
        pip install --no-cache-dir psycopg2-binary
        superset db upgrade
        superset fab create-admin \
          --username "$$ADMIN_USERNAME" \
          --firstname Admin \
          --lastname User \
          --email admin@superset.local \
          --password "$$ADMIN_PASSWORD" \
          || true
        superset init

  superset:
    image: apache/superset:4.1.2
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      superset-init:
        condition: service_completed_successfully
    environment:
      SUPERSET_CONFIG_PATH: /app/superset_config.py
    volumes:
      - ./superset_config.py:/app/superset_config.py:ro
    ports:
      - "8088:8088"
    command: >
      bash -c "
      pip install --no-cache-dir psycopg2-binary &&
      /usr/bin/run-server.sh
      "

volumes:
  pgdata:
COMPOSE

docker compose up -d