#!/bin/sh
# Runs once, on the very first start of Postgres (empty volume):
# creates Airflow's own user and database next to MLflow's.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE USER airflow WITH PASSWORD :'pw';
SQL
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE airflow OWNER airflow;"
