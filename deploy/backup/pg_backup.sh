#!/bin/sh
set -eu

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
DAILY_DIR=/backups/daily
WEEKLY_DIR=/backups/weekly
mkdir -p "$DAILY_DIR" "$WEEKLY_DIR"

DAILY_FILE="$DAILY_DIR/marketing-$TIMESTAMP.sql.gz"
AIRFLOW_FILE="$DAILY_DIR/airflow-$TIMESTAMP.sql.gz"

PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h postgres -U airflow -d marketing | gzip > "$DAILY_FILE"
PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h postgres -U airflow -d airflow   | gzip > "$AIRFLOW_FILE"

if [ "$(date +%u)" = "7" ]; then
    cp "$DAILY_FILE"   "$WEEKLY_DIR/"
    cp "$AIRFLOW_FILE" "$WEEKLY_DIR/"
fi

find "$DAILY_DIR"  -type f -name '*.sql.gz' -mtime +7  -delete
find "$WEEKLY_DIR" -type f -name '*.sql.gz' -mtime +28 -delete

echo "[backup] ok $TIMESTAMP"
