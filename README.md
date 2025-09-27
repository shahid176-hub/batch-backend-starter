# Batch Backend Starter (Phase-2)

Dockerized microservices for Task 1 (Batch Processing):

- **MinIO** (S3-compatible) with buckets: `lake/raw`, `lake/processed`, `lake/exports`
- **Postgres** (warehouse) with table `features.zone_quarter_agg`
- **Airflow** (quarterly DAG) to upload raw CSVs to MinIO
- **Spark** job to clean + aggregate NYC taxi trips and write Parquet + Postgres
- **FastAPI** delivery service: `/features?year=YYYY&quarter=Q` returns JSON/CSV

## 0) Prereqs
- Docker Desktop, Git
- Place some taxi CSVs in `airflow/dags/_data/raw/` (e.g., yellow_tripdata_2023-01.csv)

## 1) Configure
```bash
cp config/env.example .env
```

## 2) Start stack
```bash
docker compose up -d
# Wait ~30-60s
```

UIs:
- Airflow:  http://localhost:8080  (admin/admin)
- MinIO:    http://localhost:9001  (minio/minio123)

## 3) Upload raw CSVs
- Put CSVs in `airflow/dags/_data/raw/`.
- In Airflow UI, trigger DAG `quarterly_batch_pipeline` to upload them to `s3a://lake/raw/nyc_taxi/`.

## 4) Run Spark job
Add Hadoop AWS jars to `./spark/jars/` if needed (for s3a). Then:
```bash
docker compose exec spark spark-submit /opt/spark/jobs/clean_aggregate.py
```

## 5) Verify Postgres
```bash
docker compose exec postgres   psql -U postgres -d warehouse   -c "SELECT * FROM features.zone_quarter_agg LIMIT 10;"
```

## 6) Delivery API
- JSON:  http://localhost:8000/features?year=2023&quarter=1
- CSV:   http://localhost:8000/features?year=2023&quarter=1&format=csv

## Notes
- For production-like orchestration, wire Spark submit from Airflow or a CI job.
- Governance: keep raw→processed→exports separation; use least-privilege creds.