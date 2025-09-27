from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, boto3, glob

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

RAW_BUCKET = "lake"
LOCAL_RAW = "/opt/airflow/dags/_data/raw"

def ensure_local():
    os.makedirs(LOCAL_RAW, exist_ok=True)

def upload_raw_to_minio():
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS,
                      aws_secret_access_key=MINIO_SECRET)
    for path in glob.glob(f"{LOCAL_RAW}/*.csv"):
        key = f"raw/nyc_taxi/{os.path.basename(path)}"
        s3.upload_file(path, RAW_BUCKET, key)

def trigger_spark_job():
    # Lightweight: write a marker file; you will run spark-submit manually or wire CI later.
    with open('/opt/airflow/dags/_trigger_spark.txt', 'w') as f:
        f.write('triggered')
    return "ok"

with DAG(
    dag_id="quarterly_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@quarterly",
    catchup=False,
    tags=["batch","nyc-taxi"],
) as dag:

    t_prepare = PythonOperator(
        task_id="prepare_dirs",
        python_callable=ensure_local,
    )

    t_upload_to_minio = PythonOperator(
        task_id="upload_raw_to_minio",
        python_callable=upload_raw_to_minio,
    )

    t_trigger = PythonOperator(
        task_id="trigger_spark_job",
        python_callable=trigger_spark_job,
    )

    t_prepare >> t_upload_to_minio >> t_trigger