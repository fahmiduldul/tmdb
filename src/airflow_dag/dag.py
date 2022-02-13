from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
import datetime as dt
from google.cloud.dataproc_v1.types import PySparkBatch, EnvironmentConfig


PROJECT_ID = "de-porto"

MOVIES_BATCH_CONFIG = {
    "main_python_file_uri": "gs://de-porto/qoala/script/movies_table.py"
}

with DAG("tmdb", start_date=dt.datetime(2022, 1, 1), catchup=False) as dag:

    extract = DummyOperator(dag="dummy")

    movies_task = DataprocCreateBatchOperator(
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/movies_table.py"
        }
    )

    series_task = DataprocCreateBatchOperator(
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/series_table.py"
        }
    )

    dimension_task = DataprocCreateBatchOperator(
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/dimension_tables.py"
        }
    )

    extract >> [dimension_task, movies_task, series_task]