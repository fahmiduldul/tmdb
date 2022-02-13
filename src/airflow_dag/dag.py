from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
import datetime as dt
from google.cloud.dataproc_v1.types import PySparkBatch, EnvironmentConfig

PROJECT_ID = "de-porto"

def load_to_bq(**kwargs):
    ti = kwargs["ti"]


with DAG("tmdb", schedule_interval="@weekly", start_date=dt.datetime(2022, 1, 1), catchup=False) as dag:

    extract = DummyOperator(task_id="extract")

    movies_task = DataprocCreateBatchOperator(
        task_id="movies_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/movies_table.py"
        }
    )

    series_task = DataprocCreateBatchOperator(
        task_id="series_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/series_table.py"
        }
    )

    dimension_task = DataprocCreateBatchOperator(
        task_id="dimension_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        batch_id=f"movies-{dt.datetime.now().timestamp()}",
        batch={
            "main_python_file_uri": "gs://de-porto/qoala/script/dimension_tables.py"
        }
    )

    extract >> [dimension_task, movies_task, series_task]