from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
import datetime as dt
from google.cloud.dataproc_v1.types import PySparkBatch, EnvironmentConfig

PROJECT_ID = "de-porto"
CLUSTER_NAME = "tmdb"
REGION = "us-central1"


with DAG("tmdb", schedule_interval="@weekly", start_date=dt.datetime(2022, 1, 1), catchup=False) as dag:

    extract = DummyOperator(task_id="extract")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
            },
            "worker_config": {
                "num_instances": 3,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
            },
        }
    )

    movies_task = DataprocSubmitJobOperator(
        task_id="movies_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/movies_table.py"},
        }
    )

    series_task = DataprocSubmitJobOperator(
        task_id="series_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/series_table.py"},
        }
    )

    dimension_task = DataprocSubmitJobOperator(
        task_id="dimension_transform",
        project_id=PROJECT_ID,
        region="asia-southeast1",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/dimension_tables.py"},
        }
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )

    extract >> create_cluster >> [dimension_task, movies_task, series_task] >> delete_cluster