from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
import datetime as dt

PROJECT_ID = "de-porto"
CLUSTER_NAME = "tmdb"
REGION = "us-central1"


def get_files_in_gcs(bucket: str, prefix: str, extension: str):
    from google.cloud import storage
    gcs_client = storage.Client()

    res = []
    for blob in gcs_client.list_blobs(bucket, prefix=prefix, ):
        suffix = blob.name.split(".")[-1]
        if suffix == extension:
            res.append(f"gs://{bucket}/{blob.name}")

    return res

def load_to_bq(bucket: str, uri: str, table:str):
    from google.cloud import bigquery
    client = bigquery.Client()
    TABLE_ID = f"{PROJECT_ID}.de_porto.{table}"
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)

    file = get_files_in_gcs(bucket, uri, "parquet")
    for uri in file:
        job = client.load_table_from_uri(uri, TABLE_ID, job_config=config)
        job.result()


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
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/movies_table.py"},
        }
    )

    series_task = DataprocSubmitJobOperator(
        task_id="series_transform",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/series_table.py"},
        }
    )

    dimension_task = DataprocSubmitJobOperator(
        task_id="dimension_transform",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "gs://de-porto/qoala/script/dimension_tables.py"},
        }
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )

    load_movies = PythonOperator(
        task_id="load_movies",
        python_callable=load_to_bq,
        op_kwargs={"bucket": PROJECT_ID, "uri": "qoala/movies.parquet", "table": "movies"}
    )

    load_series = PythonOperator(
        task_id="load_series",
        python_callable=load_to_bq,
        op_kwargs={"bucket": PROJECT_ID, "uri": "qoala/series.parquet", "table": "series"}
    )

    load_companies = PythonOperator(
        task_id="load_companies",
        python_callable=load_to_bq,
        op_kwargs={"bucket": PROJECT_ID, "uri": "qoala/companies.parquet", "table": "companies"}
    )

    load_genres = PythonOperator(
        task_id="load_genres",
        python_callable=load_to_bq,
        op_kwargs={"bucket": PROJECT_ID, "uri": "qoala/genres.parquet", "table": "genres"}
    )

    extract >> create_cluster >> [dimension_task, movies_task, series_task] >> delete_cluster
    dimension_task >> [load_companies, load_genres]
    movies_task >> load_movies
    series_task >> load_series
