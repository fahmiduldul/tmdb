from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
import datetime as dt

PROJECT_ID = "de-porto"
DATASET_ID = 'tmdb'
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

def load_to_bq(bucket: str, uri: str, project_id: str, dataset_id: str, table:str):
    from google.cloud import bigquery
    client = bigquery.Client()
    TABLE_ID = f"{project_id}.{dataset_id}.{table}"
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)

    file = get_files_in_gcs(bucket, uri, "parquet")
    for uri in file:
        job = client.load_table_from_uri(uri, TABLE_ID, job_config=config)
        job.result()


def create_load_args(table_name: str):
    return {
        "bucket": PROJECT_ID,
        "uri": f"qoala/{table_name}.parquet",
        "project_id": PROJECT_ID,
        "dataset_id": DATASET_ID,
        "table": table_name
    }


with DAG("tmdb", schedule_interval="@weekly", start_date=dt.datetime(2022, 1, 1), catchup=False) as dag:

    # currently, there is problem extracting the file here
    # use DummyOperator temporarily
    extract = DummyOperator(task_id="extract")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2d-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
            },
            "worker_config": {
                "num_instances": 3,
                "machine_type_uri": "n2d-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
            },
        }
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done"
    )

    transform_task = {}
    transform_jobs = ["dimension", "series", "movies"]
    for transform_job in transform_jobs:
        job = DataprocSubmitJobOperator(
            task_id=f"{transform_job}_transform",
            project_id=PROJECT_ID,
            region=REGION,
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://de-porto/qoala/script/{transform_job}_table.py"},
            }
        )

        create_cluster >> job >> delete_cluster
        transform_task[transform_job] = job

    load_jobs = [
        {"table": "movies", "depend_on": transform_task["movies"]},
        {"table": "series", "depend_on": transform_task["series"]},
        {"table": "genres", "depend_on": transform_task["dimension"]},
        {"table": "companies", "depend_on": transform_task["dimension"]}
    ]

    for load_job in load_jobs:
        table_name = load_job["table"]
        job = PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_to_bq,
            op_kwargs=create_load_args(table_name)
        )

        load_job["depend_on"] >> job

    extract >> create_cluster
