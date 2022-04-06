import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
from google.cloud import storage


# https://www.cftc.gov/files/dea/history/fut_fin_xls_2011.zip
# https://www.cftc.gov/files/dea/history/fut_fin_xls_2012.zip
# https://www.cftc.gov/files/dea/history/fut_fin_xls_2019.zip


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = "https://www.cftc.gov/files/dea/history"
DATASET_FILE = "cot_reports_{{execution_date.strftime(\'%Y\')}}.zip"
URL_TEMPLATE = URL_PREFIX + "/" + "fut_fin_xls_{{execution_date.strftime(\'%Y\')}}.zip"

# URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
# DATASET_FILE = "yellow_tripdata_{{execution_date.strftime(\'%Y_%b\')}}.csv"
# URL_TEMPLATE = URL_PREFIX + "/" + "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2011, 1, 1),
    "end_date": datetime(2022, 3, 31),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="cot_reports",
    schedule_interval="@yearly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="cot_reports_download_task",
        bash_command=f"curl -sS {URL_TEMPLATE} > {path_to_local_home}/{DATASET_FILE}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{DATASET_FILE}",
            "local_file": f"{path_to_local_home}/{DATASET_FILE}",
        },
    )


    download_dataset_task  >> local_to_gcs_task