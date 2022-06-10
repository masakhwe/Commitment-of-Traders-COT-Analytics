import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Dataproc operators
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from datetime import datetime
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
TEMP_BUCKET = os.environ.get("GCP_TEMP_BUCKET")

#sample url
# https://www.cftc.gov/files/dea/history/fut_fin_txt_2022.zip

URL_PREFIX = "https://www.cftc.gov/files/dea/history"
DATASET_FILE = "cot_reports_{{execution_date.strftime(\'%Y\')}}.zip"
DATASET_FILE_UNZIPED = "cot_reports_{{execution_date.strftime(\'%Y-%m-%d\')}}.txt"
URL_TEMPLATE = URL_PREFIX + "/" + "fut_fin_txt_{{execution_date.strftime(\'%Y\')}}.zip"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

CLUSTER_NAME = 'cot-pyspark-cluster'
REGION = 'europe-west6'
MAIN_PYTHON_FILE_URI = f'gs://{TEMP_BUCKET}/code/main.py'
PYTHON_FILE_URIS = f'gs://{TEMP_BUCKET}/code/dag.py'
jar_file_1 = f"gs://{TEMP_BUCKET}/code/data_proc_jar_files/gcs-connector-hadoop3-2.2.5.jar"
jar_file_2 = f"gs://{TEMP_BUCKET}/code/data_proc_jar_files/spark-bigquery-with-dependencies_2.12-0.24.2.jar"


# dataproc cluster configuration
CLUSTER_CONFIG = {
    'master_config': {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {'boot_disk_type': "pd-standard", "boot_disk_size_gb": 512}
    },

    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512}
    }
}

# PySpark job configuration
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": MAIN_PYTHON_FILE_URI,
        "python_file_uris": [PYTHON_FILE_URIS], 
        "jar_file_uris": [jar_file_1, jar_file_2]
    },
    
}


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
    "start_date": datetime(2022, 6, 10),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id = "commitment_of_traders",
    schedule_interval = '0 0 * * 6', # weekly on saturday morning
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['cot'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_task",
        bash_command=f"cd {path_to_local_home} \
                        && wget -O {DATASET_FILE} {URL_TEMPLATE}"
    )

    unzip_dataset_task = BashOperator(
    task_id="unzip_task",
    bash_command=f"cd {path_to_local_home} \
                    && unzip {DATASET_FILE} \
                    && mv FinFutYY.txt {DATASET_FILE_UNZIPED}" 
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{DATASET_FILE_UNZIPED}",
            "local_file": f"{path_to_local_home}/{DATASET_FILE_UNZIPED}",
        },
    )

    delete_processed_files_task = BashOperator(
        task_id = 'clean_up',
        bash_command = f"cd {path_to_local_home} \
                        && rm -rf cot_reports_*" 
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id = 'create_cluster',
        project_id = PROJECT_ID,
        cluster_config = CLUSTER_CONFIG,
        region = REGION,
        cluster_name = CLUSTER_NAME
    )

    submit_job = DataprocSubmitJobOperator(
        task_id = 'pyspark_task',
        job = PYSPARK_JOB,
        location = REGION,
        project_id = PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster",
        project_id = PROJECT_ID,
        cluster_name = CLUSTER_NAME,
        region = REGION
    )


    download_dataset_task >> unzip_dataset_task >> local_to_gcs_task >> delete_processed_files_task >>create_cluster >> submit_job >> delete_cluster