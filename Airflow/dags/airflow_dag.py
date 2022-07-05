import sys
#sys.path.append('/Users/Manu/Commitment-of-Traders-COT-Analytics/Airflow/dags')
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime
from workflow import Pipeline

GCP_BUCKET = os.environ.get("GCP_BUCKET")
URL_PREFIX = "https://www.cftc.gov/files/dea/history"
DATASET_FILE = "cot_reports_{{execution_date.strftime(\'%Y\')}}.zip"
DATASET_FILE_UNZIPED = "cot_reports_{{execution_date.strftime(\'%Y-%m-%d\')}}.txt"
URL_TEMPLATE = URL_PREFIX + "/" + "fut_fin_txt_{{execution_date.strftime(\'%Y\')}}.zip"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


pipeline = Pipeline()



default_args = {
    "owner": "airflow",
    "start_date": days_ago(-4),
    "depends_on_past": False,
    "retries": 1,
}

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
        python_callable=pipeline.upload_to_gcs,
        op_kwargs={
            "bucket": GCP_BUCKET,
            "object_name": f"raw/{DATASET_FILE_UNZIPED}",
            "local_file": f"{path_to_local_home}/{DATASET_FILE_UNZIPED}",
        },
    )

    organize_columns_task = PythonOperator(
        task_id = "organize_columns_task",
        python_callable = pipeline.organize_columns,
        op_kwargs = {"local_file": f"{path_to_local_home}/{DATASET_FILE_UNZIPED}"}
    )

    get_latest_data_task = PythonOperator(
        task_id = "get_latest_data_task",
        python_callable = pipeline.get_latest_data
    )

    clean_columns_task = PythonOperator(
        task_id = "clean_columns_task",
        python_callable = pipeline.clean_columns
    )

    write_df_to_bigquery_task = PythonOperator(
        task_id = "write_df_to_bigquery_task",
        python_callable = pipeline.write_df_to_bigquery
    )

    delete_processed_files_task = BashOperator(
    task_id = 'clean_up',
    bash_command = f"cd {path_to_local_home} \
                    && rm -rf cot_reports_* \
                    && rm -rf *.parquet"
    )


    download_dataset_task >> unzip_dataset_task >> local_to_gcs_task >> organize_columns_task >> get_latest_data_task >> clean_columns_task >> write_df_to_bigquery_task >> delete_processed_files_task
