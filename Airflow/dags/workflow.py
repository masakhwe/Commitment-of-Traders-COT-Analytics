import sys
import os
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from google.oauth2 import service_account
from google.cloud import storage


class Pipeline(object):

    def __init__(self):
        self.credentials_location = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.gcp_bucket_name = os.environ.get("GCP_BUCKET")
        self.gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")
        self.path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


    def upload_to_gcs(self, bucket, object_name, local_file):
        """
            This function uploads cot data files to gcs
        """
        self.bucket = bucket
        self.object_name = object_name
        self.local_file = local_file
        self.storage = storage
        
        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        # End of Workaround

        client = self.storage.Client()
        bucket = client.bucket(self.bucket)

        blob = bucket.blob(self.object_name)
        blob.upload_from_filename(self.local_file)


    def organize_columns(self, local_file):
        """
            This function does the following:
                2. organizes the report dates into one column since they spread 3 different columns by default
                3. Writes the resulting df into locally for processing by the next task
        """
        organize_df = pd.read_csv(local_file)

    
        organize_df['Report_Date_as_YYYY-MM-DD'] = pd.to_datetime(organize_df['Report_Date_as_YYYY-MM-DD'], yearfirst=True)
        organize_df['Report_Date_as_YYYY-MM-DD'] = organize_df['Report_Date_as_YYYY-MM-DD'].dt.date
        organize_df.rename({'Report_Date_as_YYYY-MM-DD': 'report_date'}, axis=1, inplace=True)
      
        organize_df.to_parquet(f"{self.path_to_local_home}/organize_columns.parquet")


  

    def get_latest_data(self):
        """
        This function fetches the most recent unprocessed data
        """
        path = f"{self.path_to_local_home}/organize_columns.parquet"
        df_latest = pd.read_parquet(path)

        report_release = date.today() - timedelta(days = 4)
        df_latest = df_latest[df_latest['report_date'] >= report_release]
        

        df_latest.to_parquet(f"{self.path_to_local_home}/latest_data.parquet")



    def clean_columns(self):
        """
            This function makes corrections on some columns and uploads to gcs
        """ 
        path = f"{self.path_to_local_home}/latest_data.parquet"
        df_cleaned_cols  = pd.read_parquet(path)

        # separate market and Exchange names
        df_cleaned_cols['market_name'] = df_cleaned_cols['Market_and_Exchange_Names'].str.split('-').str[0]
        df_cleaned_cols['exchange_name'] = df_cleaned_cols['Market_and_Exchange_Names'].str.split('-').str[1]
        df_cleaned_cols = df_cleaned_cols.drop('Market_and_Exchange_Names', axis=1)
        
        # make column headers lower case
        df_cleaned_cols.columns = [x.lower() for x in df_cleaned_cols.columns]
        
        # Reorganize columns
        start_cols = ['market_name', 'exchange_name', 'report_date']
        df_cleaned_cols = df_cleaned_cols[start_cols + [col for col in df_cleaned_cols.columns if col not in start_cols]]
        
        df_cleaned_cols.to_parquet(f"{self.path_to_local_home}/clean_columns.parquet")

        # upload local parquet file to gcs
        bucket = self.gcp_bucket_name
        upload_location = f"cleaned/paquet/cot_cleaned_{datetime.today()}.parquet"
        local_file = f"{self.path_to_local_home}/clean_columns.parquet"
        self.upload_to_gcs(bucket, upload_location, local_file)



    def write_df_to_bigquery(self):
        """
            This function Writes to BigQuery as a table that will be consumed by dbt
        """
        path = f"{self.path_to_local_home}/clean_columns.parquet"
        df_bigquery = pd.read_parquet(path)

        authorize = service_account.Credentials.from_service_account_file(self.credentials_location)
        df_bigquery.to_gbq(
                        destination_table = 'committment_of_traders.cot',
                        project_id = self.project_id,
                        credentials = authorize,
                        if_exists = 'append'
                    )


        