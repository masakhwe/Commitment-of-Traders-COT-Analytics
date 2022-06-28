import sys
#sys.path.append('/Users/Manu/Commitment-of-Traders-COT-Analytics/Airflow/config')
import os
import numpy as np
import pandas as pd
from datetime import date, timedelta
from google.oauth2 import service_account
from google.cloud import storage
# from dotenv import load_dotenv


class Pipeline(object):

    def __init__(self):
        #load_dotenv()
        self.credentials_location = os.environ.get('GCP_CREDENTIALS_LOCATION')
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.gcp_bucket_name = os.environ.get("GCP_BUCKET")
        self.gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")
        self.credentials = os.environ.get("credentials")


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



    def organize_columns(self):
        """
            This function does the following:
                1. sets pandas connection to gcs and reads cot data into a dataframe
                2. organizes the report dates into one column since they spread 3 different columns by default
                3. Writes the resulting df into gcs for processing by the next task
        """
        organize_df = pd.DataFrame()
        current_year = date.today().year

        for year in range(2011, current_year + 1):
            path = f'gs://{self.gcp_bucket_name}/raw/cot_reports_{year}-*.txt'
            df = pd.read_csv(path, storage_options={"token": self.credentials})
            organize_df = organize_df.append(df, ignore_index=True)


        conditions = [(organize_df['Report_Date_as_YYYY-MM-DD'].isnull()),
                    (organize_df['Report_Date_as_YYYY-MM-DD'].notnull())
                    ]
        
        values = [organize_df['Report_Date_as_MM_DD_YYYY'],
                organize_df['Report_Date_as_YYYY-MM-DD']
                ]
        
        organize_df['report_date'] = np.select(conditions, values)
        
        # convert to datetime type
        organize_df['report_date'] = pd.to_datetime(organize_df['report_date'], yearfirst=True)
        
        organize_df = organize_df.drop(['Report_Date_as_MM_DD_YYYY', 'Report_Date_as_YYYY-MM-DD',  'As_of_Date_In_Form_YYMMDD'], axis=1)
        
    
        organize_df.to_parquet(f'gs://{self.gcp_bucket_name}/processing/organize_columns.parquet',
                            storage_options={"token": self.credentials})

  

    def get_latest_data(self):
        """
        This function fetches the most recent unprocessed data
        """
        path = f"gs://{self.gcp_bucket_name}/processing/organize_columns.parquet"
        df_latest = pd.read_parquet(path, storage_options={"token": self.credentials})

        report_release = date.today() - timedelta(days = 4)
        df_latest = df_latest[df_latest['report_date'] >= report_release]
        

        df_latest.to_parquet(f'gs://{self.gcp_bucket_name}/processing/latest_data.parquet',
                            storage_options={"token": self.credentials})



    def clean_columns(self):
        """
            This fuction makes corrections on some columns
        """ 
        path = f"gs://{self.gcp_bucket_name}/processing/latest_data.parquet"
        df_cleaned_cols  = pd.read_parquet(path, storage_options={"token": self.credentials})

        # separate market and Exchange names
        df_cleaned_cols['market_name'] = df_cleaned_cols['Market_and_Exchange_Names'].str.split('-').str[0]
        df_cleaned_cols['exchange_name'] = df_cleaned_cols['Market_and_Exchange_Names'].str.split('-').str[1]
        df_cleaned_cols = df_cleaned_cols.drop('Market_and_Exchange_Names', axis=1)
        
        # make column headers lower case
        df_cleaned_cols.columns = [x.lower() for x in df_cleaned_cols.columns]
        
        # Reorganize columns
        start_cols = ['market_name', 'exchange_name', 'report_date']
        df_cleaned_cols = df_cleaned_cols[start_cols + [col for col in df_cleaned_cols.columns if col not in start_cols]]
        
        df_cleaned_cols.to_parquet(f'gs://{self.gcp_bucket_name}/processing/clean_columns.parquet',
                            storage_options={"token": self.credentials})



    def write_df_to_bigquery(self):
        """
            This function Writes to BigQuery as a table that will be consumed by dbt
        """
        path = f"gs://{self.gcp_bucket_name}/processing/clean_columns.parquet"
        df_bigquery = pd.read_parquet(path, storage_options={"token": self.credentials})

        authorize = service_account.Credentials.from_service_account_file(self.credentials)
        df_bigquery.to_gbq(
                        destination_table = 'committment_of_traders.cot_pandas',
                        project_id = self.project_id,
                        credentials = authorize,
                        if_exists = 'replace'
                    )