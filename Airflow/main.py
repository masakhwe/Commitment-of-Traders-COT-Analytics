import os
import numpy as np
import pandas as pd
from datetime import date, timedelta
from google.oauth2 import service_account
from google.cloud import storage
from dotenv import load_dotenv


class Pipeline(object):

    def __init__(self):
        load_dotenv()
        self.credentials_location = os.environ.get('GCP_CREDENTIALS_LOCATION')
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.gcp_bucket_name = os.environ.get("GCP_BUCKET")
        self.gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")
        self.credentials = os.environ.get("credentials")


    def upload_to_gcs(self, bucket, object_name, local_file):
        """
            Upload cot files to google cloud storage
        """
        self.buckek = bucket
        self.object_name = object_name
        self.local_file = local_file
        
        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        # End of Workaround

        client = self.storage.Client()
        bucket = client.bucket(self.bucket)

        blob = bucket.blob(self.object_name)
        blob.upload_from_filename(self.local_file)


    def pandas_gcp_connection(self):
        """
            This function sets up the pandas connection and reads the data from cloud storage
        """
        cot_df = pd.DataFrame()
        current_year = date.today().year

        for year in range(2011, current_year + 1):
            path = f'gs://{self.gcp_bucket_name}/raw/cot_reports_{year}-*.txt'
            df = pd.read_csv(path, storage_options={"token": self.credentials})
            cot_df = cot_df.append(df, ignore_index=True)
    
        return cot_df


    def organize_columns(self, cot_df):
        """
            This function organizes the report dates into one column since currently
            they are spread in 3 different columns
        """
        conditions = [(cot_df['Report_Date_as_YYYY-MM-DD'].isnull()),
                    (cot_df['Report_Date_as_YYYY-MM-DD'].notnull())
                    ]
        
        values = [cot_df['Report_Date_as_MM_DD_YYYY'],
                cot_df['Report_Date_as_YYYY-MM-DD']
                ]
        
        cot_df['report_date'] = np.select(conditions, values)
        
        # convert to datetime type
        cot_df['report_date'] = pd.to_datetime(cot_df['report_date'], yearfirst=True)
        
        cot_df = cot_df.drop(['Report_Date_as_MM_DD_YYYY', 'Report_Date_as_YYYY-MM-DD',  'As_of_Date_In_Form_YYMMDD'], axis=1)
        
        return cot_df

  
    def get_latest_data(self, cot):
        """
        This function fetches the most recent unprocessed data
        """
        report_release = date.today() - timedelta(days = 4)
        cot_latest = cot[cot['report_date'] >= report_release]
        
        return cot_latest


    def clean_columns(self, cot_df):
        """
            This fuction makes corrections on some colums
        """ 
        # separate market and Exchange names
        cot_df['market_name'] = cot_df['Market_and_Exchange_Names'].str.split('-').str[0]
        cot_df['exchange_name'] = cot_df['Market_and_Exchange_Names'].str.split('-').str[1]
        cot_df = cot_df.drop('Market_and_Exchange_Names', axis=1)
        
        # make column headers lower case
        cot_df.columns = [x.lower() for x in cot_df.columns]
        
        # Reorganize columns
        start_cols = ['market_name', 'exchange_name', 'report_date']
        cot_df = cot_df[start_cols + [col for col in cot_df.columns if col not in start_cols]]
        
        return cot_df


    def write_df_to_gcs_in_parquet(self, cot_df):
        """
            This function writes the cleaned data back to cloud storage in parquet format
        """
        cot_df.to_parquet(f'gs://{self.gcp_bucket_name}/cleaned/pq_pandas/cot_report.parquet',
                            storage_options={"token": self.credentials}
                        )
        return cot_df


    def write_df_to_bigquery(self, cot_df):
        """
            This function Writes to BigQuery as a table that will be consumed by dbt
        """
        authorize = service_account.Credentials.from_service_account_file(self.credentials)
        cot_df.to_gbq(
                        destination_table = 'committment_of_traders.cot_pandas',
                        project_id = self.project_id,
                        credentials = authorize,
                        if_exists = 'replace'
                    )
        return cot_df