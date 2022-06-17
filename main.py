import os
import numpy as np
import pandas as pd
from datetime import date, timedelta
from google.oauth2 import service_account
from dag import Pipeline
from dotenv import load_dotenv


load_dotenv()
credentials_location = os.environ.get('GCP_CREDENTIALS_LOCATION')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
gcp_bucket_name = os.environ.get("GCP_BUCKET")
gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")
credentials = os.environ.get("credentials")


add_to_dag = Pipeline()


@add_to_dag.task()
def connection_setup():
    """
        This function sets up the pandas connection and reads the data from cloud storage
    """
    cot_df = pd.DataFrame()
    current_year = date.today().year

    for year in range(2011, current_year + 1):
        path = f'gs://{gcp_bucket_name}/raw/cot_reports_{year}-*.txt'
        df = pd.read_csv(path, storage_options={"token": credentials})
        cot_df = cot_df.append(df, ignore_index=True)
  
        
    return cot_df


@add_to_dag.task(depends_on=connection_setup)
def organize_columns(cot_df):
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


# @add_to_dag.task(depends_on=organize_columns)
# def get_latest_data(cot):
#     """
#     This function fetches the most recent unprocessed data
#     """
#     report_release = date.today() - timedelta(days = 4)
#     cot_latest = cot[cot['report_date'] >= report_release]
    
#     return cot_latest



@add_to_dag.task(depends_on=organize_columns)
def clean_columns(cot_df):
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



@add_to_dag.task(depends_on=clean_columns)
def write_to_parquet(cot_df):
    """
        This function writes the cleaned data back to cloud storage in parquet format
    """
    cot_df.to_parquet(f'gs://{gcp_bucket_name}/cleaned/pq_pandas/cot_report.parquet',
                        storage_options={"token": credentials}
                    )
    return cot_df



@add_to_dag.task(depends_on=clean_columns)
def write_to_bigquery(cot_df):
    """
        This function Writes to BigQuery as a table that will be consumed by dbt
    """
    authorize = service_account.Credentials.from_service_account_file(credentials)
    cot_df.to_gbq(
                    destination_table = 'committment_of_traders.cot_pandas',
                    project_id = PROJECT_ID,
                    credentials = authorize,
                    if_exists = 'replace'
                )
    return cot_df


add_to_dag.run()