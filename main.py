import os
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from dag import Pipeline

credentials_location = os.environ.get('GCP_CREDENTIALS_LOCATION')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
gcp_bucket_name = os.environ.get("GCP_BUCKET")
gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")


add_to_dag = Pipeline()


@add_to_dag.task()
def connection_setup():
    """
        This function sets up the pandas connection and reads the data from cloud storage
    """
    cot = pd.DataFrame()
    current_year = date.today().year
    
    for yr in range(2011, current_year + 1):
        path = f'gs://{gcp_bucket_name}/raw/cot_reports_{yr}-*.txt'
        df = pd.read_csv(path, storage_options={"token": credentials})
        cot_df = cot.append(df, ignore_index=True)
        
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


@add_to_dag.task(depends_on=organize_columns)
def get_latest_data(cot):
    """
    This function fetches the most recent unprocessed data
    """
    report_release = date.today() - timedelta(days = 4)
    cot_latest = cot[cot['report_date'] >= report_release]
    
    return cot_latest



@add_to_dag.task(depends_on=get_latest_data)
def clean_columns(cot_df):
    """
        This fuction makes corrections on some colums
    """ 
    # make column headers lower case
    cot_df.columns = [x.lower() for x in cot_df.columns]
    
    # separate market and Exchange names
    cot_df['market_name'] = cot_df['Market_and_Exchange_Names'].str.split('-').str[0]
    cot_df['exchange_name'] = cot_df['Market_and_Exchange_Names'].str.split('-').str[1]
    
    cot_df = cot_df.drop('Market_and_Exchange_Names')
    
    return cot_df



@add_to_dag.task(depends_on=clean_columns)
def write_to_parquet(cot_df):
    """
        This function writes the cleaned data back to cloud storage in parquet format
    """
    pass



@add_to_dag.task(depends_on=clean_columns)
def write_to_bigquery(cot_df):
    """
        This function Writes to BigQuery as a table that will be consumed by dbt
    """
    pass


add_to_dag.run()