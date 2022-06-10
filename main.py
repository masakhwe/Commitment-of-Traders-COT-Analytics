from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

import os
from datetime import datetime, date, timedelta
from dag import Dag

credentials_location = os.getenv('GCP_CREDENTIALS_LOCATION')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
gcp_bucket_name = os.environ.get("GCP_BUCKET")
gcp_temporary_bucket = os.environ.get("GCP_TEMP_BUCKET")


add_to_dag = Dag()


@add_to_dag.task()
def connection_setup():
    """
        This function sets up the spark connection and reads the data from cloud storage
    """
    data_date = datetime.today().strftime('%Y-%m-%d')

    spark = SparkSession.builder \
                .appName('cot_report')\
                .getOrCreate()

    cot_df = spark.read \
        .option('header', 'true') \
        .csv(f'gs://{gcp_bucket_name}/raw/*_{data_date}.txt')

    return cot_df



@add_to_dag.task(depends_on=connection_setup)
def get_latest_data(cot_df):
    """
        This function fetches the most recent unprocessed data
    """
    report_date = date.today() - timedelta(days = 4)
    cot_df = cot_df.withColumn('Report_Date_as_YYYY-MM-DD', cot_df['Report_Date_as_YYYY-MM-DD'].cast(types.DateType()))
    cot_df = cot_df.filter(cot_df['Report_Date_as_YYYY-MM-DD'] >= report_date)
    
    return cot_df



@add_to_dag.task(depends_on=get_latest_data)
def clean_columns(cot_df):
    """
        This fuction makes corrections on some colums
    """      
    cot_df = cot_df.withColumn('Contract_Units', F.substring(cot_df['Contract_Units'], 19,7))\
                .drop('As_of_Date_In_Form_YYMMDD')
    
    # Make all columns lower case
    for col in cot_df.columns:
        cot_df = cot_df.withColumnRenamed(col, col.lower())
    
    return cot_df


@add_to_dag.task(depends_on=clean_columns)
def add_schema(cot_df):
    """
        This function establishes the schema for each of the columns
    """
    desired_schema = [
            types.StringType(),types.DateType(),types.StringType(),types.StringType(),types.StringType(),types.StringType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),
            types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),
            types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),types.IntegerType(),
            types.IntegerType(),types.StringType(),types.StringType(),types.FloatType(),types.FloatType(),types.FloatType(),
            types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.FloatType(),types.StringType(),
            types.StringType(),types.StringType(),types.StringType(),types.StringType(),types.StringType()
    ]
    
    for idx, col in enumerate(cot_df.columns):
        for idx_s, dtype in enumerate(desired_schema):
            if idx == idx_s:
                cot_df = cot_df.withColumn(col, cot_df[col].cast(dtype))
            else:
                pass
            
    return cot_df



@add_to_dag.task(depends_on=add_schema)
def write_to_parquet(cot_df):
    """
        This function writes the cleaned data back to cloud storage in parquet format
    """
    cot_df.write.parquet(f'gs://{gcp_bucket_name}/cleaned/pq', mode='append')

    return cot_df



@add_to_dag.task(depends_on=add_schema)
def write_to_bigquery(cot_df):
    """
        This function Writes to BigQuery as a table that will be consumed by dbt
    """
    cot_df.write \
                .format('bigquery') \
                .option('project', PROJECT_ID) \
                .option('parentProject', PROJECT_ID) \
                .option('table', 'committment_of_traders.cot') \
                .option("temporaryGcsBucket",f"{gcp_temporary_bucket}") \
                .mode('append') \
                .save()

    return cot_df


add_to_dag.run()