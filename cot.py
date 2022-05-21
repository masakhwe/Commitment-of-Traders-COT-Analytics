import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

import os
import pandas as pd
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
MAIN_BUCKET = os.environ.get("GCP_BUCKET")
TEMP_BUCKET = os.environ.get("GCP_TEMP_BUCKET")

spark = SparkSession.builder \
    .appName('cot_report')\
    .getOrCreate()


# Reading data from google cloud storage
cot = spark.read \
    .text(f'gs://{MAIN_BUCKET}/raw/*')


# Extract the column names from the row object
col_row = cot.collect()[0].asDict()['value']


# Split col_row to get a list of columns and remove the extra quotation marks
cols_cleaned = []
cols = col_row.split(",")
for cl in cols:
    cols_cleaned.append(cl[1:-1])


# Generating new columns
cot_split = cot.select("*") # Copy the spark dataframe


#split the initial column "value"
split_cols = F.split(cot_split['value'], ",")

for key, value in enumerate(cols_cleaned):
    cot_split = cot_split.withColumn(value, split_cols.getItem(key))


# Add index column
cot_split = cot_split.withColumn('index', F.monotonically_increasing_id())

# filter out first column and drop value and index columns
cot_split = cot_split \
        .filter(cot_split['index'] >= 1) \
        .drop('value', 'index')


# Removing any leading or trailing spaces
for col_name in cot_split.columns:
    cot_split = cot_split.withColumn(col_name, F.trim(cot_split[col_name]))


# Convert to pandas dataframe
cot_panda = cot_split.toPandas()


# convert the Open_Interest_All column from string  to integer type
cot_panda = cot_panda[cot_panda['Open_Interest_All'].str.isnumeric()]
cot_panda['Open_Interest_All'] = cot_panda['Open_Interest_All'].astype(int)


# Temporary save to local environment
cot_panda.to_csv(f'gs://{TEMP_BUCKET}/code/cot_panda.csv', index=False)


# Define Schema
schema = types.StructType([
    types.StructField('Market_and_Exchange_Names', types.StringType(), True),
    types.StructField('As_of_Date_In_Form_YYMMDD', types.StringType(), True), 
    types.StructField('Report_Date_as_YYYY-MM-DD', types.DateType(), True), 
    types.StructField('CFTC_Contract_Market_Code', types.StringType(), True),
    types.StructField('CFTC_Market_Code', types.StringType(), True),
    types.StructField('CFTC_Region_Code', types.StringType(), True), 
    types.StructField('CFTC_Commodity_Code', types.StringType(), True),
    types.StructField('Open_Interest_All', types.IntegerType(), True),
    types.StructField('Dealer_Positions_Long_All', types.IntegerType(), True),
    types.StructField('Dealer_Positions_Short_All', types.IntegerType(), True),
    types.StructField('Dealer_Positions_Spread_All', types.IntegerType(), True),
    types.StructField('Asset_Mgr_Positions_Long_All', types.IntegerType(), True),
    types.StructField('Asset_Mgr_Positions_Short_All', types.IntegerType(), True),
    types.StructField('Asset_Mgr_Positions_Spread_All', types.IntegerType(), True),
    types.StructField('Lev_Money_Positions_Long_All', types.IntegerType(), True),
    types.StructField('Lev_Money_Positions_Short_All', types.IntegerType(), True),
    types.StructField('Lev_Money_Positions_Spread_All', types.IntegerType(), True),
    types.StructField('Other_Rept_Positions_Long_All', types.IntegerType(), True),
    types.StructField('Other_Rept_Positions_Short_All', types.IntegerType(), True),
    types.StructField('Other_Rept_Positions_Spread_All', types.IntegerType(), True),
    types.StructField('Tot_Rept_Positions_Long_All', types.IntegerType(), True),
    types.StructField('Tot_Rept_Positions_Short_All', types.IntegerType(), True),
    types.StructField('NonRept_Positions_Long_All', types.IntegerType(), True),
    types.StructField('NonRept_Positions_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_Open_Interest_All', types.IntegerType(), True),
    types.StructField('Change_in_Dealer_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_Dealer_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_Dealer_Spread_All', types.IntegerType(), True),
    types.StructField('Change_in_Asset_Mgr_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_Asset_Mgr_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_Asset_Mgr_Spread_All', types.IntegerType(), True),
    types.StructField('Change_in_Lev_Money_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_Lev_Money_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_Lev_Money_Spread_All', types.IntegerType(), True),
    types.StructField('Change_in_Other_Rept_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_Other_Rept_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_Other_Rept_Spread_All', types.IntegerType(), True),
    types.StructField('Change_in_Tot_Rept_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_Tot_Rept_Short_All', types.IntegerType(), True),
    types.StructField('Change_in_NonRept_Long_All', types.IntegerType(), True),
    types.StructField('Change_in_NonRept_Short_All', types.IntegerType(), True),
    types.StructField('Pct_of_Open_Interest_All', types.IntegerType(), True),
    types.StructField('Pct_of_OI_Dealer_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Dealer_Short_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Dealer_Spread_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Asset_Mgr_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Asset_Mgr_Short_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Asset_Mgr_Spread_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Lev_Money_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Lev_Money_Short_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Lev_Money_Spread_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Other_Rept_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Other_Rept_Short_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Other_Rept_Spread_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Tot_Rept_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_Tot_Rept_Short_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_NonRept_Long_All', types.FloatType(), True),
    types.StructField('Pct_of_OI_NonRept_Short_All', types.FloatType(), True),
    types.StructField('Traders_Tot_All', types.IntegerType(), True),
    types.StructField('Traders_Dealer_Long_All', types.IntegerType(), True),
    types.StructField('Traders_Dealer_Short_All', types.IntegerType(), True),
    types.StructField('Traders_Dealer_Spread_All', types.IntegerType(), True),
    types.StructField('Traders_Asset_Mgr_Long_All', types.IntegerType(), True),
    types.StructField('Traders_Asset_Mgr_Short_All', types.IntegerType(), True),
    types.StructField('Traders_Asset_Mgr_Spread_All', types.IntegerType(), True),
    types.StructField('Traders_Lev_Money_Long_All', types.IntegerType(), True),
    types.StructField('Traders_Lev_Money_Short_All', types.IntegerType(), True),
    types.StructField('Traders_Lev_Money_Spread_All', types.IntegerType(), True),
    types.StructField('Traders_Other_Rept_Long_All', types.IntegerType(), True),
    types.StructField('Traders_Other_Rept_Short_All', types.IntegerType(), True),
    types.StructField('Traders_Other_Rept_Spread_All', types.IntegerType(), True),
    types.StructField('Traders_Tot_Rept_Long_All', types.StringType(), True),
    types.StructField('Traders_Tot_Rept_Short_All', types.StringType(), True),
    types.StructField('Conc_Gross_LE_4_TDR_Long_All', types.FloatType(), True),
    types.StructField('Conc_Gross_LE_4_TDR_Short_All', types.FloatType(), True),
    types.StructField('Conc_Gross_LE_8_TDR_Long_All', types.FloatType(), True),
    types.StructField('Conc_Gross_LE_8_TDR_Short_All', types.FloatType(), True),
    types.StructField('Conc_Net_LE_4_TDR_Long_All', types.FloatType(), True),
    types.StructField('Conc_Net_LE_4_TDR_Short_All', types.FloatType(), True),
    types.StructField('Conc_Net_LE_8_TDR_Long_All', types.FloatType(), True),
    types.StructField('Conc_Net_LE_8_TDR_Short_All', types.FloatType(), True),
    types.StructField('Contract_Units', types.StringType(), True),
    types.StructField('CFTC_Contract_Market_Code_Quotes', types.StringType(), True),
    types.StructField('CFTC_Market_Code_Quotes', types.StringType(), True),
    types.StructField('CFTC_Commodity_Code_Quotes', types.StringType(), True),
    types.StructField('CFTC_SubGroup_Code', types.StringType(), True),
    types.StructField('FutOnly_or_Combined', types.StringType(), True)    
])


# Read from local environment and incorporate Schema
cot_panda_sp = spark.read \
        .option('header', 'true') \
        .schema(schema) \
        .csv(f'gs://{TEMP_BUCKET}/code/cot_panda.csv')



# Remove the extra quotation marks and brackets on multiple columns
error_cols = ['Market_and_Exchange_Names', 'Contract_Units', 'CFTC_Contract_Market_Code_Quotes', 'CFTC_Commodity_Code_Quotes', 'CFTC_SubGroup_Code', 'FutOnly_or_Combined']

for column in error_cols:
    cot_panda_sp = cot_panda_sp \
        .withColumn(column, F.regexp_replace(cot_panda_sp[column], '"', ""))


# Rename date column
cot_panda_sp = cot_panda_sp.withColumnRenamed('Report_Date_as_YYYY-MM-DD', 'Report_Date')


# Writing to file the cleaned version with correct data types
cot_panda_sp.write.parquet(f'gs://{MAIN_BUCKET}/cleaned/pq', mode='overwrite')


# Select the required columns for analysis

required_cols = [
 'Market_and_Exchange_Names',
 'Report_Date',
 'CFTC_Contract_Market_Code',
 'CFTC_Market_Code',
 'CFTC_Region_Code',
 'CFTC_Commodity_Code',
 'Open_Interest_All',
 'Dealer_Positions_Long_All',
 'Dealer_Positions_Short_All',
 'Dealer_Positions_Spread_All',
 'Asset_Mgr_Positions_Long_All',
 'Asset_Mgr_Positions_Short_All',
 'Asset_Mgr_Positions_Spread_All',
 'Lev_Money_Positions_Long_All',
 'Lev_Money_Positions_Short_All',
 'Lev_Money_Positions_Spread_All',
 'Other_Rept_Positions_Long_All',
 'Other_Rept_Positions_Short_All',
 'Other_Rept_Positions_Spread_All',
 'Tot_Rept_Positions_Long_All',
 'Tot_Rept_Positions_Short_All',
 'NonRept_Positions_Long_All',
 'NonRept_Positions_Short_All',
 'Change_in_Open_Interest_All',
 'Change_in_Dealer_Long_All',
 'Change_in_Dealer_Short_All',
 'Change_in_Dealer_Spread_All',
 'Change_in_Asset_Mgr_Long_All',
 'Change_in_Asset_Mgr_Short_All',
 'Change_in_Asset_Mgr_Spread_All',
 'Change_in_Lev_Money_Long_All',
 'Change_in_Lev_Money_Short_All',
 'Change_in_Lev_Money_Spread_All',
 'Change_in_Other_Rept_Long_All',
 'Change_in_Other_Rept_Short_All',
 'Change_in_Other_Rept_Spread_All',
 'Change_in_Tot_Rept_Long_All',
 'Change_in_Tot_Rept_Short_All',
 'Change_in_NonRept_Long_All',
 'Change_in_NonRept_Short_All',
 'Traders_Tot_All',
 'Traders_Dealer_Long_All',
 'Traders_Dealer_Short_All',
 'Traders_Dealer_Spread_All',
 'Traders_Asset_Mgr_Long_All',
 'Traders_Asset_Mgr_Short_All',
 'Traders_Asset_Mgr_Spread_All',
 'Traders_Lev_Money_Long_All',
 'Traders_Lev_Money_Short_All',
 'Traders_Lev_Money_Spread_All',
 'Traders_Other_Rept_Long_All',
 'Traders_Other_Rept_Short_All',
 'Traders_Other_Rept_Spread_All',
 'Traders_Tot_Rept_Long_All',
 'Traders_Tot_Rept_Short_All'
]

cot_select = cot_panda_sp.select(required_cols)

# Filter the latest data only that was released on Tuesday of that week
report_date = datetime.today() - timedelta(days = 4)
report_date_string = f"{report_date.year}-{report_date.month}-{report_date.day}"
cot_select_update = cot_select.filter(cot_select['Report_Date'] >= report_date_string) 

# Writing the resulting dataframe as BigQuery table
cot_select_update.write \
    .format('bigquery') \
    .option('project', PROJECT_ID) \
    .option('parentProject', PROJECT_ID) \
    .option('table', 'committment_of_traders.cot') \
    .option("temporaryGcsBucket",f"{TEMP_BUCKET}") \
    .mode('append') \
    .save()
