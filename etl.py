#!/opt/conda/bin/python
"""
 Setup Spark session to read and combine SAS files and other datasets. 
 Then, write them out to parquet formats to S3
"""

# Import modules to used
from pyspark.sql import SparkSession
import pandas as pd
import config as c
from os import listdir
from os import system
from process_jun_fl import clean_jun_df
from process_dm_datasets import process_dms
from pyspark.sql.types import StructType, StructField, StringType, \
FloatType, IntegerType, DateType
from pyspark.sql.functions import col
from uploadtoS3 import upload
from del_stg_fls import delstgfls


# Setup Spark session
def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark



def process_sas_fls(spark, c):
    # Load all i94 native SAS files into Spark, write out more efficent 
    # compressed parquet snappy format
    
    # Initialize dir, file and config variables
    dir = '../../data/18-83510-I94-Data-2016/'
    
    # Toggle below lines for small size unit tests
    #files = ['i94_jan16_sub.sas7bdat', 'i94_jun16_sub.sas7bdat']
    files = listdir(dir)
    
    # Check if I94_SAS_EXTRACTS directory exists and delete it
    print('Removing Dir ',c.I94_SAS_EXTRACTS, end='')
    system('test -d '+c.I94_SAS_EXTRACTS+' && rm -rf '+c.I94_SAS_EXTRACTS)
    print(' -> Done')
    
    # Read in all i94 native SAS files into Spark dataframe df_ft_i94
    for f in files:
        # Adding additional section for additional steps to be taken for June File
        # Found issue while load on this. Issue with additional columns.
        # Execute an additional function to clean up jun
        if (f[4:7] == 'jun'):
            clean_jun_df(spark, listdir, system, c)
        else:          
            print(f'Reading: {c.I94_SAS_EXTRACTS+f}',end='')
            df_ft_i94=spark.read.format('com.github.saurfang.sas.spark').load(dir+f)
            print(' --> Done')
            
            # Check number of columns in file
            print(f'{f} columns count: ', len(df_ft_i94.columns))
            
            print(f'Dropping columns with high Null count: occup, entdepu, insnum', end='')
            df_ft_i94 = df_ft_i94.drop('occup').drop('entdepu').drop('insnum')
            print(' --> Done')
            
            # Check number of columns in file
            print(f'{f} columns count: ', len(df_ft_i94.columns))
    
            print(f'Writing: {c.I94_SAS_EXTRACTS}{f[0:9]}',end='')
            df_ft_i94.write.parquet(c.I94_SAS_EXTRACTS+f[0:9])
            print(' --> Done')        

       
    return spark, df_ft_i94


def combine_i94_datasets(spark, c, df_ft_i94):
    # List out all files in i94 folder
    dir = c.I94_SAS_EXTRACTS
    files = listdir(dir)
    
    # Do data validations, check row counts while processing and combining datasets:
    if '.ipynb_checkpoints' in files:
        files.remove('.ipynb_checkpoints')
    print(files)
    i = 1
    for f in files:
        if i:
            print(f'Reading: {f}', end='')
            df_ft_i94=spark.read.parquet(dir+f)
            print(f'\t Record count: ',df_ft_i94.count())
            i = 0
        else:
            print(f'Reading: {f}', end='')
            
            # Combining dataframe with all SAS files load
            df_ft_i94 = df_ft_i94.union(spark.read.parquet(dir+f))
            
            # Doing data validations with row count
            print(f'\t Record count: ',df_ft_i94.count())
    else:
        print('Final df_ft_i94 count: ',df_ft_i94.count())
    
    # Check schema
    df_ft_i94.printSchema()
    return df_ft_i94


def empty_stage(c):
    # Delete Current Staging directory
    print('Removing Dir ',c.S3_STAGE, end='')
    system('test -d '+c.S3_STAGE+' && rm -rf '+c.S3_STAGE)
    print(' -> Done')
    pass


def stage_ft_i94(spark, df_ft_i94, c):
    # Write out these combined clean Fact Table: df_ft_i94 data to file:
    print(f'Writing to {c.S3_STAGE}', end='')
    df_ft_i94.write.parquet(c.S3_STAGE+'ft_i94')
    print(' -> Done')
    pass



def main():
    spark = create_spark_session()
    
    spark, df_ft_i94 = process_sas_fls(spark, c)
    
    df_ft_i94 = combine_i94_datasets(spark, c, df_ft_i94)
    
    empty_stage(c)
    
    stage_ft_i94(spark, df_ft_i94, c)
    
    # Process rest of all Dimension Table source files and write to parquet files:
    process_dms(spark, pd, StructType, StructField, StringType, FloatType, 
    IntegerType, DateType, col, c)
    
    #Upload to S3
    upload(localfs=c.S3_STAGE, bucket=c.I94_TABLE_FLS)

    #Notify to check files and validation
    print(f'Please check further in:  \
          \n 1. S3 for files in bucket: {c.I94_TABLE_FLS} \
          \n 2. EMR Cluster Step Logs for Steps run logs, including validations run \
          \n 3. EMR notebook: \'Analytics\' ')
    
    resp = input('\n\nOnce validated, please confirm delete local stage folders? (y/n): ')
    if(resp=='y'):
        delstgfls()
    else:
        print('Okay, we\'ll leave files as is.')
        
    
if __name__ == "__main__":
    main()

