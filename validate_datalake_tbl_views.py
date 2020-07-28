# -*- coding: utf-8 -*-
"""
Load data from S3 into EMR and validate data
"""

# Import Spark modules
from pyspark.sql import SparkSession

# Setup Spark session
spark = SparkSession.builder.appName(
    "Validate_datalake_tbl_views").enableHiveSupport().getOrCreate()


# Read table files and load dataframes
S3_STAGE = 's3_staging/'

##########################################################################
# For table read location, toggle below line with line after to change
# from local filesystem to AWS S3
##########################################################################
#I94_TABLE_FLS = S3_STAGE
I94_TABLE_FLS = 's3://kbucket-udfls/i94_table_fls/'
#DQ = I94_TABLE_FLS + 'validation_checks/'


# Read parquet files:
df_ft_i94 = spark.read.parquet(I94_TABLE_FLS + 'ft_i94')
df_dm_AddrState = spark.read.parquet(I94_TABLE_FLS + 'dm_AddrState')
df_dm_CountryofEntry = spark.read.parquet(I94_TABLE_FLS + 'dm_CountryofEntry')
df_dm_PortofEntry = spark.read.parquet(I94_TABLE_FLS + 'dm_PortofEntry')
df_dm_Purpose = spark.read.parquet(I94_TABLE_FLS + 'dm_Purpose')
df_dm_ModeofEntry = spark.read.parquet(I94_TABLE_FLS + 'dm_ModeofEntry')
df_dm_CityDemoGraph = spark.read.parquet(I94_TABLE_FLS + 'dm_CityDemoGraph')
df_dm_Airport = spark.read.parquet(I94_TABLE_FLS + 'dm_Airport')
df_dm_airline_name = spark.read.parquet(I94_TABLE_FLS + 'dm_airline_name')
df_dm_Weather = spark.read.parquet(I94_TABLE_FLS + 'dm_Weather')


#################################################
# Fact Table in S3: df_ft_i94 (EMR view: ft_i94)
#################################################

# Create a temp view for ft_i94 and exploring it
df_ft_i94.createOrReplaceTempView('ft_i94')

# Verify of count of dataframe count
print('--------')
print('ft_i94')
print('--------')
print('Count of df_ft_i94: ', df_ft_i94.count())

# Verify of count of table view
print('Count of view ft_i94:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM ft_i94
""")
tbl_count.show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM ft_i94
    LIMIT 5
""")
row_sample.show(5)

#dfc = str(df_ft_i94.count())
#tc = str(tbl_count)
#rc = str(row_sample)

# Write validation results to S3
# validation = ['Count of df_ft_i94: '+dfc+'\n',
#            'Count of view ft_i94:'+tc+'\n',
#            'Sample rows:\n'+ rc
#
#validation = spark.createDataFrame(validation)
#
#validation.write.mode("overwrite").format("text").option(header="true") \
#.save(DQ+"ft_i94.txt")
#row_sample.write.mode("append").format("text").option(header="true") \
#.save(DQ+"ft_i94.txt")


###############################################################
# Dim Table in S3: df_dm_AddrState (EMR view: dm_AddrState)
###############################################################

# Create a temp view for dm_AddrState and exploring it
df_dm_AddrState.createOrReplaceTempView('dm_AddrState')


# Verify of count of dataframe count
print('--------------')
print('dm_AddrState')
print('--------------')
print('Count of df_dm_AddrState: ', df_dm_AddrState.count())


# Verify of count of table view
print('Count of view dm_AddrState:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_AddrState
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_AddrState
""").show(5)


########################################################################
# Dim Table in S3: df_dm_CountryofEntry (EMR view: dm_CountryofEntry)
########################################################################

# Create a temp view for dm_CountryofEntry and exploring it
df_dm_CountryofEntry.createOrReplaceTempView('dm_CountryofEntry')

# See a sample of dataset
print('------------------')
print('dm_CountryofEntry')
print('------------------')
print('Count of df_dm_CountryofEntry: ', df_dm_CountryofEntry.count())

# Verify of count of table view
print('Count of dm_CountryofEntry:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_CountryofEntry
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_CountryofEntry
""").show(5)


########################################################################
# Dim Table in S3: df_dm_PortofEntry (EMR view: dm_PortofEntry)
########################################################################

# Create a temp view for dm_PortofEntry and exploring it
df_dm_PortofEntry.createOrReplaceTempView('dm_PortofEntry')

# See a sample of dataset
print('------------------')
print('dm_PortofEntry')
print('------------------')
print('Count of df_dm_PortofEntry: ', df_dm_PortofEntry.count())

# Verify of count of table view
print('Count of dm_PortofEntry:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_PortofEntry
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_PortofEntry
""").show(5)


########################################################################
# Dim Table in S3: df_dm_Purpose (EMR view: dm_Purpose)
########################################################################

# Create a temp view for dm_Purpose and exploring it
df_dm_Purpose.createOrReplaceTempView('dm_Purpose')

# See a sample of dataset
print('-----------')
print('dm_Purpose')
print('-----------')
print('Count of df_dm_Purpose: ', df_dm_Purpose.count())

# Verify of count of table view
print('Count of dm_Purpose:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_Purpose
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_Purpose
""").show(5)


#
########################################################################
# Dim Table in S3: df_dm_ModeofEntry (EMR view: dm_ModeofEntry)
########################################################################

# Create a temp view for dm_ModeofEntry and exploring it
df_dm_ModeofEntry.createOrReplaceTempView('dm_ModeofEntry')

# See a sample of dataset
print('--------------')
print('dm_ModeofEntry')
print('--------------')
print('Count of df_dm_ModeofEntry: ', df_dm_ModeofEntry.count())

# Verify of count of table view
print('Count of dm_ModeofEntry:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_ModeofEntry
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_ModeofEntry
""").show(5)


########################################################################
# Dim Table in S3: df_dm_CityDemoGraph (EMR view: dm_CityDemoGraph)
########################################################################

# Create a temp view for dm_CityDemoGraph and exploring it
df_dm_CityDemoGraph.createOrReplaceTempView('dm_CityDemoGraph')

# See a sample of dataset
print('----------------')
print('dm_CityDemoGraph')
print('----------------')
print('Count of df_dm_CityDemoGraph: ', df_dm_CityDemoGraph.count())

# Verify of count of table view
print('Count of dm_CityDemoGraph:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_CityDemoGraph
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_CityDemoGraph
""").show(5)


########################################################################
# Dim Table in S3: df_dm_Airport (EMR view: dm_Airport)
########################################################################

# Create a temp view for dm_Airport and exploring it
df_dm_Airport.createOrReplaceTempView('dm_Airport')

# See a sample of dataset
print('-----------')
print('dm_Airport')
print('-----------')
print('Count of df_dm_Airport: ', df_dm_Airport.count())

# Verify of count of table view
print('Count of dm_Airport:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_Airport
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_Airport
""").show(5)


########################################################################
# Dim Table in S3: df_dm_airline_name (EMR view: dm_airline_name)
########################################################################

# Create a temp view for dm_airline_name and exploring it
df_dm_airline_name.createOrReplaceTempView('dm_airline_name')


# See a sample of dataset
print('----------------')
print('dm_airline_name')
print('----------------')
print('Count of df_dm_airline_name: ', df_dm_airline_name.count())

# Verify of count of table view
print('Count of dm_airline_name:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_airline_name
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_airline_name
""").show(5)


########################################################################
# Dim Table in S3: df_dm_Weather (EMR view: dm_Weather)
########################################################################

# Create a temp view for df_dm_Weather and exploring it
df_dm_Weather.createOrReplaceTempView('dm_Weather')


# See a sample of dataset
print('----------------')
print('dm_Weather')
print('----------------')
print('Count of df_dm_Weather: ', df_dm_Weather.count())


# Verify of count of table view
print('Count of dm_Weather:')
tbl_count = spark.sql("""
    SELECT COUNT(*) Rows
    FROM dm_Weather
""").show()

# See a sample of dataset
print('Sample rows:')
row_sample = spark.sql("""
    SELECT *
    FROM dm_Weather
    LIMIT 5
""").show()
