#!/opt/conda/bin/python

"""
 Read all other datasets and write to S3 staging as parquet
"""

def process_dms(spark, pd, StructType, StructField, StringType, FloatType, 
IntegerType, DateType, col, c):
    ############################################
    # US Cities Demographics (dm_CityDemoGraph)
    ############################################
    # Read in US Cities Demographics
    df_CityDemoGraph = pd.read_csv('us-cities-demographics.csv', sep=';')

    # See Current Columns
    print('Columns: ', df_CityDemoGraph.columns.values)

    # Check shape of data in df
    print('\nShape:', df_CityDemoGraph.shape)

    # Set new Schema, specifying datatypes and appropriate column names
    schema = StructType([StructField("City", StringType(), True),
                         StructField("State", StringType(), True),
                         StructField("MedianAge", FloatType(), True),
                         StructField("Male", FloatType(), True),
                         StructField("Female", FloatType(), True),
                         StructField("TotalPop", IntegerType(), True),
                         StructField("Veterans", FloatType(), True),
                         StructField("Foreign_born", FloatType(), True),
                         StructField("Avg_Hshld_Size", FloatType(), True),
                         StructField("St_cd", StringType(), True),
                         StructField("Race", StringType(), True),
                         StructField("Count", IntegerType(), True)])

    # Convert to Spark Dataframe
    df_dm_CityDemoGraph = spark.createDataFrame(df_CityDemoGraph, schema)

    # See New Schema
    print(df_dm_CityDemoGraph.printSchema())

    print(df_CityDemoGraph.head(2))

    # Write out Parquet format to S3:
    df_dm_CityDemoGraph.write.parquet(c.S3_STAGE+'dm_CityDemoGraph')

    ############################################
    # US Airport Codes (dm_Airport)
    ############################################
    # Read in Airport Codes file
    df_Airport = pd.read_csv('airport-codes_csv.csv')

    # See Current Columns
    print('\n', 'Columns: ', df_Airport.columns.values)

    # Check shape of data in df
    print('\n', 'Shape: ', df_Airport.shape)

    # Set schema, specifying datatypes and appropriate column names
    schema = StructType([StructField("ident", StringType(), True),
                         StructField("type", StringType(), True),
                         StructField("name", StringType(), True),
                         StructField("elevation_ft", FloatType(), True),
                         StructField("continent", StringType(), True),
                         StructField("iso_country", StringType(), True),
                         StructField("iso_region", StringType(), True),
                         StructField("municipality", StringType(), True),
                         StructField("gps_code", StringType(), True),
                         StructField("iata_code", StringType(), True),
                         StructField("local_code", StringType(), True),
                         StructField("coordinates", StringType(), True)])

    # Convert to Spark Dataframe
    df_dm_Airport = spark.createDataFrame(df_Airport, schema=schema)

    # Getting error here, 'field gps_code: Can not merge type <class
    # 'pyspark.sql.types.StringType'> and <class 'pyspark.sql.types.DoubleType'>'
    # Need to check on this a little more, will come back to this later, perhaps
    # need to split into two columns. Coord1, Coord2 Issue fixed, incorrect df got
    # passed, with incorrect schema. Fixed

    # See head sample of rows
    print('\n', 'Head:', df_Airport.head(2), '\n')

    # Write out Parquet format to S3:
    df_dm_Airport.write.parquet(c.S3_STAGE+'dm_Airport')

    ############################################
    # Airline data (dm_airline_name)
    ############################################

    # Schema
    schema = StructType([StructField("_c0", IntegerType(), True), 
                         StructField("_c1", StringType(), True), 
                         StructField("_c2", StringType(), True), 
                         StructField("_c3", StringType(), True), 
                         StructField("_c4", StringType(), True), 
                         StructField("_c5", StringType(), True), 
                         StructField("_c6", StringType(), True), 
                         StructField("_c7", StringType(), True)])

    # Read airlines data files:
    df_dm_airline_name = spark.read.csv('airlines.dat', schema=schema)

    # Renaming Columns with appropriate headers found on
    # openflights.org/data.html
    df_dm_airline_name = df_dm_airline_name.withColumnRenamed("_c0", "id") \
        .withColumnRenamed("_c1", "name") \
        .withColumnRenamed("_c2", "Alias") \
        .withColumnRenamed("_c3", "IATA") \
        .withColumnRenamed("_c4", "ICAO") \
        .withColumnRenamed("_c5", "Callsign") \
        .withColumnRenamed("_c6", "Country") \
        .withColumnRenamed("_c7", "Active")
        
    
    # See head sample of rows
    print('\n', 'Head:', df_dm_airline_name.head(2), '\n')   

    # Selecting only revelant columns out of these:
    # And filter out 'null' and '-' from IATA        
    # See duplicates on Lufthansa Cargo and Japan Airlines Do
    # Filtering them out, leaving only 1 entry each
    # Added to df filters on dm_airline_name below and added back to dataframe
    df_dm_airline_name = df_dm_airline_name.withColumnRenamed("name","carrier") \
                            .withColumnRenamed("iata", "code") \
                            .filter(col("iata") != "null") \
                            .filter(col("active") == 'Y') \
                            .filter("name NOT LIKE 'Lufthansa Cargo'") \
                            .filter("carrier NOT LIKE 'Japan Airlines Do%'") \
                            .dropDuplicates()
                
    # Write out Parquet format to S3:
    df_dm_airline_name.createOrReplaceTempView('dm_airline_name')
    
    # Checking out values that are those duplicates removed:
    print('Verifying duplicates for Lufthansa and Japan Airlines not present any more:')
    spark.sql("""
        SELECT carrier, code, country, callsign, icao
        FROM dm_airline_name
        WHERE code IN (SELECT code
                        FROM dm_airline_name
                        GROUP BY code
                        HAVING COUNT(code) > 1
                        ORDER BY 1 DESC
        )
        ORDER BY 2
    """).show(100)
    
    # See head sample of rows
    print('\n', 'Head:', df_dm_airline_name.head(2), '\n')

    # Write out Parquet format to S3:
    df_dm_airline_name.write.parquet(c.S3_STAGE+'dm_airline_name')

    ############################################
    # Weather Temps (dm_Weather)
    ############################################
    # Going to use PySpark directly to read this file. More than 8 million records here
    #dm_Weather = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')

    # Read Weather Temps csv with Pyspark, header provided
    df_dm_Weather = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv',
                                   header=True)

    # See Current Columns
    print('Columns: ', df_dm_Weather.columns, '\n')

    # See head sample of rows
    print('\n', df_dm_Weather.show(7), '\n')

    # Current Schema
    print('\n', 'Current Schema: ', df_dm_Weather.printSchema())

    # Schema needs to be modified to show correct representation
    schema = StructType([StructField("dt", DateType(), True),
                         StructField("AverageTemperature", FloatType(), True),
                         StructField("AverageTemperatureUncertainty", FloatType(), True),
                         StructField("City", StringType(), True),
                         StructField("Country", StringType(), True),
                         StructField("Latitude", StringType(), True),
                         StructField("Longitude", StringType(), True)])
    
    # Re-read file with new schema
    df_dm_Weather = spark.read.csv(
        '../../data2/GlobalLandTemperaturesByCity.csv', header=True, schema=schema)

    # See updated Schema
    print('\n', 'Updated Schema:', df_dm_Weather.printSchema())
    
    # As we are only dealing with US data, removing other countries
    # Added to df filters on dm_Weather to see only United States info and added back to dataframe
    # Faced an issue with EMR: 'UnicodeEncodeError: 'ascii' codec can't encode character u'\xc5''
    # while reading dataframe, didn't see this in unit test environment. Removing other countries addressed issue
    df_dm_Weather = df_dm_Weather.filter(col("Country") == "United States")\
                            .dropDuplicates()

    # See head sample of rows after update
    df_dm_Weather.show(7)

    # Write out Parquet format to S3:
    df_dm_Weather.write.parquet(c.S3_STAGE+'dm_Weather')

    ############################################
    ### I94ADDR (dm_AddrState)
    ############################################
    filepath = 'I94ADDR.json'
    df_dm_AddrState = pd.read_json(filepath)

    # See head sample of rows
    print('\n', 'Head: ', df_dm_AddrState.head())

    # See Dtypes
    print('\n', 'Dtypes: ', df_dm_AddrState.dtypes, '\n')

    # Specify schema
    schema = StructType([StructField("Statecode", StringType(), True),
                         StructField("Statename", StringType(), True)])

    # Convert to Spark Dataframe Getting error on datatypes, need to check, found
    # some issue with String and Longdata being detected for Statecode, provided
    # Schema with String, fixed
    df_dm_AddrState = spark.createDataFrame(df_dm_AddrState, schema=schema)

    # See head sample of rows
    df_dm_AddrState.show(3)

    # Write out Parquet format to S3:
    df_dm_AddrState.write.parquet(c.S3_STAGE+'dm_AddrState')

    ############################################
    ### I94CIT (dm_CountryofEntry)
    ############################################
    # Read file
    filepath = 'I94CIT.json'
    df_dm_CountryofEntry = pd.read_json(filepath)

    # See head sample of rows
    print(df_dm_CountryofEntry.head())

    # Convert to Spark Dataframe
    df_dm_CountryofEntry = spark.createDataFrame(df_dm_CountryofEntry)

    # See head sample of rows
    df_dm_CountryofEntry.show(5)

    # Write out Parquet format to S3:
    df_dm_CountryofEntry.write.parquet(c.S3_STAGE+'dm_CountryofEntry')

    ############################################
    ###  I94PORT (dm_PortofEntry)
    ############################################
    # Read file
    filepath = 'I94PORT.json'
    df_dm_PortofEntry = pd.read_json(filepath)

    # See head sample of rows
    print(df_dm_PortofEntry.head(3))

    # See shape
    print('\nShape:', df_dm_PortofEntry.shape)

    # Set schema
    schema = StructType([StructField("City", StringType(), True),
                         StructField("Code", StringType(), True),
                         StructField("State", StringType(), True)])

    # Convert to Spark Dataframe Getting error on datatypes, need to check. Same
    # issue with Long/String Type conversion. Provided Schema, fixed
    df_dm_PortofEntry = spark.createDataFrame(df_dm_PortofEntry, schema=schema)

    # Check Schema
    df_dm_PortofEntry.printSchema()

    # See head sample of rows
    df_dm_PortofEntry.show(3)

    # Write out Parquet format to S3:
    df_dm_PortofEntry.write.parquet(c.S3_STAGE+'dm_PortofEntry')

    ############################################
    ### I94PURPOSE (dm_Purpose)
    ############################################
    # Read file
    filepath = 'I94PURPOSE.json'
    df_dm_Purpose = pd.read_json(filepath)

    # See head sample of rows
    print(df_dm_Purpose.head())

    # Convert to Spark Dataframe
    df_dm_Purpose = spark.createDataFrame(df_dm_Purpose)

    # Check schema
    df_dm_Purpose.printSchema()

    # See head sample of rows
    df_dm_Purpose.show(3)

    # Write out Parquet format to S3:
    df_dm_Purpose.write.parquet(c.S3_STAGE+'dm_Purpose')

    ############################################
    ### I94MODE (dm_ModeofEntry)
    ############################################
    # Read file
    filepath = 'I94MODE.json'
    df_dm_ModeofEntry = pd.read_json(filepath)

    # See head sample of rows
    df_dm_ModeofEntry.head()

    # Convert to Spark Dataframe
    df_dm_ModeofEntry = spark.createDataFrame(df_dm_ModeofEntry)

    # Check schema
    df_dm_ModeofEntry.printSchema()

    # See head sample of rows
    df_dm_ModeofEntry.show(4)

    # Write out Parquet format to S3:
    df_dm_ModeofEntry.write.parquet(c.S3_STAGE+'dm_ModeofEntry')
