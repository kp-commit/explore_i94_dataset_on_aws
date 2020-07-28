#!/opt/conda/bin/python
"""
# Correct issue with SAS dataset
# Issue identified as number of columns marked to be deleted but not delete in Jun file
# Reload after dropping additional columns
"""

def clean_jun_df(spark, listdir, system, c):
    
    # Load June file with issues
    df_ft_i94_jun=spark.read.format('com.github.saurfang.sas.spark')\
    .load('../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat')
    
    
    # Check Jun file schema 
    jun_cols = df_ft_i94_jun.columns
    print('Jun_cols: ', jun_cols)
    
    # Check number of columns in June file
    print('June columns: ', len(df_ft_i94_jun.columns))

    
    
    
    # Read another file that load properly: Jan file
    df_ft_i94_jan=spark.read.format('com.github.saurfang.sas.spark')\
    .load('../../data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat')
    
    ### Drop extra columns with high null counts
    df_ft_i94_jan = df_ft_i94_jan.drop('occup').drop('entdepu').drop('insnum')
    
    # Check number of columns in Jan file
    jan_cols = df_ft_i94_jan.columns
    print('Jan_cols: ',jan_cols)
    
    # Check number of columns in Jan file
    print('Jan columns:', len(df_ft_i94_jan.columns))
    
    # Get Set difference (additional columns) between Jan and Jun files:
    diff = list(set(jun_cols)-set(jan_cols))
    print('\nDiffernce of columns:', diff,'\n')
    
    
    # Remove extra columns from Jun dataset and write to parquet
    
    #Load Jun file
    df_ft_i94_jun=spark.read.format('com.github.saurfang.sas.spark')\
    .load('../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat')
    
    # Drop difference (extra) of columns
    df_ft_i94_jun=df_ft_i94_jun.drop(*diff)
    
    # Check Jun file schema 
    jun_cols = df_ft_i94_jun.columns
    print('Jun_cols: ', jun_cols)
    
    # Check number of columns in June file
    print('June columns: ', len(df_ft_i94_jun.columns)) 
     
    
    # Delete i94_jun folder - run os command below to remove dir
    #print('Removing: '+c.I94_SAS_EXTRACTS+'i94_jun16', end='')
    #system('test -f '+c.I94_SAS_EXTRACTS+'i94_jun16 && rm -rf '+c.I94_SAS_EXTRACTS+'i94_jun16')
    #print(' --> Done')
    
    # Write cleaned Jun data file:
    print('Writing cleaned dataset:'+c.I94_SAS_EXTRACTS+'i94_jun16', end='')
    df_ft_i94_jun.write.parquet(c.I94_SAS_EXTRACTS+'i94_jun16')
    print(' --> Done')
