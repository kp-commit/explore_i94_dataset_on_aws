#!/opt/conda/bin/python
"""
Upload files to S3
"""

#f = "i94_sample"
localfs = 's3_staging/dm_airline_name/'
bucket = "s3://kbucket-udfls/i94_table_fls/dm_airline_name/"

import config as c
from os import system

command = 'aws s3 cp '+localfs+' '+bucket+' --recursive'
print(f'Executing: {command}\n')

print('Setting AWS Access and uploading to S3\n')

system('export AWS_ACCESS_KEY_ID='+c.AWS_ACCESS_KEY_ID+
       ' && export AWS_SECRET_ACCESS_KEY='+c.AWS_SECRET_ACCESS_KEY+
       ' && export AWS_DEFAULT_REGION='+c.REGION_NAME+
       ' && aws s3 cp '+localfs+' '+bucket+' --recursive')
print('\nFiles upload to S3 -> Done')