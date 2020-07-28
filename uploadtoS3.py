#!/opt/conda/bin/python
"""
Upload files to S3
"""

def upload(localfs, bucket):
    #f = "i94_sample"
    #localfs = 's3_test_files'
    #bucket = "s3://kbucket-udfls/i94dl-sparkfls/"
    
    import config as c
    from os import system
    
    print('Setting AWS Access and uploading to S3\n')
    
    command = 'aws s3 cp '+localfs+' '+bucket+' --recursive'
    print(f'Executing: {command}\n')
    
    system('export AWS_ACCESS_KEY_ID='+c.AWS_ACCESS_KEY_ID+
           ' && export AWS_SECRET_ACCESS_KEY='+c.AWS_SECRET_ACCESS_KEY+
           ' && export AWS_DEFAULT_REGION='+c.REGION_NAME+
           ' && aws s3 cp '+localfs+' '+bucket+' --recursive')
    print('\nFiles upload to S3 -> Done')