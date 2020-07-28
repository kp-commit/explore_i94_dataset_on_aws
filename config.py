# AWS Info
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
REGION_NAME='us-west-2'
ACCOUNT_NUM=''

# Local fs dir to store SAS extract files
# and stage S3 files before upload
S3_STAGE = 's3_staging/'
I94_SAS_EXTRACTS = 'i94_sas_extracts/'


#Instance configs, get these from your AWS account
CLUSTER_NAME='i94_travel'
EC2KEYNAME=''
MASTERINSTANCETYPE='m5.xlarge'
SLAVEINSTANCETYPE='m5.xlarge'
INSTANCECOUNT=4

# Security Groups (get from AWS account and fill this out 
# these lines in lanuch_cluster.py to use with notebook)
EC2SUBNETID=''
EMRMANAGEDMASTERSECURITYGROUP=''
EMRMANAGEDSLAVESECURITYGROUP=''


# Buckets
#I94_TABLE_FLS = S3_STAGE
I94_TABLE_FLS='s3://<tables_bucket>/i94_table_fls/'
SPARK_SCRIPTS='s3://<spark_scripts_bucket>/'
