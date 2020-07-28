#!/opt/conda/bin/python
"""
 To lanuch create EMR cluster, includes specifications for EC2 instances, job execution
"""
import boto3
import config as c

emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)


print('CREATING CLUSTER:')
response = emr.run_job_flow(
    Name=c.CLUSTER_NAME,
    LogUri='s3://aws-logs-'+ c.ACCOUNT_NUM + '-' + c.REGION_NAME + '/elasticmapreduce/',
    ReleaseLabel='emr-5.29.0',
    Instances={
        'MasterInstanceType': c.MASTERINSTANCETYPE,
        'SlaveInstanceType': c.SLAVEINSTANCETYPE,
        'InstanceCount': c.INSTANCECOUNT,
        'Ec2KeyName': c.EC2KEYNAME,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'HadoopVersion': '2.8.5',
        
        # For using EMR notebooks within same after lanuch
        # Fill these out in config.py
        # If not, these 3 lines below can be commented out for job execution
        'Ec2SubnetId': c.EC2SUBNETID,
        'EmrManagedMasterSecurityGroup': c.EMRMANAGEDMASTERSECURITYGROUP,
        'EmrManagedSlaveSecurityGroup': c.EMRMANAGEDSLAVESECURITYGROUP
    },
    Steps=[
        {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'state-pusher-script'
                ]
            }
        },
        {
            'Name': 'copy spark script files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'aws', 's3', 'cp', c.SPARK_SCRIPTS, '/home/hadoop/', '--recursive'
                ]
            }
        },
        {
            'Name': 'Install Py3 Numpy',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
# Faced an issue with EMR with matplotlib, numpy==1.15 specifically was being asked for
# even though higher ver as already present, so set as below. Graphs plotting now.
                'Args': [
                    'sudo', 'python3', '-m', 'pip', 'install', 'numpy==1.15'
                ]
            }
        },
        {
            'Name': 'Install Py3 Pandas',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'sudo', 'python3', '-m', 'pip', 'install', 'pandas'
                ]
            }
        },
        {
            'Name': 'Install Py3 Matplotlib',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'sudo', 'python3', '-m', 'pip', 'install', 'matplotlib'
                ]
            }
        },
        {
            'Name': 'Create i94 tables views and validating datalake',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/validate_datalake_tbl_views.py'
                ]
            }
        }
    ],
    Applications=[
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Hive'
        },
        {
            'Name': 'Spark'
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)
print('EMR cluster start initiated.')
print(f'Creating table views from S3 files at {c.I94_TABLE_FLS}')
print(f'Data validation scheduled')