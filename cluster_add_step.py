#!/opt/conda/bin/python
import boto3
import config as c

emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)

# Add steps here:
ListofSteps=[
#       {
#            'Name': 'Create_i94_Datalake',
#            'ActionOnFailure': 'TERMINATE_CLUSTER',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'spark-submit', '/home/hadoop/etl.py'
#                ]
#            }
#        },
#        {
#            'Name': 'New Step Name',
#            'ActionOnFailure': 'CONTINUE',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'argForNewStep'
#                ]
#            }
#        }
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
            'Name': 'Create i94 tables views and Validate',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/validate_datalake_tbl_views.py'
                ]
            }
        }
#        {
#            'Name': 'Install Py3 Numpy',
#            'ActionOnFailure': 'CONTINUE',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
## Faced an issue with EMR with matplotlib, numpy==1.15 specifically was being asked for
## even though higher ver as already present, so set as below. Graphs plotting now.
#                'Args': [
#                    'sudo', 'python3', '-m', 'pip', 'install', 'numpy==1.15'
#                ]
#            }
#        },
#        {
#            'Name': 'Install Py3 Pandas',
#            'ActionOnFailure': 'CONTINUE',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'sudo', 'python3', '-m', 'pip', 'install', 'pandas'
#                ]
#            }
#        },
#        {
#            'Name': 'Install Py3 Matplotlib',
#            'ActionOnFailure': 'CONTINUE',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': [
#                    'sudo', 'python3', '-m', 'pip', 'install', 'matplotlib'
#                ]
#            }
#        }

    ]

# get cluster id:
job_flow_id = input('Enter Cluster id j-: ')
print("Job flow ID:", job_flow_id)

# Add aditional steps
step_response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=ListofSteps)
step_ids = step_response['StepIds']

print("Added Step IDs:", step_ids)