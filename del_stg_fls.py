#!/opt/conda/bin/python
"""
Delete temp staging folders created on local filesystem
"""
import config as c
from os import system

def delstgfls():
    print('Deleting staging files', end='')    
    system('rm -R '+c.S3_STAGE+' && rm -R '+c.I94_SAS_EXTRACTS)
    print(' -> Done')
    pass