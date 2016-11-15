from __future__ import print_function

"""
Taken from bundle.py in SkillsParser.
"""

import argparse

from deployment_lib.lambda_bundler import (
    copy_deployment_files,
    copy_virtual_env_libs,
    make_deployment_dir,
    zipdir,
    copy_deployment_into_latest,
    deploy,
    LATEST_DEPLOYMENT_FILE_PATH,
    delete_directory)


DEPLOYMENT_FILES = [
    'main.py',
    'estimator.py',
    'sframes',
    'CA-2016-11-09.comparable.csv',
    'CA-2016-11-09.request.csv'
]

ANACONDA_VENV_PATH = "venv"

parser = argparse.ArgumentParser(description='Create an AWS Lambda Deployment for resume parsing')
parser.add_argument('--create_deployment', action='store_true')
parser.add_argument('--deploy', action='store_true')
args = parser.parse_args()

if args.create_deployment:
    print('Creating deployment')
    (NEW_DEPLOYMENT_DIR, CURRENT_DEPLOYMENT_NAME) = make_deployment_dir()

    print("Copying deployment files")
    copy_deployment_files(NEW_DEPLOYMENT_DIR, DEPLOYMENT_FILES)
    copy_virtual_env_libs(NEW_DEPLOYMENT_DIR, ANACONDA_VENV_PATH)
    new_deployment_zipfile_name = "deployments/{}.zip".format(CURRENT_DEPLOYMENT_NAME)
    zipdir(NEW_DEPLOYMENT_DIR, new_deployment_zipfile_name)
    copy_deployment_into_latest(new_deployment_zipfile_name)

    print('Successfully created new deployment at {0:s} and {1:s}'.format(new_deployment_zipfile_name,
                                                                          LATEST_DEPLOYMENT_FILE_PATH))

    print("Deleting deployment directory to clean up")
    delete_directory(NEW_DEPLOYMENT_DIR)

elif args.deploy:
    LAMBDA_FUNCTION_NAMES = ['estimateDeliveryCost']
    deploy(LAMBDA_FUNCTION_NAMES)