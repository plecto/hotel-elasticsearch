import os

import boto3
import botocore
import requests


def boto3_resource_factory(service: str, region_name: str = None):
    if region_name is None:
        region_name = _get_region()
    return boto3.resource(service, region_name=region_name)


def botocore_client_factory(service, region_name: str = None):
    if region_name is None:
        region_name = _get_region()
    return botocore.session.get_session().create_client(service, region_name=region_name)

def _get_region():
    region_name = os.environ.get('AWS_DEFAULT_REGION', None)
    if region_name is None:
        region_name = os.environ.get('EC2_REGION', None)
    if region_name is None:
        response = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=1)
        region_name = response.json().get('region', None)
    if region_name is None:
        raise ValueError('Could not determine region name')

    return region_name