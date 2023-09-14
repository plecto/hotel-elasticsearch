import logging

import requests
import botocore.exceptions
from frigga_snake.names import Names

from hotel_elasticsearch.aws_utils import boto3_resource_factory

logger = logging.getLogger('hotel_elasticsearch.cluster_node')


class ClusterNode(object):
    def __init__(self, name):
        parser = Names(name)
        self._name = name
        self._elastic_search_cluster_name = f"{parser.app}-{parser.stack}"
        self._backup_path = parser.stack
        self._tags = None

    def __str__(self):
        return self.elastic_search_cluster_name

    @property
    def elastic_search_cluster_name(self):
        return self._elastic_search_cluster_name

    @property
    def backup_path(self):
        return self._backup_path

    @property
    def is_master(self):
        return 'master' in self._name

    @property
    def is_elected_master(self):
        try:
            response = requests.get('http://localhost:9200/_cat/master', timeout=1)
            response.raise_for_status()
            return response.text.split()[-1] == self.instance_id
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.HTTPError,
            IndexError
        ):
            logger.exception('Could not determine if this node is the elected master')
            return False

    @property
    def is_data(self):
        return 'data' in self._name

    @property
    def is_client(self):
        return 'client' in self._name

    @property
    def instance_id(self):
        try:
            return requests.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=1).text
        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.HTTPError):
            return None

    @property
    def tags(self):
        if self._tags is not None:
            return self._tags
        else:
            self._tags = {}
            if self.instance_id:
                try:
                    ec2 = boto3_resource_factory('ec2')
                    ec2instance = ec2.Instance(self.instance_id)
                    # Tag keys must be unique, so we can safely flatten the list of tags into one dict
                    for tag in ec2instance.tags:
                        self._tags[tag['Key']] = tag['Value']
                except botocore.exceptions.ClientError:
                    pass

        return self._tags
