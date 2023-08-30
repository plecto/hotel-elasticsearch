import requests
import boto3.ec2
import botocore.exceptions
from frigga_snake.names import Names


class ClusterNode(object):
    def __init__(self, name):
        parser = Names(name)
        self._name = name
        self._elastic_search_cluster_name = "%s-%s" % (parser.app, parser.stack)
        self._tags = None


    @property
    def elastic_search_cluster_name(self):
        return self._elastic_search_cluster_name

    @property
    def is_master(self):
        return 'master' in self._name

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
            if self.instance_id:
                try:
                    ec2 = boto3.resource('ec2')
                    ec2instance = ec2.Instance(self.instance_id)
                    self._tags = ec2instance.tags
                except botocore.exceptions.ClientError:
                    pass
            else:
                self._tags = {}

        return self._tags
