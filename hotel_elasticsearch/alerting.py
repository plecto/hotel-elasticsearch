import logging

import requests
from aws_secretsmanager_caching import SecretCacheConfig, SecretCache
from requests.exceptions import HTTPError

from hotel_elasticsearch.aws_utils import botocore_client_factory
from hotel_elasticsearch.clusternode import ClusterNode
from hotel_elasticsearch.configuration import HotelElasticSearchConfig


logger = logging.getLogger('hotel_elasticsearch.cluster_node')


class BaseAlerter(object):
    def __init__(self, config: dict, cluster_node: ClusterNode):
        self.cluster_node = cluster_node
        self.config = config
        self.validate_config()

    def validate_config(self):
        raise NotImplementedError

    def alert(self, message):
        raise NotImplementedError


class NoopAlerter(BaseAlerter):
    def validate_config(self):
        pass

    def alert(self, message):
        pass


class PagerDutyAlerter(BaseAlerter):
    def validate_config(self):
        assert 'pagerduty' in self.config
        assert 'type' in self.config['pagerduty']
        if self.config['pagerduty']['type'] == 'aws-secret':
            assert 'secret_name' in self.config['pagerduty']

    def alert(self, message):
        result = requests.post(
            'https://events.pagerduty.com/v2/enqueue',
            json={
                'routing_key': get_aws_secret(self.config['pagerduty']['secret_name']),
                'event_action': 'trigger',
                'payload': {
                    'summary': message,
                    'severity': 'warning',
                    'source': 'urn:hotel-elasticSearch',
                }
            }
        )
        try:
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e.response.text)
            logger.exception(requests.exceptions.HTTPError)


def alerter_factory(cluster_node):
    config = HotelElasticSearchConfig()
    if config['hotel']['alerting']['alerter'] == 'pagerduty':
        return PagerDutyAlerter(config['hotel']['alerting'], cluster_node)
    else:
        return NoopAlerter({}, cluster_node)


def get_aws_secret(secret_name):
    client = botocore_client_factory('secretsmanager')
    cache_config = SecretCacheConfig()  # Initializes an LRU cache on the thread
    cache = SecretCache(config=cache_config, client=client)
    return cache.get_secret_string(secret_name)