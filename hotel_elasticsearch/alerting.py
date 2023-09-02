import requests

from hotel_elasticsearch.clusternode import ClusterNode
from hotel_elasticsearch.configuration import HotelElasticSearchConfig


class BaseAlerter(object):
    def __init__(self, config: dict, cluster_node: ClusterNode):
        self.config = config
        self.validate_config()

    def validate_config(self):
        raise NotImplementedError

    def alert(self, message):
        raise NotImplementedError


class PagerDutyAlerter(BaseAlerter):
    def validate_config(self):
        if 'pagerduty_service_key' not in self.config:
            raise ValueError('pagerduty_service_key is required')

    def alert(self, message):
        requests.post(
            'https://events.pagerduty.com/v2/enqueue',
            json={
                'routing_key': self.config['pagerduty_api_key'],
                'event_action': 'trigger',
                'payload': {
                    'summary': message,
                    'severity': 'warning',
                    'source': 'urn:hotel-elasticSearch',
                }
            }
        )


def alerter_factory(cluster_node):
    config = HotelElasticSearchConfig()
    if config['hotel']['alerter'] == 'PagerDutyAlerter':
        return PagerDutyAlerter(config['hotel']['pagerduty'], cluster_node)
