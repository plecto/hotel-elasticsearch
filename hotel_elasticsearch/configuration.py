import logging
from collections import OrderedDict
import yaml

logger = logging.getLogger('cluster_node')

class ElasticSearchConfig(OrderedDict):
    def __init__(self, cluster, config_file="/etc/elasticsearch/elasticsearch.yml"):
        self.config_file = config_file
        with open(config_file) as f:
            super(ElasticSearchConfig, self).__init__(sorted(yaml.safe_load(f.read()).items()))
        self['cluster']['name'] = cluster.elastic_search_cluster_name
        self['node']['roles'] = []
        if cluster.is_data:
            self['node']['roles'].append('data')
        if cluster.is_master:
            self['node']['roles'].append('master')
        if cluster.tags.get('initial_master', 'False') == 'True':
            self['cluster']['initial_master_nodes'] = [cluster.instance_id]

    def save(self):
        """
        Save to config_dir
        :return:
        """
        with open(self.config_file, 'w') as f:
            config = yaml.safe_dump(dict(self), default_flow_style=False)
            f.write(config)


class HotelElasticSearchConfig(OrderedDict):
    def __init__(self, config_file="/etc/hotel-elasticsearch/hotel-config.yml"):
        try:
            with open(config_file) as f:
                super(HotelElasticSearchConfig, self).__init__(sorted(yaml.safe_load(f.read()).items()))
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found, using defaults")

        # Set defaults
        if 'hotel' not in self:
            self['hotel'] = OrderedDict()
        if 'alerting' not in self['hotel']:
            self['hotel']['alerting'] = OrderedDict()
            if 'alerter' not in self['hotel']['alerting']:
                self['hotel']['alerting']['alerter'] = None
        if 'backup' not in self['hotel']:
            self['hotel']['backup'] = OrderedDict()
        if 'bucket' not in self['hotel']['backup']:
            self['hotel']['backup']['bucket'] = 'hotel-elasticsearch-backup'



