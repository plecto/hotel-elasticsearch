from collections import OrderedDict
import yaml


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
    def __init__(self, cluster, config_file="/etc/hotel-elasticsearch/hotel-config.yml"):
        # Set defaults
        self['hotel']['alerter'] = None
        self['hotel']['backup']['bucket'] = 'hotel-elasticsearch-backup'

        self.config_file = config_file
        try:
            with open(config_file) as f:
                super(HotelElasticSearchConfig, self).__init__(sorted(yaml.safe_load(f.read()).items()))
        except FileNotFoundError:
            pass

