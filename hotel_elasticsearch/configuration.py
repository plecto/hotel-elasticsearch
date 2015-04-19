from collections import OrderedDict
import yaml


class ElasticSearchConfig(OrderedDict):
    def __init__(self, cluster, config_file="/etc/elasticsearch/elasticsearch.yml"):
        self.config_file = config_file
        with open(config_file) as f:
            super(ElasticSearchConfig, self).__init__(sorted(yaml.safe_load(f.read()).items()))
        self['cluster']['name'] = cluster.elastic_search_cluster_name
        self['node']['data'] = cluster.data
        self['node']['master'] = cluster.master
        self['http.enabled'] = not (cluster.data and cluster.master) or (cluster.data and cluster.master)

    def save(self):
        """
        Save to config_dir
        :return:
        """
        with open(self.config_file, 'w') as f:
            config = yaml.safe_dump(dict(self), default_flow_style=False)
            f.write(config)
