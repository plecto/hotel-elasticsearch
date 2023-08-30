from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest.mock import PropertyMock, patch

from hotel_elasticsearch.clusternode import ClusterNode
from hotel_elasticsearch.configuration import ElasticSearchConfig
import yaml


class TestConfiguration(TestCase):
    initial_data = """cluster:
  name: elasticsearch-testcase
discovery:
  type: ec2
http.enabled: true
node:
  availability_zone: \${EC2_AZ}
  instance_id: \${EC2_INSTANCE_ID}
  name: test_node
  roles: ['master']
path:
  data: /mnt/elasticsearch/
  logs: /var/log/elasticsearch
routing:
  allocation:
    awareness:
      attributes: instance_id, availability_zone
"""

    def setUp(self):
        self.config_file_instance = NamedTemporaryFile()
        self.config_file_instance.write(self.initial_data.encode('utf-8'))
        self.config_file_instance.seek(0)

    def test_initial_file(self):
        config = ElasticSearchConfig(ClusterNode("elasticsearch-testcase-master-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        # self.assertEqual(self.initial_data, self.config_file_instance.read())
        self.assertDictEqual(yaml.safe_load(self.initial_data), yaml.safe_load(self.config_file_instance.read().decode('utf-8')))
        self.config_file_instance.close()

    def test_client_node(self):
        self.initial_data = self.initial_data.replace("['master']", "[]")
        config = ElasticSearchConfig(ClusterNode("elasticsearch-testcase-client-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.safe_load(self.initial_data), yaml.safe_load(self.config_file_instance.read()))
        self.config_file_instance.close()

    def test_master_node(self):
        config = ElasticSearchConfig(ClusterNode("elasticsearch-testcase-master-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.safe_load(self.initial_data), yaml.safe_load(self.config_file_instance.read()))
        self.config_file_instance.close()

    @patch('hotel_elasticsearch.clusternode.ClusterNode.tags', {'initial_master': 'True'})
    @patch('hotel_elasticsearch.clusternode.ClusterNode.instance_id', "i-1234567890")
    def test_initial_master_node(self):
        config = ElasticSearchConfig(ClusterNode("elasticsearch-testcase-master-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        reference_config = yaml.safe_load(self.initial_data)
        reference_config['cluster']['initial_master_nodes'] = ['i-1234567890']
        self.assertDictEqual(reference_config, yaml.safe_load(self.config_file_instance.read()))
        self.config_file_instance.close()

    def test_data_node(self):
        self.initial_data = self.initial_data.replace("['master']", "['data']")
        config = ElasticSearchConfig(ClusterNode("elasticsearch-testcase-data-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.safe_load(self.initial_data), yaml.safe_load(self.config_file_instance.read()))
        self.config_file_instance.close()