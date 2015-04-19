from tempfile import NamedTemporaryFile
from unittest import TestCase
from hotel_elasticsearch.cluster import Cluster
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
  data: false
  master: true
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
        self.config_file_instance.write(self.initial_data)
        self.config_file_instance.seek(0)

    def test_initial_file(self):
        config = ElasticSearchConfig(Cluster("elasticsearch-testcase-master-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        # self.assertEqual(self.initial_data, self.config_file_instance.read())
        self.assertDictEqual(yaml.load(self.initial_data), yaml.load(self.config_file_instance.read()))
        self.config_file_instance.close()

    def test_client_node(self):
        self.initial_data = self.initial_data.replace("data: false", "data: false")
        self.initial_data = self.initial_data.replace("master: true", "master: false")
        config = ElasticSearchConfig(Cluster("elasticsearch-testcase-client-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.load(self.initial_data), yaml.load(self.config_file_instance.read()))
        self.config_file_instance.close()

    def test_master_node(self):
        self.initial_data = self.initial_data.replace("data: false", "data: false")
        config = ElasticSearchConfig(Cluster("elasticsearch-testcase-master-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.load(self.initial_data), yaml.load(self.config_file_instance.read()))
        self.config_file_instance.close()

    def test_data_node(self):
        self.initial_data = self.initial_data.replace("master: true", "master: false")
        self.initial_data = self.initial_data.replace("data: false", "data: true")
        config = ElasticSearchConfig(Cluster("elasticsearch-testcase-data-h0slave-r01"), config_file=self.config_file_instance.name)
        config.save()
        self.assertDictEqual(yaml.load(self.initial_data), yaml.load(self.config_file_instance.read()))
        self.config_file_instance.close()