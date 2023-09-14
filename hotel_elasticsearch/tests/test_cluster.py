from unittest import TestCase
from hotel_elasticsearch.clusternode import ClusterNode


class TestCluster(TestCase):

    def test_elastic_cluster_name(self):
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-master-h0slave-r01").elastic_search_cluster_name,
            "elasticsearch-prod1"
        )

    def test_elastic_cluster_master(self):
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-master-h0slave-r01").is_master,
            True
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-client-h0slave-r01").is_master,
            False
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-data-h0slave-r01").is_master,
            False
        )

    def test_elastic_cluster_client(self):
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-master-h0slave-r01").is_client,
            False
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-client-h0slave-r01").is_client,
            True
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-data-h0slave-r01").is_client,
            False
        )

    def test_elastic_cluster_data(self):
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-master-h0slave-r01").is_data,
            False
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-client-h0slave-r01").is_data,
            False
        )
        self.assertEqual(
            ClusterNode("elasticsearch-prod1-data-h0slave-r01").is_data,
            True
        )