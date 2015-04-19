from unittest import TestCase
from hotel_elasticsearch.cluster import Cluster


class TestCluster(TestCase):

    def test_elastic_cluster_name(self):
        self.assertEqual(
            Cluster("elasticsearch-prod-master-h0slave-r01").elastic_search_cluster_name,
            "elasticsearch-prod"
        )

    def test_elastic_cluster_master(self):
        self.assertEqual(
            Cluster("elasticsearch-prod-master-h0slave-r01").master,
            True
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-client-h0slave-r01").master,
            False
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-data-h0slave-r01").master,
            False
        )

    def test_elastic_cluster_client(self):
        self.assertEqual(
            Cluster("elasticsearch-prod-master-h0slave-r01").client,
            False
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-client-h0slave-r01").client,
            True
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-data-h0slave-r01").client,
            False
        )

    def test_elastic_cluster_data(self):
        self.assertEqual(
            Cluster("elasticsearch-prod-master-h0slave-r01").data,
            False
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-client-h0slave-r01").data,
            False
        )
        self.assertEqual(
            Cluster("elasticsearch-prod-data-h0slave-r01").data,
            True
        )