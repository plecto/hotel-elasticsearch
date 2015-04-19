from frigga_snake.names import Names


class Cluster(object):
    def __init__(self, name):
        parser = Names(name)
        self._name = name
        self._elastic_search_cluster_name = "%s-%s" % (parser.app, parser.stack)


    @property
    def elastic_search_cluster_name(self):
        return self._elastic_search_cluster_name

    @property
    def master(self):
        return 'master' in self._name

    @property
    def data(self):
        return 'data' in self._name

    @property
    def client(self):
        return 'client' in self._name