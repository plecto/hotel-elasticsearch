import json
from threading import Thread
import time

import requests

from hotel_elasticsearch.backup import backup_thread
from hotel_elasticsearch.clusternode import ClusterNode
from hotel_elasticsearch.configuration import ElasticSearchConfig
from hotel_elasticsearch.service import ElasticSearchService
import os
import sys
import logging.config


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'hotel_elasticsearch.backup': {
            'level': 'INFO',
            'handlers': ['console'],
            'propagate': False
        },
        'hotel_elasticsearch.cluster_node': {
            'level': 'INFO',
            'handlers': ['console'],
            'propagate': False
        },
        'hotel_elasticsearch.service': {
            'level': 'INFO',
            'handlers': ['console'],
            'propagate': False
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
        }
    },
    'root': {
        'level': 'WARN',
        'handlers': ['console']
    }

})


def run():
    if 'CLOUD_CLUSTER' in os.environ:
        name = os.environ['CLOUD_CLUSTER']
    elif len(sys.argv) > 1:
        name = sys.argv[1]
    else:
        raise Exception("No name provided, please provide it as 1st argument")

    cluster_node = ClusterNode(name)

    try:
        config = ElasticSearchConfig(cluster_node)
    except IOError:
        if len(sys.argv) > 2:
            config = ElasticSearchConfig(cluster_node, sys.argv[2])
        else:
            raise Exception("No config path provided, please provide it as 2nd argument")
    config.save()

    service = ElasticSearchService()

    if not service.running():
        service.start()

    # Install any required templates only on the first run
    if cluster_node.tags.get('initial_master', 'False') == 'True':
        try:
            for file in os.listdir('/etc/hotel-elasticsearch/templates'):
                if file.endswith('.json'):
                    with open('/etc/hotel-elasticsearch/templates/' + file) as f:
                        response = requests.put(
                            'http://localhost:9200/_template/' + file[:-5],
                            json.dumps(file.read()),
                            headers={'Content-Type': 'application/json'},
                            params={'include_type_name': 'true'},
                        )
                        response.raise_for_status()

        except FileNotFoundError():
            pass


    backup = Thread(target=backup_thread, args=(cluster_node,) )
    backup.daemon = True
    backup.start()

    while True:  # Keep main thread alive until cancelled
        time.sleep(600)
