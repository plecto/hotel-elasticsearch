from threading import Thread
import time
from hotel_elasticsearch.backup import backup_thread
from hotel_elasticsearch.clusternode import ClusterNode
from hotel_elasticsearch.configuration import ElasticSearchConfig
from hotel_elasticsearch.service import ElasticSearchService
import os
import sys
import logging


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'hotel_elasticsearch.backup': {
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

    cluster = ClusterNode(name)

    try:
        config = ElasticSearchConfig(cluster)
    except IOError:
        if len(sys.argv) > 2:
            config = ElasticSearchConfig(cluster, sys.argv[2])
        else:
            raise Exception("No config path provided, please provide it as 2nd argument")
    config.save()

    service = ElasticSearchService()

    if not service.running():
        service.start()

    backup = Thread(target=backup_thread, )
    backup.daemon = True
    backup.start()

    while True:  # Keep main thread alive until cancelled
        time.sleep(600)
