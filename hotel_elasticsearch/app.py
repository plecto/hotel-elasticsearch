import argparse
import json
from threading import Thread
import time

import requests

from hotel_elasticsearch.alerting import alerter_factory
from hotel_elasticsearch.backup import backup_thread, BackupManager
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


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Manage and run elasticsearch cluster nodes",
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 1.0.0"
    )
    parser.add_argument('--name', help='Name of the cluster. If left out will be read from CLOUD_CLUSTER env var')
    parser.add_argument('--run', help='Run the elasticsearch node', action='store_true')
    parser.add_argument(
        '--set_restore_cluster', dest='restore_cluster_stackname', help='Set the cluster to restore from. Fx "prod5"'
    )
    parser.add_argument('--list_backups', help='List backups that can be restored', action='store_true')
    parser.add_argument('--configure_backup', help='Configure the backup repository', action='store_true')
    parser.add_argument(
        '--initiate_backup',
        help='Initiate a backup now. Requires the backup repository and SLM to be defined already',
        action='store_true'
    )
    parser.add_argument(
        '--restore_backup', dest='backup_id', help='Restore the snapshot with <backup_id>. The cluster must be empty'
    )
    parser.add_argument('--test_alerting', help='Test the alerting', action='store_true')

    return parser


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)
    if args.name is None and 'CLOUD_CLUSTER' in os.environ:
        args.name = os.environ['CLOUD_CLUSTER']
    elif args.name is None:
        raise Exception("No name provided, please provide it with --name")
    if args.run:
        run(args.name)
    elif args.restore_cluster_stackname:
        cluster_node = ClusterNode(args.name)
        backup_manager = BackupManager(cluster_node)
        backup_manager.configure_restore_repository(args.restore_cluster_stackname)
    elif args.configure_backup:
        cluster_node = ClusterNode(args.name)
        backup_manager = BackupManager(cluster_node)
        backup_manager.configure_backup()
    elif args.initiate_backup:
        cluster_node = ClusterNode(args.name)
        backup_manager = BackupManager(cluster_node)
        backup_manager.initiate_backup()
    elif args.list_backups:
        cluster_node = ClusterNode(args.name)
        backup_manager = BackupManager(cluster_node)
        print(backup_manager.list_snapshots_from_restore_repository())
    elif args.backup_id:
        cluster_node = ClusterNode(args.name)
        backup_manager = BackupManager(cluster_node)
        backup_manager.restore_backup(args.backup_id)
    elif args.test_alerting:
        cluster_node = ClusterNode(args.name)
        alerter = alerter_factory(cluster_node)
        alerter.alert(f"This is a test invocation of the alerting system from {cluster_node}")


def run(name):
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


if __name__ == '__main__':
    main()
