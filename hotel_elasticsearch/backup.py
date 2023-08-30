import datetime
import os
import random
import time

import requests
from requests.exceptions import HTTPError
import logging

logger = logging.getLogger('hotel_elasticsearch.backup')


class BackupException(Exception):
    pass


class BackupManager(object):
    def __init__(self, cluster_node):
        self.cluster_node = cluster_node

    def check_backup(self):
        try:
            result = requests.get('http://localhost:9200/_snapshot/cluster_backup')
            result.raise_for_status()
            result = requests.get('http://localhost:9200/_slm/policy/nightly-backup')
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            self.configure_backup()

        try:
            result = requests.get('http://localhost:9200/_snapshot/cluster_backup/*?order=desc&size=1')
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            raise BackupException('Backup listing request failed')

        result_dict = result.json()
        if 'snapshots' not in result_dict or len(result_dict['snapshots']) == 0:
            raise BackupException('No backups found')
        else:
            snapshot = result_dict['snapshots'][0]
            if snapshot['state'] != 'SUCCESS':
                raise BackupException('Latest backup is not in a success state')
            elif snapshot["start_time"] < datetime.datetime.now() - datetime.timedelta(days=1):
                raise BackupException('Latest backup is older than 1 day')



    def restore_backup(self):
        raise NotImplementedError('This is a dangerous operation, do it manually')

    def configure_backup(self):
        self._configure_backup_repository()
        self._configure_backup_schedule()

    def _configure_backup_repository(self):
        try:
            result = requests.put(
                'http://localhost:9200/_snapshot/cluster_backup',
                json={
                    {
                        'type': 's3',
                        'settings': {
                            'bucket': self.bucket,
                            'base_path': self.cluster_node.backup_path,
                            'max_snapshot_bytes_per_sec': '200mb',
                        }
                    }
                }
            )
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            raise BackupException('Could not configure backup bucket')

    def _configure_backup_schedule(self):
        try:
            result = requests.put(
                'http://localhost:9200/_slm/policy/nightly-backup',
                {
                    'schedule': '0 30 3 * * ?',
                    'name': '<nightly-backup-{now/d}>',
                    'repository': 'cluster_backup',
                    'config': {
                        'indices': '*',
                        'include_global_state': False
                    },
                    'retention': {
                        'expire_after': '7d',
                        'min_count': 7,
                        'max_count': 14
                    }
                }
            )
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            raise BackupException('Could not configure backup schedule')


    @property
    def bucket(self):
        return os.environ.get('CLUSTER_BACKUP_BUCKET', 'hotel-elasticsearch-backup')


def backup_thread(cluster_node):
    bm = BackupManager(cluster_node)

    while True:
        time.sleep(random.randrange(5*60, 10*60))
        bm.check_backup()

