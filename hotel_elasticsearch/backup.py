import datetime
import os
import random
import time

import requests
from requests.exceptions import HTTPError
import logging

from hotel_elasticsearch.alerting import alerter_factory
from hotel_elasticsearch.configuration import HotelElasticSearchConfig

logger = logging.getLogger('hotel_elasticsearch.backup')


class BackupException(Exception):
    pass


class BackupManager(object):
    NIGHTLY_BACKUP_POLICY = 'nightly-backup'
    BACKUP_REPOSITORY = 'cluster_backup'
    RESTORE_REPOSITORY = 'cluster_restore'
    def __init__(self, cluster_node):
        self.cluster_node = cluster_node
        self.config = HotelElasticSearchConfig()

    def check_backup(self):
        # Only the elected master should check the backup
        if not self.cluster_node.is_elected_master:
            return
        try:
            try:
                self._check_backup_configuration()
            except HTTPError as e:
                logger.exception(e)
                self.configure_backup()

            try:
                result = requests.get(f'http://localhost:9200/_snapshot/{self.BACKUP_REPOSITORY}/*?order=desc&size=1')
                result.raise_for_status()
            except HTTPError as e:
                logger.exception(e)
                raise BackupException('Backup listing request failed') from e

            result_dict = result.json()
            if 'snapshots' not in result_dict or len(result_dict['snapshots']) == 0:
                raise BackupException('No backups found')
            else:
                snapshot = result_dict['snapshots'][0]
                if snapshot['state'] not in ['SUCCESS', 'IN_PROGRESS']:
                    raise BackupException('Latest backup is not in a success state')
                elif snapshot["start_time"] < datetime.datetime.now() - datetime.timedelta(days=1):
                    raise BackupException('Latest backup is older than 1 day')
        except BackupException as e:
            logger.exception(e)
            alerter = alerter_factory(self.cluster_node)
            alerter.alert(str(e))

    def initiate_backup(self):
        try:
            self._check_backup_configuration()
            result = requests.post(f'http://localhost:9200/_slm/policy/{self.NIGHTLY_BACKUP_POLICY}/_execute')
            result.raise_for_status()
        except HTTPError as e:
            logger.error(e.response.text)
            logger.exception(e)
            raise BackupException('Backup request failed') from e

    def _check_backup_configuration(self):
        result = requests.get(f'http://localhost:9200/_snapshot/{self.BACKUP_REPOSITORY}')
        result.raise_for_status()
        result = requests.get(f'http://localhost:9200/_slm/policy/{self.NIGHTLY_BACKUP_POLICY}')
        result.raise_for_status()

    def restore_backup(self, backup_id):
        if not self.cluster_is_empty():
            raise BackupException('Cannot restore backup, cluster is not empty')
        if not self.restore_repository_exists():
            raise BackupException('Cannot restore backup, restore repository does not exist')
        try:
            result = requests.post(
                f'http://localhost:9200/_snapshot/{self.RESTORE_REPOSITORY}/{backup_id}/_restore',
                json={
                    'indices': '*,-.*',
                }
            )
            result.raise_for_status()
        except HTTPError as e:
            logger.error(e.response.text)
            logger.exception(e)
            raise BackupException('Backup restore request failed') from e

    def cluster_is_empty(self):
        response = requests.get('http://localhost:9200/*,-.*/_stats?pretty')
        if response.json()['_shards']['total'] > 0:
            return False
        return True

    def restore_repository_exists(self):
        try:
            result = requests.get(f'http://localhost:9200/_snapshot/{self.RESTORE_REPOSITORY}')
            result.raise_for_status()
            return True
        except HTTPError as e:
            logger.exception(e)
            return False

    def configure_backup(self):
        self._configure_backup_repository()
        self._configure_backup_schedule()

    def _configure_backup_repository(self):
        try:
            result = requests.put(
                f'http://localhost:9200/_snapshot/{self.BACKUP_REPOSITORY}',
                json={
                        'type': 's3',
                        'settings': {
                            'bucket': self.bucket,
                            'base_path': self.cluster_node.backup_path,
                            'max_snapshot_bytes_per_sec': '200mb',
                        }
                }
            )
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            # Even though the request fails the repository is still created and should be deleted.
            requests.delete(f'http://localhost:9200/_snapshot/{self.BACKUP_REPOSITORY}')
            raise BackupException('Could not configure backup repository') from e

    def configure_restore_repository(self, backup_path, bucket=None):
        if bucket is None:
            bucket = self.bucket
        try:
            result = requests.put(
                f'http://localhost:9200/_snapshot/{self.RESTORE_REPOSITORY}',
                json={
                        'type': 's3',
                        'settings': {
                            'bucket': bucket,
                            'base_path': backup_path,
                            'readonly': True
                        }
                }
            )
            result.raise_for_status()
        except HTTPError as e:
            logger.exception(e)
            # Even though the request fails the repository is still created and should be deleted.
            requests.delete(f'http://localhost:9200/_snapshot/{self.RESTORE_REPOSITORY}')
            raise BackupException('Could not configure restore repository')

    def _configure_backup_schedule(self):
        try:
            result = requests.put(
                'http://localhost:9200/_slm/policy/nightly-backup',
                json={
                    'schedule': '0 30 3 * * ?',
                    'name': '<nightly-backup-{now/d}>',
                    'repository': self.BACKUP_REPOSITORY,
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

    def list_snapshots_from_restore_repository(self):
        result = requests.get(
            f'http://localhost:9200/_cat/snapshots/{self.RESTORE_REPOSITORY}/?h=id,s,sti,eti,i,ss,fs,r&v'
        )
        result.raise_for_status()
        return result.text

    @property
    def bucket(self):
        return HotelElasticSearchConfig()['hotel']['backup']['bucket']


def backup_thread(cluster_node):
    bm = BackupManager(cluster_node)

    while True:
        time.sleep(random.randrange(5*60, 10*60))
        bm.check_backup()

