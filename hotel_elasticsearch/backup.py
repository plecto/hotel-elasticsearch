import random
import time
import datetime
from hotel_elasticsearch.dist_locks import Timer, BlockedByAnotherTimerException
import requests
from requests.exceptions import HTTPError
import logging

logger = logging.getLogger('hotel_elasticsearch.backup')

class BackupManager(object):
    def check_backup(self):
        pass

    def restore_backup(self):
        pass

    def configure_backup(self):
        pass


def backup_thread():
    bm = BackupManager()

    while True:
        time.sleep(random.randrange(5*60, 10*60))
        bm.check_backup()

