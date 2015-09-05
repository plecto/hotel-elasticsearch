import random
import time
import datetime
from hotel_elasticsearch.dist_locks import Timer, BlockedByAnotherTimerException
import requests
from requests.exceptions import HTTPError
import logging

logger = logging.getLogger('hotel_elasticsearch.backup')

class BackupManager(object):
    def run_backup(self):
        now = datetime.datetime.now()
        snapshot_name = 'snapshot_%s-%s-%s_%s' % (now.year, now.month, now.day, now.hour)
        logger.info("Executing backup command, snapshot name: %s" % snapshot_name)
        r = requests.put("http://localhost:9200/_snapshot/%s/%s/" % (
            'hotel-backup',
            snapshot_name
        ))
        r.raise_for_status()
        logger.info("Snapshot successfully scheduled")


def backup_thread():
    while True:
        try:
            with Timer('es_backup', days=1) as t:
                bm = BackupManager()
                try:
                    bm.run_backup()
                except HTTPError as e:
                    logger.exception(e)
                    t.clear_timer()  # If backup failed, try again on this or another node
        except BlockedByAnotherTimerException:  # Backup has already been run, let it pass
            pass
        time.sleep(random.randrange(60, 120))
