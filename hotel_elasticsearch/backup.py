import random
import time
import datetime
from hotel_elasticsearch.dist_locks import Timer, BlockedByAnotherTimerException
import requests
from requests.packages.urllib3.exceptions import HTTPError


class BackupManager(object):
    def run_backup(self):
        now = datetime.datetime.now()
        r = requests.put("http://localhost:9200/_snapshot/%s/%s/" % (
            'hotel-backup',
            'snapshot_%s-%s-%sT%s' % (now.year, now.month, now.day, now.hour)
        ))
        r.raise_for_status()


def backup_thread():
    while True:
        try:
            with Timer('es_backup', days=1) as t:
                bm = BackupManager()
                try:
                    bm.run_backup()
                except HTTPError:
                    t.clear_timer()  # If backup failed, try again on this or another node
        except BlockedByAnotherTimerException:  # Backup has already been run, let it pass
            pass
        time.sleep(random.randrange(60, 120))