from threading import Thread
import time
import datetime
from boto.dynamodb2.exceptions import ItemNotFound, ConditionalCheckFailedException
from boto.dynamodb2.items import Item
from hotel_elasticsearch.dynamodb import get_lock_table


class BlockedByAnotherTimerException(Exception):
    pass


class Timer(object):
    def __init__(self, key, days=0, hours=0, minutes=0, seconds=0, wait=0, dynamodb_connection=None):
        self.key = key
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.wait = wait
        self.expires = self.days*60*60*24 + self.hours * 60 * 60 + self.minutes * 60 + self.seconds
        self.dynamodb_connection = dynamodb_connection

    def __enter__(self):
        lock_table = get_lock_table(connection=self.dynamodb_connection)
        try:
            lock_item = lock_table.get_item(lock_name=self.key, consistent=True)
            if lock_item['expires'] < time.time():
                lock_item['expires'] = time.time() + self.expires
                try:
                    lock_item.save()
                    return self
                except ConditionalCheckFailedException:
                    pass
        except ItemNotFound:
            lock_item = Item(lock_table, data={
                'lock_name': self.key,
                'expires': time.time() + self.expires
            })
            lock_item.save()
            return self
        raise BlockedByAnotherTimerException()

    def clear_timer(self):
        lock_table = get_lock_table(connection=self.dynamodb_connection)
        lock_item = lock_table.get_item(lock_name=self.key, consistent=True)
        lock_item['expires'] = time.time()
        lock_item.save()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass