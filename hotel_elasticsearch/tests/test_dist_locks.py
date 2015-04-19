import time
from boto.dynamodb2.layer1 import DynamoDBConnection
from unittest import TestCase
from hotel_elasticsearch.dist_locks import Timer, BlockedByAnotherTimerException
from hotel_elasticsearch.dynamodb import get_lock_table


class TestLocks(TestCase):
    # Expects DynamoDB local to work

    def setUp(self):
        # Use DynamoDB local for testing!
        self.connection = DynamoDBConnection(
            aws_access_key_id='foo',         # Dummy access key
            aws_secret_access_key='bar',     # Dummy secret key
            host='localhost',                # Host where DynamoDB Local resides
            port=8000,                       # DynamoDB Local port (8000 is the default)
            is_secure=False,                 # Disable secure connections
            region='eu-west-1',
        )

    def test_timer_expiry(self):
        test_timer = 'test_timer'
        lock_table = get_lock_table(connection=self.connection)

        def timer_function(**kwargs):
            with Timer(test_timer, dynamodb_connection=self.connection, **kwargs) as t:
                pass

        now = time.time()
        timer_function(seconds=10)
        itm = lock_table.get_item(lock_name=test_timer, consistent=True)
        self.assertTrue(now < itm['expires'] < now + 12)  # Assert the expiry date is correct allowing some slack
        self.assertRaises(BlockedByAnotherTimerException, timer_function, seconds=10)

        t = Timer(test_timer, dynamodb_connection=self.connection, seconds=0)
        t.clear_timer()  # clean up