import boto
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING
from boto.exception import JSONResponseError


def get_lock_table(connection=None):
    if not connection:
        connection = boto.dynamodb2.connect_to_region('eu-west-1')

    lock_table_name = 'hotel_elasticsearch_lock'

    lock_table_config = dict(
        connection=connection,
        schema=[
            HashKey('lock_name', data_type=STRING),
        ],
        throughput={
            'read': 5,
            'write': 5,
        }
    )

    try:
        connection.describe_table(lock_table_name)
    except JSONResponseError:
        Table.create(
            lock_table_name,
            **lock_table_config
        )

    return Table(
        lock_table_name,
        **lock_table_config
    )