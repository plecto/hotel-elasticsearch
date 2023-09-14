# hotel-elasticsearch

This library modifies configuration files of Elasticsearch to reflect if the node should be:

- Master node
- Client node
- Data node

It also conveniently manages the process itself and provides backup functionality.

## Installation
    
    ```pip install hotel-elasticsearch```

## Configuration
Configuration can be done in a yaml file /etc/hotel-elasticsearch/config.yaml

```yaml
hotel:
  backup:
    bucket: <mybucket>
  alerting:
    alerter: pagerduty|None
    pagerduty:
      type: aws-secret
      secret_name: <secret_name>

```

## Usage

```
usage: hotel-elasticsearch [OPTION] [FILE]...

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --name NAME           Name of the cluster. If left out will be read from
                        CLOUD_CLUSTER env var
  --run                 Run the elasticsearch node
  --set_restore_cluster RESTORE_CLUSTER_STACKNAME
                        Set the cluster to restore from. Fx "prod5"
  --list_backups        List backups that can be restored
  --restore_backup BACKUP_ID
                        Restore the snapshot with <backup_id>. The cluster
                        must be empty
  --test_alerting       Test the alerting

