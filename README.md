# hotel-elasticsearch

This library modifies configuration files of Elastic Search to reflect if the node should be:

- Master node
- Client node
- Data node

It also conveniently manages the process itself and provides backup functionality.

## Configuration
Configuration can be done in a yaml file /etc/hotel-elasticsearch/config.yaml

```yaml
hotel:
  alerter: None|PagerDuty
  pager_duty:
    service_key: <pager duty service key>
    subdomain: <pager duty subdomain>
```
