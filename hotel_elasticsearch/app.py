from threading import Thread
import time
from hotel_elasticsearch.backup import backup_thread
from hotel_elasticsearch.cluster import Cluster
from hotel_elasticsearch.configuration import ElasticSearchConfig
from flask import Flask, jsonify
from hotel_elasticsearch.service import ElasticSearchService
import os
import sys


app = Flask(__name__)

@app.route("/")
def home():
    service = ElasticSearchService()
    return jsonify(
        running=service.running()
    )

@app.route("/start", methods=['POST'])
def start_es():
    service = ElasticSearchService()
    service.start()
    return jsonify(

    )

@app.route("/stop", methods=['POST'])
def stop_es():
    service = ElasticSearchService()
    service.stop()
    return jsonify(

    )


def run():
    if 'CLOUD_CLUSTER' in os.environ:
        name = os.environ['CLOUD_CLUSTER']
    elif len(sys.argv) > 1:
        name = sys.argv[1]
    else:
        raise Exception("No name provided, please provide it as 1st argument")

    cluster = Cluster(name)

    try:
        config = ElasticSearchConfig(cluster)
    except IOError:
        if len(sys.argv) > 2:
            config = ElasticSearchConfig(cluster, sys.argv[2])
        else:
            raise Exception("No config path provided, please provide it as 2nd argument")
    config.save()

    service = ElasticSearchService()

    if not service.running():
        service.start()

    webserver = Thread(target=app.run, )
    webserver.daemon = True
    webserver.start()

    backup = Thread(target=backup_thread, )
    backup.daemon = True
    backup.start()

    while True:  # Keep main thread alive until cancelled
        time.sleep(600)