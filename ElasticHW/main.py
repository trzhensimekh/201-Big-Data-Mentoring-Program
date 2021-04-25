import csv
import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from logstash_async.handler import AsynchronousLogstashHandler

es = Elasticsearch(
    hosts=['localhost'],
    http_auth=('elastic', 'changeme')
)

host = 'localhost'
port = 8089

test_logger = logging.getLogger('python-logstash-logger')
test_logger.setLevel(logging.DEBUG)
async_handler = AsynchronousLogstashHandler(host, port, database_path=None)
test_logger.addHandler(async_handler)


def store_record(record, index, elastic=es):
    try:
        elastic.index(index=index, body=record)
    except Exception as ex:
        test_logger.error(f"Error while creating index{ex}")


mapping = {
    'mappings': {
        "properties": {
            "iata": {
                "type": "text"
            },
            "airport": {
                "type": "text",
            },
            "city": {
                "type": "text"
            },
            "state": {
                "type": "text",
            },
            "country": {
                "type": "text",
            },
            "lat": {
                "type": "float",
            },
            "long": {
                "type": "float"
            },
            "published_from": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            }
        }
    }
}

response = es.indices.create(index='usa_airports', body=mapping, ignore=400)

with open('resources/airports.csv') as airports:
    csv_reader = csv.DictReader(airports)
    for row in csv_reader:
        row['date'] = datetime.utcnow()
        if row['state'] == 'NY':
            test_logger.warning("Imitating WARN log")
        store_record(row,'usa_airports')
        test_logger.info("INFO log example")

print('response', response)
