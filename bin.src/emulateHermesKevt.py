#!/usr/bin/env python

# Emulate HermesK events
# input: Rucio DID (scope:name) from stdin. One DID per line

# make sure those DID have metadata "rubin_butler" and "rubin_sidecar" defined.

from rucio.client.replicaclient import ReplicaClient
from rucio.client.didclient import DIDClient
from confluent_kafka import Producer
import json
import time

# Adjust the following for your need
RSE = "SLAC_BUTLER_DISK"
RSEurlPrefix = "davs://sdfdtn005.slac.stanford.edu"
kafkacfg = {
               'bootstrap.servers': '134.79.23.189:9094',
               'client.id': "angry_bird"
           }

global transferProtocol, producer, replicaClient, DIDclient

def sendKafkaMsg(did: str, rse: str):

    scope, name = did.split(":")

    message = {}
    message["scope"] = scope
    message["name"] = name
    message["dst-rse"] = RSE
    replicas = replicaClient.list_replicas(
                                           dids=[{'scope':scope, 'name':name}],
                                           schemes=[transferProtocol],
                                           rse_expression=RSE
                                          )
    message["dst-url"] = None
    for replica in replicas:
        message["dst-url"] = replica["rses"][RSE][0]
        #if message["dst-url"] is not None:
        if message["dst-url"].startswith(RSEurlPrefix):
            break
        else:
            message["dst-url"] = None

    if message["dst-url"] is None:
        print(f'Skip: can not find a matching URL {did}')
        return
    
    try:
        metadataInJsonPlugin = DIDclient.get_metadata(scope=scope, name=name, plugin='JSON')
    
        message["rubin_butler"] = metadataInJsonPlugin['rubin_butler']
        message["rubin_sidecar"] = metadataInJsonPlugin['rubin_sidecar']
    except Exception as e:
        print(f'Skip: can not find Rucio metadata {did}')
        return
    
    #timestamp = int(time.time())
    timestamp = "tomorrow"

    topic = f'{RSE}-{scope}'
    
    d = {
            "event_type": "transfer-done",
            "payload": message,
            "created_at": str(timestamp)
        }
    
    print(json.dumps(d, indent=2))
    
    value = json.dumps(d)
    
    producer.produce(topic=topic, key="message", value=value)
    producer.flush()

if __name__ == '__main__':

    transferProtocol = RSEurlPrefix.split(":")[0]
    producer = Producer(kafkacfg)
    replicaClient = ReplicaClient()
    DIDclient = DIDClient()

    count = 0
    while 1:
        try:
            did = input().strip()
            sendKafkaMsg(did=did, rse=RSE)
            count += 1
            if count == 100:
                time.sleep(1)
                count = 0
        except Exception as e:
            break 
