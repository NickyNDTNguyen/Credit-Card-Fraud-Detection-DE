from kafka import KafkaConsumer
from rules import *
from dao import *
import sys
import datetime
import json


bootstrap_servers = ['18.211.252.152:9092']
topicName = 'transactions-topic-verified'
consumer = KafkaConsumer (topicName, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest')

try:
    for message in consumer:
        trans = json.loads(message.value)
        status = verify_rules(trans["card_id"], trans["amount"], trans["postcode"], trans["transaction_dt"])
        insertCardTrans(trans, status)
        print("Transaction has been inserted into card transactions table.")
        if (status==True):            
            updateLookup(trans)
            print("Last successful transaction has been updated!")
        else:
            print('There is suspicious transaction! FRAUD!')
            
except KeyboardInterrupt:
    sys.exit()
    