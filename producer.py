import json
import uuid
from confluent_kafka import Producer
import os
config = {'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVERS', 'broker:9092')}


producer=Producer(config)

def feedback(err,msg):
    if err:
        print(f"error:{err}")
    else:
        print(f"succesful:{msg.value().decode('utf-8')}")
        print(f"Topic: {msg.topic()} Offset: {msg.offset()} Partition: {msg.partition()}")

order={
    "orderid":str(uuid.uuid4()),
    "user":"shobi",
    "item":"burger",
    "quantity":2
}

value=json.dumps(order).encode('utf-8')
producer.produce(topic="order",value=value,callback=feedback)
producer.flush()