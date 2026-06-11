import json
import uuid
from confluent_kafka import Producer

config={'bootstrap.servers':'localhost:9092'}

producer=Producer(config)

def feedback(err,msg):
    if err:
        print(f"error:{err}")
    else:
        print(f"succesful:{msg.value().decode('utf-8')}")

order={
    "orderid":str(uuid.uuid4()),
    "user":"kau",
    "item":"pizza",
    "quantity":1
}

value=json.dumps(order).encode('utf-8')
producer.produce(topic="order",value=value,callback=feedback)
producer.flush()