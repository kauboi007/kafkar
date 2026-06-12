import json

from confluent_kafka import Consumer

config={
    "bootstrap.servers":"broker:9092",
    "group.id":"myorder-tracker",
    "auto.offset.reset":"earliest"
}
consumer=Consumer(config)

consumer.subscribe(['order'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        val = msg.value().decode("utf-8")
        value = json.loads(val)
        for x in value.keys():
            print(f"{x} : {value.get(x)}")
        print("\n")
except KeyboardInterrupt:
    print("stopping consumer")
finally:
    consumer.close()