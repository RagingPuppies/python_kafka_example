import uuid
import time
import random
import string
from confluent_kafka import Producer
"""
acks=0 (None also -1)
acks=1 (leader)
acks=all
"""
topic = 'test'
p = Producer({
    'bootstrap.servers': 'A_kafka1:9092'
})

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print("failed to deliver message: {}".format(err.str()))
    else:
        print("produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))
        
i = 0
while True:
    i += 1
    start_time = time.time()
    p.produce(topic, value=randomString(300), callback=acked)
    print("%s latency " % ((time.time() - start_time) * 1000))
    p.flush(10)
    if i==20000:
        break


