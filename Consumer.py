from confluent_kafka import Consumer, KafkaError

topic = 'test'
c = Consumer({
    'bootstrap.servers': 'B_kafka1:9092',
    'acks': '-1',
	'group.id': 'testConsumer',
#    'auto.offset.reset': 'earliest'
})

c.subscribe([topic])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
	
    print('Received message: {}'.format(msg.value().decode('utf-8')))
	


c.close()
