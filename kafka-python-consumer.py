#!/usr/bin/python3

from kafka import KafkaConsumer

consumer = KafkaConsumer('example-topic',
			 bootstrap_servers='10.0.0.2:9092',
			 auto_offset_reset='earliest',
			 enable_auto_commit=True,
			 consumer_timeout_ms=1000
			)

for msg in consumer:
	print (msg.value)

consumer.close()
