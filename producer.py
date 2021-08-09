#!/usr/bin/python3

from kafka import KafkaProducer

from random import seed, randint, gauss

import time
import sys

node = sys.argv[1]
tClass = float(sys.argv[2])
mSizeString = sys.argv[3]
mRate = float(sys.argv[4])
nTopics = int(sys.argv[5])

seed(1)

mSizeParams = mSizeString.split(',')
msgSize = 0

nodeID = node[1:]

producer = KafkaProducer(bootstrap_servers="10.0.0."+nodeID+":9092")

while True:

	if mSizeParams[0] == 'fixed':
		msgSize = int(mSizeParams[1])
	elif mSizeParams[0] == 'gaussian':
		msgSize = int(gauss(float(mSizeParams[1]), float(mSizeParams[2])))
	
		if msgSize < 1:
			msgSize = 1

	message = [97] * msgSize
	bMsg = bytearray(message)

	topicID = randint(0, nTopics-1)
	topicName = 'topic-'+str(topicID)

	#TODO: write to random topic
	producer.send(topicName, bMsg)
	time.sleep(1.0/(mRate*tClass))


#def on_send_success(record_metadata):
    #print(record_metadata.topic)
    #print(record_metadata.partition)
    #print(record_metadata.offset)

#def on_send_error(excp):
    #log.error('I am an errback', exc_info=excp)
    # handle exception

#producer.send('topic-0', b'raw_bytes1').add_callback(on_send_success).add_errback(on_send_error)

#Sleep is important to give time for producer
#to send the message before process ends
#time.sleep(0.01)








