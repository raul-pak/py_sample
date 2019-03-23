#!/usr/bin/python

import sys
import re
import codecs
import time
import random
from kafka import KafkaProducer
from kafka.structs import TopicPartition
from kafka.structs import OffsetAndMetadata
import kafka.errors as Errors

debug = 0

key_min=0
key_max=5000
interval=1
delim=','
max_cycle=0
rows=100

#hosts=['localhost:9092']
hosts=['myLinux:9092']
client_id=''
topic=''
partition=0
acks=1
retries=0
batch_size=16384
linger_ms=0
buffer_memory=33554432
max_request_size=1048576
#request_timeout_ms=30000
request_timeout_ms=10000
nargs=len(sys.argv)
i=1
while i < nargs:
	if sys.argv[i] == '--bootstrap_servers':
		hosts=sys.argv[i+1].split(',')
		i += 2
		continue
	elif sys.argv[i] == '--acks':
		if argv[i+1] == 'all':
			acks=sys.argv[i+1]
		else:
			acks=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--retries':
		retries=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--batch_size':
		batch_size=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--linger_ms': # ms
		linger_ms=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--buffer_memory': # ms
		buffer_memory=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--max_request_size': # ms
		max_request_size=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--request_timeout_ms': # ms
		request_timeout_ms=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--client': # client_id
		client_id=sys.argv[i+1]
		i += 2
		continue
	elif sys.argv[i] == '--topic': # partition
		tp=sys.argv[i+1].split('/')
		topic=tp[0]
		if len(tp) > 1:
			partition=int(tp[1])
		i += 2
		continue
	elif sys.argv[i] == '--cycle':
		max_cycle=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--rows':
		rows=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--interval':
		interval=float(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--key_range':
		keys=sys.argv[i+1].split(",")
		if len(keys) > 1:
			key_min=int(keys[0])
			key_max=int(keys[1])
		else:
			key_min=0
			key_max=int(keys[0])
		i += 2
		continue
	elif sys.argv[i] == '--debug':
		debug=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i][0] == '--help':
		print('Usage: ' + sys.argv[0] + ' [ options ]')
		sys.exit(1)
	else:
		print('Unknown option: ' + sys.argv[i] )
		sys.exit(1)

if not topic:
	print(
"""Usage: sender.py --topic 'topic[/partition]'
               [--bootstrap_servers' host:port  ]
               [--acks 0,(1),'all' ]
               [--retries  (0)  ]
               [--batch_size'(16384) ]
               [--linger_ms  (0) # ms ]
               [--buffer_memory  (33554432) ]
               [--max_request_size (1048576) ]
               [--request_timeout_ms (10000) # ms ]
               [--client client_id ]
               [--cycle (unlimited) ]
               [--rows  (30) ]
               [--interval (5) ]
""")
	sys.exit(1)

if client_id:
	producer=KafkaProducer( bootstrap_servers=hosts,
			acks=acks,
			retries=retries,
			batch_size=batch_size,
			linger_ms=linger_ms,
			buffer_memory=buffer_memory,
			max_request_size=max_request_size,
			request_timeout_ms=request_timeout_ms,
			client_id=client_id)
else:
	producer=KafkaProducer( bootstrap_servers=hosts,
			acks=acks,
			retries=retries,
			batch_size=batch_size,
			linger_ms=linger_ms,
			buffer_memory=buffer_memory,
			max_request_size=max_request_size,
			request_timeout_ms=request_timeout_ms)

key_id=0
random.seed()
row=0
cycle=0

try:
	while True:
		key_id= '%08d' % (random.randrange( key_min,key_max))
		tstmp=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
		latitude=str(random.gauss(35,20))
		longitude=str(random.gauss(140,20))
		message= key_id + delim + tstmp + delim + latitude + delim + longitude
		
		try:
			rMeta=producer.send( topic=topic , partition=partition, value=message)
			if debug > 1000000:
				print(rMeta.get())
		except(Errors.KafkaTimeoutError) as e:
			print('KafkaTimeoutError: ' + str(e))

		row += 1
		if row == rows:
			row=0
			cycle += 1
			if debug == 1:
				print(rMeta.get())
			if interval:
				time.sleep(interval)
			if max_cycle and cycle == max_cycle:
				break

except(KeyboardInterrupt):
		print('Receive user interrupt.')
		print('exitting.....')
		producer.close()
		sys.exit(0)


producer.close()
