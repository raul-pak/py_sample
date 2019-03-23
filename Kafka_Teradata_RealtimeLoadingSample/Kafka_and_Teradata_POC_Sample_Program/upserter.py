#!/usr/bin/python

import sys
import re
import codecs
import time
import datetime
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.structs import OffsetAndMetadata
import kafka.errors as kafkaerror

import teradata

tdpid=""

debug=0
rows=500
delim=','
topic=''
partition=[]
group=''
#hosts=['localhost:9092']
hosts=['myLinux:9092']
client=''
#timeout=-1
consumer_timeout_ms=5000	# default timeout
seek_to_beginning=0
seek_to_end=0
offset=[]
assign=1

#SQL="""insert into continuous_load values (?,?,?,?) """
#SQL="""insert into continuous_load_10m values (?,?,?,?) """
SQL="""update continuous_upsert set datatime = ?, latitude= ? mod 100 , longitude = ? where key_id = ?
       else insert into continuous_upsert values (?,?,?,?)"""
#SQL="""update continuous_upsert set datatime = ?, latitude= ? mod 20 , longitude = ? where key_id = ?"""


def upsert_rows( session, nrow, values):
	param_array=[]
	offsets=''
	
	for m  in values:
		vs = m.value.split(delim)
		if debug > 10000000000000000:
			print ( m.value )
			print ( vs )
		param=( vs[1],vs[2],vs[3],vs[0],vs[0], vs[1], vs[2], vs[3] )
		#param=( vs[1],vs[2],vs[3],vs[0])
		param_array.append(param)
	
	params=tuple(param_array)
	for  p in reading_partition:
		v=[ x for x in  values if x.partition==p ]
		if v:
			offsets += '(%d,%d)' % (p,v[-1].offset)

	if params:
		st_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
		start=datetime.datetime.now()
		try:
			session.executemany(SQL, params,batch=True,logParamFrequency=rows)
			end=datetime.datetime.now()
			dur=end-start
			print("%s: upsert %d rows(%d),  %02d.%06d sec, (parition,offset)=[%s]" 
				% (st_time, nrow, len(params),dur.seconds, dur.microseconds,offsets))
		except(teradata.api.DatabaseError) as e:
			print(e)


def process_rows( nrow, values):
	offsets=''
	for  p in reading_partition:
		v=[ x for x in  values if x.partition==p ]
		if v:
			offsets += '(%d,%d)' % (p,v[-1].offset)
	
	if debug:
		print("##### Received %d rows. Last offset (partiton, offset) =[ %s ].  %d row processed." 
			% (nrow, offsets, len(values)))
	

nargs=len(sys.argv)
i=1
topicpartition=None	# current topicpartition -  if set, used as default topicpartition
while i < nargs:
	if sys.argv[i] == '--topic':
		tp=sys.argv[i+1].split("/")
		topic=tp[0]
		if len(tp) > 1:
			ps=tp[1].split(",")
			for p in ps:
				partition.append(int(p))
		i += 2
		continue
	elif sys.argv[i] == '--group':
		group=sys.argv[i+1]
		i += 2
		continue
	elif sys.argv[i] == '--hosts':
		hosts=sys.argv[i+1].split(',')
		i += 2
		continue
	elif sys.argv[i] == '--timeout': # ms
		consumer_timeout_ms=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--beginning':
		seek_to_beginning=1
		i += 1
		continue
	elif sys.argv[i] == '--end':
		seek_to_end=1
		i += 1
		continue
	elif sys.argv[i] == '--offset':
		ofst=sys.argv[i+1].split(",")
		for o in ofst:
			offset.append(int(o))
		i += 2
		continue
	elif sys.argv[i] == '--row':
		rows=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i] == '--tdpid':
		tdpid=sys.argv[i+1]
		i += 2
		continue
	elif sys.argv[i] == '--debug':
		debug=int(sys.argv[i+1])
		i += 2
		continue
	elif sys.argv[i][0] == '-':
		print('Unknown option: ' + sys.argv[i] )
		print('Usage: ' + sys.argv[0] + ' --topic topic [ --group group --hosts list_of_hosts --timeout t(ms) ]')
		sys.exit(1)

if not topic:
	print('--topic option is required.')
	print('Usage: ' + sys.argv[0] + ' --topic topic [ --group group --hosts list_of_hosts --timeout t(ms) ]')
	sys.exit(1)

#######################################################
print("Setting up Teradata Connection....")

uda=teradata.UdaExec()
if tdpid:
	session=uda.connect("${ODBC}",system=tdpid)
else:
	session=uda.connect("${ODBC}")



#######################################################
print("Setting up Kafka Connections.... ")

if group:
	print("Conntecting server, and subscribe %s. group_id is %s." % (topic, group))
	if partition:
		consumer=KafkaConsumer( group_id = group, client_id='upserter',
			bootstrap_servers=hosts,consumer_timeout_ms=consumer_timeout_ms)
	else:
		consumer=KafkaConsumer( topic, group_id = group, 
			bootstrap_servers=hosts,consumer_timeout_ms=consumer_timeout_ms)
else:
	print("Conntecting server, and subscribe %s " % (topic))
	if partition:
		consumer=KafkaConsumer(
			bootstrap_servers=hosts,consumer_timeout_ms=consumer_timeout_ms)
	else:
		consumer=KafkaConsumer( topic,
			bootstrap_servers=hosts,consumer_timeout_ms=consumer_timeout_ms)


topicPartition=[]
for p in partition:
	topicPartition.append(TopicPartition(topic,p))

if topicPartition:
	try:
		consumer.assign(topicPartition)
	except(kafkaerror.IllegalStateError) as e:
		print("IllegalStateError: %s" % (e))
	reading_partition=partition
else:
	reading_partition=consumer.partitions_for_topic(topic)

print(reading_partition)

if debug:
	print("topics: %s" % consumer.topics())
	print( "partition: %s" % partition)
	print( "assignment: %s" % consumer.assignment())
	print( "subscription: %s" % consumer.subscription())


if seek_to_beginning:
	try:
		consumer.seek_to_beginning()
	except(AssertionError) as e:
		print("AssertionError: %s" % (e))
		sys.exit(1)
elif seek_to_end:
	try:
		consumer.seek_to_end()
	except(AssertionError) as e:
		print("AssertionError: %s" % (e))
		sys.exit(1)
if offset:
	i=0
	for tp in topicPartition:
		try:
			consumer.seek(tp,offset[i])
			if len(offset)>1:
				i+=1
		except(AssertionError) as e:
			print("AssertionError: %s" % (e))
			sys.exit(1)

#######################################################################
row=0
mvalues=[]
try:
	while True:
		try:
			for message in consumer:
				row += 1
				if debug > 10000000:
					print ("%s:%d:%d: key=%s value=%s" % (message.topic, 
								message.partition, message.offset, 
								message.key, message.value))
				mvalues.append(message)
				if row == rows:
					upsert_rows( session, row, mvalues)
					if debug:
						process_rows( row, mvalues)
					row=0
					mvalues=mvalues[:0]
	
			
			print('Consumer Timeout')
			if row:
				upsert_rows( session,row, mvalues)
				if debug:
					process_rows( row, mvalues)
				row=0
				mvalues=mvalues[:0]
			
		except(StopIteration) as e:
			print('StopIteration: %s' % (e))
			continue
except(KeyboardInterrupt):
	upsert_rows( session,row, mvalues)
	if debug:
		process_rows( row, mvalues)
	print('Receive user interrupt...')
	consumer.close()
	session.close()
	sys.exit(0)

consumer.close()	
session.close()
			
