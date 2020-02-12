from kafka import KafkaConsumer

if __name__ == "__main__":
	consumer = KafkaConsumer("police.department.calls.for.service.topic",
							bootstrap_servers="PLAINTEXT://localhost:9092",
							group_id="0",
							auto_offset_reset="earliest")
	for message in consumer:
		message.value.decode('UTF-8')
		print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
											 message.offset, message.key,
											 message.value))
