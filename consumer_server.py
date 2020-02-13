import json

from kafka import KafkaConsumer

import utils
import config

if __name__ == "__main__":
    topic_name = utils.get_topic_name(config.INPUT_FILE_NAME)
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=config.BOOTSTRAP_SERVERS,
                             group_id="0",
                             auto_offset_reset="earliest",
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        print(f"Consumed message: topic= {message.topic}, key={message.key} value={message.value}")
