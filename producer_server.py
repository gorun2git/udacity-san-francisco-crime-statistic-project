from kafka import KafkaProducer
import json
import time
import utils


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            input_data = json.loads(f.read())
            for d in input_data:
                message = utils.dict_to_binary(d)
                self.send(self.topic, message)
                time.sleep(1)

