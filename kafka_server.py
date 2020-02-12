import os
import gzip
from pathlib import Path

import producer_server


def _get_topic_name(name):
	topic_name = name.replace("-", '.')
	return f"{topic_name}.topic"


def _prepare_input_file(file_name):
	compressed_file = f"{Path(__file__).parents[0]}/{file_name}.json.gz"
	output_file = f"{Path(__file__).parents[0]}/{file_name}.json"
	if os.path.isfile(compressed_file):
		with gzip.open(compressed_file, "r") as f:
			compressed_input_file_content = f.read()
		with open(output_file, "w") as f:
			f.write(compressed_input_file_content.decode('UTF-8'))

	return output_file


def run_kafka_server():
	f_name = "police-department-calls-for-service"
	input_file = _prepare_input_file(f_name)
	topic_name = _get_topic_name(f_name)

	producer = producer_server.ProducerServer(
		input_file=input_file,
		topic=topic_name,
		bootstrap_servers="PLAINTEXT://localhost:9092",
		client_id="0"
	)

	return producer


def feed():
	producer = run_kafka_server()
	producer.generate_data()


if __name__ == "__main__":
	feed()
