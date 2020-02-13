import producer_server
import config
import utils


def run_kafka_server():
	f_name = config.INPUT_FILE_NAME
	input_file = utils.prepare_input_file(f_name)
	topic_name = utils.get_topic_name(f_name)

	producer = producer_server.ProducerServer(
		input_file=input_file,
		topic=topic_name,
		bootstrap_servers=config.BOOTSTRAP_SERVERS
	)
	return producer


def feed():
	producer = run_kafka_server()
	producer.generate_data()


if __name__ == "__main__":
	feed()
