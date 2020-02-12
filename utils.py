import os
import gzip
import json
from pathlib import Path


def get_topic_name(name):
    topic_name = name.replace("-", '.')
    return f"{topic_name}.topic"


def prepare_input_file(file_name):
    compressed_file = f"{Path(__file__).parents[0]}/{file_name}.json.gz"
    output_file = f"{Path(__file__).parents[0]}/{file_name}.json"
    if os.path.isfile(compressed_file):
        with gzip.open(compressed_file, "r") as f:
            compressed_input_file_content = f.read()
        with open(output_file, "w") as f:
            f.write(compressed_input_file_content.decode('UTF-8'))

    return output_file

def dict_to_binary(json_dict):
    return json.dumps(json_dict).encode('UTF-8')