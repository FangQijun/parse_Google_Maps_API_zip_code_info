import json
import os
import pandas as pd


def add_to_empty_response_list(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    empty_response_record_file = os.path.join(".", "output", "zip_codes_with_empty_api_response.csv")
    with open(empty_response_record_file, "a+") as record_file:
        record_file.write("{}\n".format(zip_code))


def add_to_multi_response_list(zip_code, length):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    empty_response_record_file = os.path.join(".", "output", "zip_codes_with_multiple_api_responses.csv")
    if not os.path.exists(empty_response_record_file):
        with open(empty_response_record_file, "a+") as record_file:
            record_file.write("{col1},{col2}\n".format(col1="zip_code", col2="num_response"))
    with open(empty_response_record_file, "a+") as record_file:
        record_file.write("{col1},{col2}\n".format(col1=str(zip_code), col2=str(length)))


def parse_zip_code_info(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))


def read_json_file(zip_code):
    json_file_path = os.path.join(".", "data", "json1-{}.json".format(zip_code))
    with open(json_file_path, "r") as json_file:
        json_response = json.load(json_file)
        if len(json_response["results"]) == 0:
            add_to_empty_response_list(zip_code)
        elif len(json_response["results"]) > 1:
            add_to_multi_response_list(zip_code, len(json_response))
        else:
            json_response = json_response["results"][0]
            parse_zip_code_info(zip_code)


if __name__ == "__main__":
    zip_code = "01701"
    read_json_file(zip_code)
