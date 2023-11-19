import requests
import pandas as pd
import json
import os
from parse_api_response import read_json_file
from helper_function import *


def get_zip_list_to_query(): # TODO
    # list, notes = ["01701", "01702"], "normal zip code"
    # list, notes = ["01703", "01704", "01705"], "special zip code - p.o. box"
    # list, notes = ["01776", "01778"], "normal zip code"
    # list, notes = ["01111"], "special zip code - standalone"
    list, notes = ["09060"], "special zip code - military"
    return list, notes


def send_one_api_payload(zip_code, country_code, api_token, verbose=False):
    request_url = """
    https://maps.googleapis.com/maps/api/geocode/json?address={zip_code},{country_code}&key={api_token}
    """.format(
        zip_code=zip_code,
        country_code=country_code,
        api_token=api_token
    )
    response = requests.get(request_url).json()
    if verbose:
        print(response)
    return response


if __name__ == "__main__":
    api_token = os.environ.get("GOOGLE_MAPS_API_TOKEN")
    list_zip_codes_to_query, notes = get_zip_list_to_query()

    if not os.path.exists(os.path.join(".", "data")):
        os.makedirs(os.path.join(".", "data"))
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))

    for zip_code in list_zip_codes_to_query:
        if os.path.exists(os.path.join(".", "output", "valid_zip_codes_info.tsv")):
            df_zip_codes_already_parsed = pd.read_csv(
                os.path.join(".", "output", "valid_zip_codes_info.tsv"),
                dtype=str, sep="\t", usecols=["zip_code"]
            )
            list_zip_codes_already_parsed = list(df_zip_codes_already_parsed["zip_code"])
        else:
            list_zip_codes_already_parsed = []
        if zip_code not in list_zip_codes_already_parsed:
            api_response = send_one_api_payload(
                zip_code=zip_code,
                country_code="US",
                api_token=api_token,
                verbose=True
            )
            if api_response["status"] != "OK":
                record_bad_response(
                    zip_code,
                    "status_code_received",
                    api_response["status"],
                    filename="zip_codes_with_non_ok_response_code"
                )
                continue
            else:
                response_filename = "json1-{}.json".format(zip_code)
                with open(os.path.join(".", "data", response_filename), 'w') as f:
                    f.write(json.dumps(api_response))
                read_json_file(zip_code, notes=notes)