import json
import os
import pandas as pd
from helper_function import *


def add_to_empty_response_list(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    empty_response_record_file = os.path.join(".", "output", "zip_codes_with_empty_api_response.csv")
    if not os.path.exists(empty_response_record_file):
        with open(empty_response_record_file, "a+") as record_file:
            record_file.write("zip_code\n")
    with open(empty_response_record_file, "a+") as record_file:
        record_file.write("{}\n".format(zip_code))


def add_to_multi_response_list(zip_code, length):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    multi_response_record_file = os.path.join(".", "output", "zip_codes_with_multiple_api_responses.csv")
    if not os.path.exists(multi_response_record_file):
        with open(multi_response_record_file, "a+") as record_file:
            record_file.write("{col1},{col2}\n".format(col1="zip_code", col2="num_response"))
    with open(multi_response_record_file, "a+") as record_file:
        record_file.write("{col1},{col2}\n".format(col1=str(zip_code), col2=str(length)))


def add_to_non_postal_code_list(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    non_postal_code_record_file = os.path.join(".", "output", "zip_codes_with_non_postal_code_response.csv")
    if not os.path.exists(non_postal_code_record_file):
        with open(non_postal_code_record_file, "a+") as record_file:
            record_file.write("zip_code\n")
    with open(non_postal_code_record_file, "a+") as record_file:
        record_file.write("{}\n".format(zip_code))


def add_to_missing_addr_components_list(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    missing_addr_comp_record_file = os.path.join(".", "output", "zip_codes_missing_address_components_response.csv")
    if not os.path.exists(missing_addr_comp_record_file):
        with open(missing_addr_comp_record_file, "a+") as record_file:
            record_file.write("zip_code\n")
    with open(missing_addr_comp_record_file, "a+") as record_file:
        record_file.write("{}\n".format(zip_code))


def add_to_mismatched_zip_code_list(zip_code):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    mismatched_zip_code_record_file = os.path.join(".", "output", "zip_codes_mismatched_response.csv")
    if not os.path.exists(mismatched_zip_code_record_file):
        with open(mismatched_zip_code_record_file, "a+") as record_file:
            record_file.write("zip_code\n")
    with open(mismatched_zip_code_record_file, "a+") as record_file:
        record_file.write("{}\n".format(zip_code))


def check_zip_code_type(json_result):
    return "postal_code" in json_result["types"]


def parse_zip_code_info(zip_code, json_result):
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    if not check_zip_code_type(json_result):
        add_to_non_postal_code_list(zip_code)
        return

    val_geo_type = "zip code"
    val_google_place_id = safe_get(json_result, "place_id")
    val_google_location_type = safe_get(json_result, "geometry", "location_type")
    val_centroid_lat = safe_get(json_result, "geometry", "location", "lat")
    val_centroid_lon = safe_get(json_result, "geometry", "location", "lng")
    val_northeast_bound_lat = safe_get(json_result, "geometry", "bounds", "northeast", "lat")
    val_northeast_bound_lon = safe_get(json_result, "geometry", "bounds", "northeast", "lng")
    val_southwest_bound_lat = safe_get(json_result, "geometry", "bounds", "southwest", "lat")
    val_southwest_bound_lon = safe_get(json_result, "geometry", "bounds", "southwest", "lng")

    if not check_key_exists(json_result, "address_components"):
        add_to_missing_addr_components_list(zip_code)
        return
    df_address_components = pd.json_normalize(json_result["address_components"])

    for index, row in df_address_components.iterrows():
        if "postal_code" in row["types"]:
            val_zip_code = row["long_name"]
        elif "locality" in row["types"] and "political" in row["types"]:
            val_municipality = row["long_name"]
            val_municipality_abbreviation = row["short_name"]
        elif "administrative_area_level_2" in row["types"] and "political" in row["types"]:
            val_county = row["long_name"]
            val_county_abbreviation = row["short_name"]
        elif "administrative_area_level_1" in row["types"] and "political" in row["types"]:
            val_state = row["long_name"]
            val_state_abbreviation = row["short_name"]
        elif "country" in row["types"] and "political" in row["types"]:
            val_country = row["long_name"]
            val_country_code = row["short_name"]

    if not val_zip_code or val_zip_code != zip_code:
        add_to_mismatched_zip_code_list(zip_code)
        return

    df_zip_code_info_columns = [
        "geo_type", "zip_code", "google_place_id", "google_location_type",
        "centroid_lat", "centroid_lon",
        "northeast_bound_lat", "northeast_bound_lon",
        "southwest_bound_lat", "southwest_bound_lon",
        "municipality", "municipality_abbreviation",
        "county", "county_abbreviation",
        "state", "state_abbreviation",
        "country", "country_code"
    ]
    df_zip_code_info_values = [
        locals().get("val_" + c, '') for c in df_zip_code_info_columns
    ]
    df_zip_code_info = pd.DataFrame(
        [{key: val for (key, val) in zip(df_zip_code_info_columns, df_zip_code_info_values)}],
        columns=df_zip_code_info_columns
    )
    # print(df_zip_code_info)

    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))
    valid_zip_code_record_file = os.path.join(".", "output", "valid_zip_codes_info.tsv")
    if not os.path.exists(valid_zip_code_record_file):
        df_zip_code_info.to_csv(
            valid_zip_code_record_file, sep="\t",
            header=True, index=False, mode="w"
        )
    else:
        df_zip_code_info.to_csv(
            valid_zip_code_record_file, sep="\t",
            header=False, index=False, mode="a"
        )
    return df_zip_code_info


def read_json_file(zip_code):
    json_file_path = os.path.join(".", "data", "json1-{}.json".format(zip_code))
    with open(json_file_path, "r") as json_file:
        json_response = json.load(json_file)
        if len(json_response["results"]) == 0:
            add_to_empty_response_list(zip_code)
        elif len(json_response["results"]) > 1:
            add_to_multi_response_list(zip_code, len(json_response))
        else:
            json_result = json_response["results"][0]
            parse_zip_code_info(zip_code, json_result)


if __name__ == "__main__":
    zip_code = "01701"
    read_json_file(zip_code)
