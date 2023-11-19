import json
import os
import pandas as pd
from helper_function import *


def record_bad_response(z_code, *args, filename, extension="csv"):
    bad_record_file = os.path.join(".", "output", filename + "." + extension)
    if not os.path.exists(bad_record_file):
        with open(bad_record_file, "a+") as f:
            if len(args) > 0:
                text_headers = ",".join(["zip_code"] + [args[0]])
            else:
                text_headers = "zip_code"
            f.write("{}\n".format(text_headers))
    with open(bad_record_file, "a+") as f:
        if len(args) > 0:
            text_entries = ",".join([z_code] + [args[1]])
        else:
            text_entries = z_code
        f.write("{}\n".format(text_entries))


def check_zip_code_type(json_result):
    return "postal_code" in json_result["types"]


def parse_zip_code_info(zip_code, json_result):
    if not check_zip_code_type(json_result):
        record_bad_response(zip_code, filename="zip_codes_with_non_postal_code_response")
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
        record_bad_response(zip_code, filename="zip_codes_missing_address_components_in_response")
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
        record_bad_response(
            zip_code, "returned_zip_code", val_zip_code,
            filename="zip_codes_wrong_postal_code_in_response"
        )
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
        locals().get("val_" + c, '') for c in df_zip_code_info_columns  # In case variable `val_xyz` doesn't exist
    ]
    df_zip_code_info = pd.DataFrame(
        [{key: val for (key, val) in zip(df_zip_code_info_columns, df_zip_code_info_values)}],
        columns=df_zip_code_info_columns
    )

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
            record_bad_response(zip_code, filename="zip_codes_with_empty_api_response")
        elif len(json_response["results"]) > 1:
            record_bad_response(
                zip_code,
                "num_response", str(len(json_response)),
                filename="zip_codes_with_multiple_api_responses"
            )
        else:
            json_result = json_response["results"][0]
            parse_zip_code_info(zip_code, json_result)


if __name__ == "__main__":
    if not os.path.exists(os.path.join(".", "output")):
        os.makedirs(os.path.join(".", "output"))

    # zip_code = "00000"  # To test empty response
    # zip_code = "26140"  # To test multi-result response
    # zip_code = "021"  # To test non-postal-code response
    # zip_code = "01706"  # To test responses with no "address_components"
    # zip_code = "01707"  # To test responses returning a mismatched zip code
    # zip_code = "01701"  # A normal zip code file
    zip_code = "01703"  # A special zip code file

    if os.path.exists(os.path.join(".", "output", "valid_zip_codes_info.tsv")):
        df_zip_codes_already_parsed = pd.read_csv(
            os.path.join(".", "output", "valid_zip_codes_info.tsv"),
            dtype=str, sep="\t", usecols=["zip_code"]
        )
        if zip_code not in list(df_zip_codes_already_parsed["zip_code"]):
            read_json_file(zip_code)
    else:
        read_json_file(zip_code)
