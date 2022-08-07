import datetime
import json

from project.constant import KST


def get_secret_data() -> dict:
    with open("project/secret.json") as f:
        data = json.load(f)
    return data


def convert_utc_to_kst_datetime(datetime_str: str, dt_format: str) -> datetime:
    utc_dt = datetime.datetime.strptime(datetime_str, dt_format)

    return KST.convert(utc_dt + datetime.timedelta(hours=9))


def convert_empty_string_value_to_null(d: dict) -> dict:
    for k, v in d.items():
        if v == "":
            d[k] = None
    return d
