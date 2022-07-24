import datetime
import json

import pendulum

KST = pendulum.timezone("Asia/Seoul")


def get_secret_data() -> dict:
    with open("secret/airflow.json") as f:
        data = json.load(f)
    return data


def convert_utc_to_kst_datetime(datetime_str: str, dt_format: str) -> datetime:
    utc_dt = datetime.datetime.strptime(datetime_str, dt_format)

    return KST.convert(utc_dt + datetime.timedelta(hours=9))
